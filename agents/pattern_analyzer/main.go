package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	"github.com/sleepysweety/fraud-detection/agents/shared"
)

var (
	tracer      trace.Tracer
	workerID    string
	currentLoad atomic.Int64
	processed   atomic.Int64
)

type Analyzer struct {
	ctx context.Context
	nc  *nats.Conn
	rdb *redis.Client
}

type bid struct {
	WorkerID string `json:"worker_id"`
	Load     int64  `json:"load"`
}

func main() {
	workerID = uuid.New().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var shutdown func()
	var err error

	tracer, shutdown, err = shared.InitTracer("pattern-analyzer")
	if err != nil {
		log.Printf("[PatternAnalyzer] WARN: трассировка недоступна: %v", err)
		tracer = trace.NewNoopTracerProvider().Tracer("")
		shutdown = func() {}
	}
	defer shutdown()

	natsURL := getEnv("NATS_URL", nats.DefaultURL)
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(10),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("[PatternAnalyzer] Ошибка подключения к NATS: %v", err)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			log.Printf("[PatternAnalyzer] WARN: drain error: %v", err)
		}
	}()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[PatternAnalyzer] Ошибка подключения к Redis: %v", err)
	}

	log.Printf("[PatternAnalyzer] workerID=%s NATS=%s Redis=%s", workerID[:8], natsURL, redisAddr)

	a := &Analyzer{ctx: ctx, nc: nc, rdb: rdb}

	// Auction: respond to bid requests
	auctionSub, err := nc.Subscribe(shared.SubjectTransactionsAuction, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		b := bid{WorkerID: workerID, Load: currentLoad.Load()}
		data, _ := json.Marshal(b)
		if err := nc.Publish(msg.Reply, data); err != nil {
			log.Printf("[PatternAnalyzer] WARN: ошибка ставки: %v", err)
		}
	})
	if err != nil {
		log.Fatalf("[PatternAnalyzer] Ошибка подписки на auction: %v", err)
	}
	defer auctionSub.Unsubscribe()

	// Worker inbox: receive won transactions
	workerSubject := shared.SubjectTransactionsWorkerPrefix + workerID
	workerSub, err := nc.Subscribe(workerSubject, func(msg *nats.Msg) {
		currentLoad.Add(1)
		defer currentLoad.Add(-1)
		a.handleValidated(msg)
		processed.Add(1)
	})
	if err != nil {
		log.Fatalf("[PatternAnalyzer] Ошибка подписки на worker inbox: %v", err)
	}
	defer workerSub.Unsubscribe()

	log.Printf("[PatternAnalyzer] Готов к аукциону: auction=%s worker=%s",
		shared.SubjectTransactionsAuction, workerSubject)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()

	log.Printf("[PatternAnalyzer] Завершение. workerID=%s обработано=%d",
		workerID[:8], processed.Load())
}

func (a *Analyzer) handleValidated(msg *nats.Msg) {
	// Восстанавливаем родительский span из заголовков — связываем с collector'ом
	ctx := shared.ExtractContext(a.ctx, msg)
	spanCtx, span := tracer.Start(ctx, "transaction.analyze_patterns")
	defer span.End()

	var vr shared.ValidationResult
	if err := json.Unmarshal(msg.Data, &vr); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid json")
		log.Printf("[PatternAnalyzer] ERROR: невалидный JSON: %v", err)
		return
	}

	tx := vr.Transaction
	span.SetAttributes(
		attribute.String("tx.id", tx.ID),
		attribute.String("tx.account_id", tx.AccountID),
		attribute.Float64("tx.amount", tx.Amount),
		attribute.String("worker.id", workerID[:8]),
	)

	result := a.analyze(spanCtx, tx)

	span.SetAttributes(
		attribute.Bool("tx.suspicious", result.Suspicious),
		attribute.Int("tx.pattern_count", len(result.Patterns)),
		attribute.Float64("tx.frequency_score", result.FrequencyScore),
	)

	data, err := json.Marshal(result)
	if err != nil {
		span.RecordError(err)
		log.Printf("[PatternAnalyzer] ERROR: ошибка сериализации: %v", err)
		return
	}

	if err := shared.PublishWithContext(spanCtx, a.nc, shared.SubjectTransactionsAnalyzed, data); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		log.Printf("[PatternAnalyzer] ERROR: ошибка публикации: %v", err)
		return
	}

	span.SetStatus(codes.Ok, "analyzed")
	log.Printf("[PatternAnalyzer] INFO: worker=%s txID=%s suspicious=%v patterns=%v score=%.2f",
		workerID[:8], tx.ID, result.Suspicious, result.Patterns, result.FrequencyScore)
}

func (a *Analyzer) analyze(spanCtx context.Context, tx shared.Transaction) shared.PatternResult {
	var patterns []string
	freqScore := 0.0
	amountDev := 0.0

	_, freqSpan := tracer.Start(spanCtx, "check.frequency")
	freqScore = a.checkFrequency(tx)
	freqSpan.End()
	if freqScore > 5 {
		patterns = append(patterns, fmt.Sprintf("high_frequency:%.0f_per_min", freqScore))
	}

	_, amtSpan := tracer.Start(spanCtx, "check.amount_deviation")
	amountDev = a.checkAmountDeviation(tx)
	amtSpan.End()
	if amountDev > 3.0 {
		patterns = append(patterns, fmt.Sprintf("amount_deviation:%.1fx", amountDev))
	}

	hour := tx.Timestamp.UTC().Hour()
	if hour >= 0 && hour < 5 {
		patterns = append(patterns, "unusual_hour")
	}

	if tx.Amount > 10000 {
		patterns = append(patterns, "large_amount")
	}

	a.saveToHistory(tx)

	suspicious := len(patterns) > 0
	return shared.PatternResult{
		Transaction:     tx,
		Suspicious:      suspicious,
		Patterns:        patterns,
		FrequencyScore:  freqScore,
		AmountDeviation: amountDev,
	}
}

func (a *Analyzer) checkFrequency(tx shared.Transaction) float64 {
	key := fmt.Sprintf("freq:%s", tx.AccountID)
	now := float64(tx.Timestamp.Unix())
	window := float64(60)

	pipe := a.rdb.Pipeline()
	pipe.ZAdd(a.ctx, key, &redis.Z{Score: now, Member: tx.ID})
	pipe.ZRemRangeByScore(a.ctx, key, "-inf", fmt.Sprintf("%f", now-window))
	pipe.ZCard(a.ctx, key)
	pipe.Expire(a.ctx, key, 5*time.Minute)
	cmds, err := pipe.Exec(a.ctx)
	if err != nil {
		log.Printf("[PatternAnalyzer] WARN: Redis pipeline error: %v", err)
		return 0
	}

	count := cmds[2].(*redis.IntCmd).Val()
	return float64(count)
}

func (a *Analyzer) checkAmountDeviation(tx shared.Transaction) float64 {
	key := fmt.Sprintf("amounts:%s", tx.AccountID)

	a.rdb.LPush(a.ctx, key, tx.Amount)
	a.rdb.LTrim(a.ctx, key, 0, 99)
	a.rdb.Expire(a.ctx, key, 24*time.Hour)

	vals, err := a.rdb.LRange(a.ctx, key, 0, -1).Result()
	if err != nil || len(vals) < 3 {
		return 0
	}

	var sum, count float64
	for _, v := range vals {
		amount, err := strconv.ParseFloat(v, 64)
		if err != nil {
			continue
		}
		sum += amount
		count++
	}
	avg := sum / count

	if avg == 0 {
		return 0
	}
	return math.Abs(tx.Amount-avg) / avg
}

func (a *Analyzer) saveToHistory(tx shared.Transaction) {
	key := fmt.Sprintf("tx:history:%s", tx.AccountID)
	data, _ := json.Marshal(tx)
	a.rdb.LPush(a.ctx, key, string(data))
	a.rdb.LTrim(a.ctx, key, 0, 49)
	a.rdb.Expire(a.ctx, key, 48*time.Hour)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
