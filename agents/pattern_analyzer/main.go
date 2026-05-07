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
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	"github.com/sleepysweety/fraud-detection/agents/shared"
)

var (
	ctx    = context.Background()
	tracer trace.Tracer
)

type Analyzer struct {
	nc  *nats.Conn
	rdb *redis.Client
}

func main() {
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
	defer nc.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[PatternAnalyzer] Ошибка подключения к Redis: %v", err)
	}

	log.Printf("[PatternAnalyzer] Подключён к NATS: %s, Redis: %s", natsURL, redisAddr)

	a := &Analyzer{nc: nc, rdb: rdb}

	sub, err := nc.QueueSubscribe(shared.SubjectTransactionsValidated, "analyzers", func(msg *nats.Msg) {
		a.handleValidated(msg)
	})
	if err != nil {
		log.Fatalf("[PatternAnalyzer] Ошибка подписки: %v", err)
	}
	defer sub.Unsubscribe()

	log.Printf("[PatternAnalyzer] Слушает тему: %s", shared.SubjectTransactionsValidated)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("[PatternAnalyzer] Завершение работы")
}

func (a *Analyzer) handleValidated(msg *nats.Msg) {
	spanCtx, span := tracer.Start(context.Background(), "transaction.analyze_patterns")
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

	if err := a.nc.Publish(shared.SubjectTransactionsAnalyzed, data); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		log.Printf("[PatternAnalyzer] ERROR: ошибка публикации: %v", err)
		return
	}

	span.SetStatus(codes.Ok, "analyzed")
	log.Printf("[PatternAnalyzer] INFO: txID=%s suspicious=%v patterns=%v score=%.2f",
		tx.ID, result.Suspicious, result.Patterns, result.FrequencyScore)
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
	pipe.ZAdd(ctx, key, &redis.Z{Score: now, Member: tx.ID})
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%f", now-window))
	pipe.ZCard(ctx, key)
	pipe.Expire(ctx, key, 5*time.Minute)
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("[PatternAnalyzer] WARN: Redis pipeline error: %v", err)
		return 0
	}

	count := cmds[2].(*redis.IntCmd).Val()
	return float64(count)
}

func (a *Analyzer) checkAmountDeviation(tx shared.Transaction) float64 {
	key := fmt.Sprintf("amounts:%s", tx.AccountID)

	a.rdb.LPush(ctx, key, tx.Amount)
	a.rdb.LTrim(ctx, key, 0, 99)
	a.rdb.Expire(ctx, key, 24*time.Hour)

	vals, err := a.rdb.LRange(ctx, key, 0, -1).Result()
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
	a.rdb.LPush(ctx, key, string(data))
	a.rdb.LTrim(ctx, key, 0, 49)
	a.rdb.Expire(ctx, key, 48*time.Hour)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
