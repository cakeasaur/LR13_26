package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	"github.com/sleepysweety/fraud-detection/agents/shared"
)

var tracer trace.Tracer

const (
	weightFrequency = 0.30
	weightAmount    = 0.25
	weightPatterns  = 0.25
	weightGeo       = 0.10
	weightTime      = 0.10

	thresholdLow    = 30.0
	thresholdMedium = 55.0
	thresholdHigh   = 75.0
)

type Assessor struct {
	ctx context.Context
	nc  *nats.Conn
	rdb *redis.Client
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var shutdown func()
	var err error

	tracer, shutdown, err = shared.InitTracer("risk-assessor")
	if err != nil {
		log.Printf("[RiskAssessor] WARN: трассировка недоступна: %v", err)
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
		log.Fatalf("[RiskAssessor] Ошибка подключения к NATS: %v", err)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			log.Printf("[RiskAssessor] WARN: drain error: %v", err)
		}
	}()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[RiskAssessor] Ошибка подключения к Redis: %v", err)
	}

	log.Printf("[RiskAssessor] Подключён к NATS: %s, Redis: %s", natsURL, redisAddr)

	a := &Assessor{ctx: ctx, nc: nc, rdb: rdb}

	sub, err := nc.QueueSubscribe(shared.SubjectTransactionsAnalyzed, "assessors", func(msg *nats.Msg) {
		a.handleAnalyzed(msg)
	})
	if err != nil {
		log.Fatalf("[RiskAssessor] Ошибка подписки: %v", err)
	}
	defer sub.Unsubscribe()

	log.Printf("[RiskAssessor] Слушает тему: %s", shared.SubjectTransactionsAnalyzed)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	log.Println("[RiskAssessor] Завершение работы")
}

func (a *Assessor) handleAnalyzed(msg *nats.Msg) {
	ctx := shared.ExtractContext(a.ctx, msg)
	ctx, span := tracer.Start(ctx, "transaction.assess_risk")
	defer span.End()

	var pr shared.PatternResult
	if err := json.Unmarshal(msg.Data, &pr); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid json")
		log.Printf("[RiskAssessor] ERROR: невалидный JSON: %v", err)
		return
	}

	result := a.assess(pr)

	span.SetAttributes(
		attribute.String("tx.id", pr.Transaction.ID),
		attribute.Float64("tx.risk_score", result.RiskScore),
		attribute.String("tx.risk_level", result.RiskLevel),
	)

	data, err := json.Marshal(result)
	if err != nil {
		span.RecordError(err)
		log.Printf("[RiskAssessor] ERROR: ошибка сериализации: %v", err)
		return
	}

	if err := shared.PublishWithContext(ctx, a.nc, shared.SubjectTransactionsRisk, data); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		log.Printf("[RiskAssessor] ERROR: ошибка публикации: %v", err)
		return
	}

	span.SetStatus(codes.Ok, result.RiskLevel)
	log.Printf("[RiskAssessor] INFO: txID=%s score=%.1f level=%s",
		pr.Transaction.ID, result.RiskScore, result.RiskLevel)
}

func (a *Assessor) assess(pr shared.PatternResult) shared.RiskResult {
	tx := pr.Transaction

	freqScore := normalize(pr.FrequencyScore, 0, 20) * 100
	amountScore := normalize(pr.AmountDeviation, 0, 10) * 100
	patternScore := float64(len(pr.Patterns)) * 20
	if patternScore > 100 {
		patternScore = 100
	}
	geoScore := a.geoRisk(tx.CountryCode)
	timeScore := timeRisk(tx.Timestamp)

	riskScore := freqScore*weightFrequency +
		amountScore*weightAmount +
		patternScore*weightPatterns +
		geoScore*weightGeo +
		timeScore*weightTime

	riskScore += a.accountPenalty(tx.AccountID)
	if riskScore > 100 {
		riskScore = 100
	}

	level := riskLevel(riskScore)
	a.saveRiskScore(tx, riskScore, level)

	return shared.RiskResult{
		Transaction: tx,
		Patterns:    pr,
		RiskScore:   riskScore,
		RiskLevel:   level,
	}
}

func (a *Assessor) geoRisk(country string) float64 {
	highRisk := map[string]float64{"NG": 90, "RO": 75, "PK": 70, "BD": 65, "VN": 60, "UA": 55, "BY": 55}
	mediumRisk := map[string]float64{"CN": 40, "RU": 40, "IN": 35, "BR": 30}
	if v, ok := highRisk[country]; ok {
		return v
	}
	if v, ok := mediumRisk[country]; ok {
		return v
	}
	return 10
}

func timeRisk(t time.Time) float64 {
	hour := t.UTC().Hour()
	if hour >= 1 && hour < 5 {
		return 70
	}
	if hour >= 22 || hour == 0 {
		return 40
	}
	return 10
}

func (a *Assessor) accountPenalty(accountID string) float64 {
	key := fmt.Sprintf("blocks:%s", accountID)
	count, err := a.rdb.Get(a.ctx, key).Int64()
	if err != nil {
		return 0
	}
	penalty := float64(count) * 5
	if penalty > 25 {
		return 25
	}
	return penalty
}

func (a *Assessor) saveRiskScore(tx shared.Transaction, score float64, level string) {
	key := fmt.Sprintf("risk:%s", tx.ID)
	a.rdb.HSet(a.ctx, key,
		"account_id", tx.AccountID,
		"score", fmt.Sprintf("%.2f", score),
		"level", level,
		"timestamp", tx.Timestamp.Format(time.RFC3339),
	)
	a.rdb.Expire(a.ctx, key, 24*time.Hour)
}

func riskLevel(score float64) string {
	switch {
	case score < thresholdLow:
		return "LOW"
	case score < thresholdMedium:
		return "MEDIUM"
	case score < thresholdHigh:
		return "HIGH"
	default:
		return "CRITICAL"
	}
}

func normalize(value, min, max float64) float64 {
	if max == min {
		return 0
	}
	n := (value - min) / (max - min)
	if n < 0 {
		return 0
	}
	if n > 1 {
		return 1
	}
	return n
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
