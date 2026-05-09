package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
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

type Blocker struct {
	ctx     context.Context
	nc      *nats.Conn
	rdb     *redis.Client
	allowed atomic.Int64
	blocked atomic.Int64
	review  atomic.Int64
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var shutdown func()
	var err error

	tracer, shutdown, err = shared.InitTracer("blocker")
	if err != nil {
		log.Printf("[Blocker] WARN: трассировка недоступна: %v", err)
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
		log.Fatalf("[Blocker] Ошибка подключения к NATS: %v", err)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			log.Printf("[Blocker] WARN: drain error: %v", err)
		}
	}()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[Blocker] Ошибка подключения к Redis: %v", err)
	}

	log.Printf("[Blocker] Подключён к NATS: %s, Redis: %s", natsURL, redisAddr)

	b := &Blocker{ctx: ctx, nc: nc, rdb: rdb}

	sub, err := nc.QueueSubscribe(shared.SubjectTransactionsRisk, "blockers", func(msg *nats.Msg) {
		b.handleRisk(msg)
	})
	if err != nil {
		log.Fatalf("[Blocker] Ошибка подписки: %v", err)
	}
	defer sub.Unsubscribe()

	log.Printf("[Blocker] Слушает тему: %s", shared.SubjectTransactionsRisk)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	log.Printf("[Blocker] Завершение. Разрешено: %d, заблокировано: %d, на проверке: %d",
		b.allowed.Load(), b.blocked.Load(), b.review.Load())
}

func (b *Blocker) handleRisk(msg *nats.Msg) {
	ctx := shared.ExtractContext(b.ctx, msg)
	ctx, span := tracer.Start(ctx, "transaction.block_decision")
	defer span.End()

	var rr shared.RiskResult
	if err := json.Unmarshal(msg.Data, &rr); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid json")
		log.Printf("[Blocker] ERROR: невалидный JSON: %v", err)
		return
	}

	decision := b.decide(rr)

	span.SetAttributes(
		attribute.String("tx.id", decision.TransactionID),
		attribute.String("tx.account_id", decision.AccountID),
		attribute.String("tx.action", decision.Action),
		attribute.Float64("tx.risk_score", decision.RiskScore),
		attribute.String("tx.risk_level", decision.RiskLevel),
	)

	data, err := json.Marshal(decision)
	if err != nil {
		span.RecordError(err)
		log.Printf("[Blocker] ERROR: ошибка сериализации: %v", err)
		return
	}

	if err := shared.PublishWithContext(ctx, b.nc, shared.SubjectTransactionsDecision, data); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		log.Printf("[Blocker] ERROR: ошибка публикации: %v", err)
		return
	}

	b.saveDecision(decision)
	b.updateCounters(decision.Action)

	span.SetStatus(codes.Ok, decision.Action)
	log.Printf("[Blocker] INFO: txID=%s accountID=%s score=%.1f level=%s → %s",
		decision.TransactionID, decision.AccountID, decision.RiskScore, decision.RiskLevel, decision.Action)
}

func (b *Blocker) decide(rr shared.RiskResult) shared.Decision {
	tx := rr.Transaction
	var reasons []string
	action := "ALLOW"

	reasons = append(reasons, rr.Patterns.Patterns...)

	switch rr.RiskLevel {
	case "CRITICAL":
		action = "BLOCK"
		reasons = append(reasons, fmt.Sprintf("critical_risk_score:%.1f", rr.RiskScore))
		b.incrementBlockCount(tx.AccountID)
	case "HIGH":
		if b.previousBlockCount(tx.AccountID) > 0 {
			action = "BLOCK"
			reasons = append(reasons, "repeated_suspicious_activity")
		} else {
			action = "REVIEW"
			reasons = append(reasons, fmt.Sprintf("high_risk_score:%.1f", rr.RiskScore))
		}
	case "MEDIUM":
		action = "REVIEW"
		reasons = append(reasons, fmt.Sprintf("medium_risk_score:%.1f", rr.RiskScore))
	default:
		action = "ALLOW"
	}

	return shared.Decision{
		TransactionID: tx.ID,
		AccountID:     tx.AccountID,
		Action:        action,
		RiskScore:     rr.RiskScore,
		RiskLevel:     rr.RiskLevel,
		Reasons:       reasons,
		Timestamp:     time.Now(),
	}
}

func (b *Blocker) saveDecision(d shared.Decision) {
	key := fmt.Sprintf("decision:%s", d.TransactionID)
	data, _ := json.Marshal(d)
	b.rdb.Set(b.ctx, key, string(data), 7*24*time.Hour)

	listKey := "decisions:recent"
	b.rdb.LPush(b.ctx, listKey, string(data))
	b.rdb.LTrim(b.ctx, listKey, 0, 499)
	b.rdb.Expire(b.ctx, listKey, 24*time.Hour)

	b.rdb.Incr(b.ctx, fmt.Sprintf("stats:action:%s", d.Action))
	b.rdb.Incr(b.ctx, fmt.Sprintf("stats:level:%s", d.RiskLevel))
}

func (b *Blocker) incrementBlockCount(accountID string) {
	key := fmt.Sprintf("blocks:%s", accountID)
	b.rdb.Incr(b.ctx, key)
	b.rdb.Expire(b.ctx, key, 30*24*time.Hour)
}

func (b *Blocker) previousBlockCount(accountID string) int64 {
	key := fmt.Sprintf("blocks:%s", accountID)
	count, _ := b.rdb.Get(b.ctx, key).Int64()
	return count
}

func (b *Blocker) updateCounters(action string) {
	switch action {
	case "ALLOW":
		b.allowed.Add(1)
	case "BLOCK":
		b.blocked.Add(1)
	case "REVIEW":
		b.review.Add(1)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
