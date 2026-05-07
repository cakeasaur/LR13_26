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

	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	"github.com/sleepysweety/fraud-detection/agents/shared"
)

var ctx = context.Background()

type Blocker struct {
	nc      *nats.Conn
	rdb     *redis.Client
	allowed atomic.Int64
	blocked atomic.Int64
	review  atomic.Int64
}

func main() {
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
	defer nc.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[Blocker] Ошибка подключения к Redis: %v", err)
	}

	log.Printf("[Blocker] Подключён к NATS: %s, Redis: %s", natsURL, redisAddr)

	b := &Blocker{nc: nc, rdb: rdb}

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
	log.Printf("[Blocker] Завершение. Разрешено: %d, заблокировано: %d, на проверке: %d",
		b.allowed.Load(), b.blocked.Load(), b.review.Load())
}

func (b *Blocker) handleRisk(msg *nats.Msg) {
	var rr shared.RiskResult
	if err := json.Unmarshal(msg.Data, &rr); err != nil {
		log.Printf("[Blocker] ERROR: невалидный JSON: %v", err)
		return
	}

	decision := b.decide(rr)

	data, err := json.Marshal(decision)
	if err != nil {
		log.Printf("[Blocker] ERROR: ошибка сериализации: %v", err)
		return
	}

	if err := b.nc.Publish(shared.SubjectTransactionsDecision, data); err != nil {
		log.Printf("[Blocker] ERROR: ошибка публикации: %v", err)
		return
	}

	b.saveDecision(decision)
	b.updateCounters(decision.Action)

	log.Printf("[Blocker] INFO: txID=%s accountID=%s score=%.1f level=%s → %s",
		decision.TransactionID, decision.AccountID, decision.RiskScore, decision.RiskLevel, decision.Action)
}

func (b *Blocker) decide(rr shared.RiskResult) shared.Decision {
	tx := rr.Transaction
	var reasons []string
	action := "ALLOW"

	// Собираем причины из паттернов
	reasons = append(reasons, rr.Patterns.Patterns...)

	switch rr.RiskLevel {
	case "CRITICAL":
		action = "BLOCK"
		reasons = append(reasons, fmt.Sprintf("critical_risk_score:%.1f", rr.RiskScore))
		b.incrementBlockCount(tx.AccountID)

	case "HIGH":
		// Если аккаунт уже был заблокирован — блокируем сразу
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
	// Сохраняем решение в Redis
	key := fmt.Sprintf("decision:%s", d.TransactionID)
	data, _ := json.Marshal(d)
	b.rdb.Set(ctx, key, string(data), 7*24*time.Hour)

	// Добавляем в общий список решений для веб-панели
	listKey := "decisions:recent"
	b.rdb.LPush(ctx, listKey, string(data))
	b.rdb.LTrim(ctx, listKey, 0, 499) // последние 500
	b.rdb.Expire(ctx, listKey, 24*time.Hour)

	// Счётчики по действиям
	b.rdb.Incr(ctx, fmt.Sprintf("stats:action:%s", d.Action))
	b.rdb.Incr(ctx, fmt.Sprintf("stats:level:%s", d.RiskLevel))
}

func (b *Blocker) incrementBlockCount(accountID string) {
	key := fmt.Sprintf("blocks:%s", accountID)
	b.rdb.Incr(ctx, key)
	b.rdb.Expire(ctx, key, 30*24*time.Hour)
}

func (b *Blocker) previousBlockCount(accountID string) int64 {
	key := fmt.Sprintf("blocks:%s", accountID)
	count, _ := b.rdb.Get(ctx, key).Int64()
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
