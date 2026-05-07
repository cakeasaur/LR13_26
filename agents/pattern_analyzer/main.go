package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	"github.com/sleepysweety/fraud-detection/agents/shared"
)

var ctx = context.Background()

type Analyzer struct {
	nc  *nats.Conn
	rdb *redis.Client
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
	var vr shared.ValidationResult
	if err := json.Unmarshal(msg.Data, &vr); err != nil {
		log.Printf("[PatternAnalyzer] ERROR: невалидный JSON: %v", err)
		return
	}

	tx := vr.Transaction
	result := a.analyze(tx)

	data, err := json.Marshal(result)
	if err != nil {
		log.Printf("[PatternAnalyzer] ERROR: ошибка сериализации: %v", err)
		return
	}

	if err := a.nc.Publish(shared.SubjectTransactionsAnalyzed, data); err != nil {
		log.Printf("[PatternAnalyzer] ERROR: ошибка публикации: %v", err)
		return
	}

	log.Printf("[PatternAnalyzer] INFO: txID=%s suspicious=%v patterns=%v score=%.2f",
		tx.ID, result.Suspicious, result.Patterns, result.FrequencyScore)
}

func (a *Analyzer) analyze(tx shared.Transaction) shared.PatternResult {
	var patterns []string
	freqScore := 0.0
	amountDev := 0.0

	// 1. Частота транзакций за последние 60 секунд
	freqScore = a.checkFrequency(tx)
	if freqScore > 5 {
		patterns = append(patterns, fmt.Sprintf("high_frequency:%.0f_per_min", freqScore))
	}

	// 2. Отклонение суммы от средней по аккаунту
	amountDev = a.checkAmountDeviation(tx)
	if amountDev > 3.0 {
		patterns = append(patterns, fmt.Sprintf("amount_deviation:%.1fx", amountDev))
	}

	// 3. Ночные транзакции (00:00 - 05:00 UTC)
	hour := tx.Timestamp.UTC().Hour()
	if hour >= 0 && hour < 5 {
		patterns = append(patterns, "unusual_hour")
	}

	// 4. Крупная сумма (> 10000)
	if tx.Amount > 10000 {
		patterns = append(patterns, "large_amount")
	}

	// 5. Сохраняем транзакцию в историю
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

// checkFrequency возвращает кол-во транзакций от этого аккаунта за последние 60 сек
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

// checkAmountDeviation считает отклонение суммы от среднего по аккаунту
func (a *Analyzer) checkAmountDeviation(tx shared.Transaction) float64 {
	key := fmt.Sprintf("amounts:%s", tx.AccountID)

	// Добавляем текущую сумму
	a.rdb.LPush(ctx, key, tx.Amount)
	a.rdb.LTrim(ctx, key, 0, 99) // последние 100 транзакций
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

// saveToHistory сохраняет транзакцию в Redis для использования другими агентами
func (a *Analyzer) saveToHistory(tx shared.Transaction) {
	key := fmt.Sprintf("tx:history:%s", tx.AccountID)
	data, _ := json.Marshal(tx)
	a.rdb.LPush(ctx, key, string(data))
	a.rdb.LTrim(ctx, key, 0, 49) // последние 50 транзакций
	a.rdb.Expire(ctx, key, 48*time.Hour)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
