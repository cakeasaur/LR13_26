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

	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	"github.com/sleepysweety/fraud-detection/agents/shared"
)

var ctx = context.Background()

// Веса для каждого фактора риска
const (
	weightFrequency  = 0.30
	weightAmount     = 0.25
	weightPatterns   = 0.25
	weightGeo        = 0.10
	weightTime       = 0.10
)

// Пороги уровней риска
const (
	thresholdLow      = 30.0
	thresholdMedium   = 55.0
	thresholdHigh     = 75.0
)

type Assessor struct {
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
		log.Fatalf("[RiskAssessor] Ошибка подключения к NATS: %v", err)
	}
	defer nc.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[RiskAssessor] Ошибка подключения к Redis: %v", err)
	}

	log.Printf("[RiskAssessor] Подключён к NATS: %s, Redis: %s", natsURL, redisAddr)

	a := &Assessor{nc: nc, rdb: rdb}

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
	log.Println("[RiskAssessor] Завершение работы")
}

func (a *Assessor) handleAnalyzed(msg *nats.Msg) {
	var pr shared.PatternResult
	if err := json.Unmarshal(msg.Data, &pr); err != nil {
		log.Printf("[RiskAssessor] ERROR: невалидный JSON: %v", err)
		return
	}

	result := a.assess(pr)

	data, err := json.Marshal(result)
	if err != nil {
		log.Printf("[RiskAssessor] ERROR: ошибка сериализации: %v", err)
		return
	}

	if err := a.nc.Publish(shared.SubjectTransactionsRisk, data); err != nil {
		log.Printf("[RiskAssessor] ERROR: ошибка публикации: %v", err)
		return
	}

	log.Printf("[RiskAssessor] INFO: txID=%s score=%.1f level=%s",
		pr.Transaction.ID, result.RiskScore, result.RiskLevel)
}

func (a *Assessor) assess(pr shared.PatternResult) shared.RiskResult {
	tx := pr.Transaction

	// 1. Частота транзакций → 0–100
	freqScore := normalize(pr.FrequencyScore, 0, 20) * 100

	// 2. Отклонение суммы → 0–100
	amountScore := normalize(pr.AmountDeviation, 0, 10) * 100

	// 3. Паттерны → каждый +20 очков, макс 100
	patternScore := float64(len(pr.Patterns)) * 20
	if patternScore > 100 {
		patternScore = 100
	}

	// 4. Гео-риск по стране
	geoScore := a.geoRisk(tx.CountryCode)

	// 5. Временной риск
	timeScore := timeRisk(tx.Timestamp)

	// Взвешенная сумма
	riskScore := freqScore*weightFrequency +
		amountScore*weightAmount +
		patternScore*weightPatterns +
		geoScore*weightGeo +
		timeScore*weightTime

	// Учитываем историю блокировок аккаунта
	riskScore += a.accountPenalty(tx.AccountID)
	if riskScore > 100 {
		riskScore = 100
	}

	level := riskLevel(riskScore)

	// Сохраняем score в Redis для веб-панели
	a.saveRiskScore(tx, riskScore, level)

	return shared.RiskResult{
		Transaction: tx,
		Patterns:    pr,
		RiskScore:   riskScore,
		RiskLevel:   level,
	}
}

// geoRisk возвращает риск-балл по стране (0–100)
func (a *Assessor) geoRisk(country string) float64 {
	highRisk := map[string]float64{
		"NG": 90, "RO": 75, "PK": 70, "BD": 65,
		"VN": 60, "UA": 55, "BY": 55,
	}
	mediumRisk := map[string]float64{
		"CN": 40, "RU": 40, "IN": 35, "BR": 30,
	}
	if v, ok := highRisk[country]; ok {
		return v
	}
	if v, ok := mediumRisk[country]; ok {
		return v
	}
	return 10 // низкий риск по умолчанию
}

// timeRisk возвращает риск по часу транзакции (0–100)
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

// accountPenalty возвращает штраф за прошлые блокировки аккаунта
func (a *Assessor) accountPenalty(accountID string) float64 {
	key := fmt.Sprintf("blocks:%s", accountID)
	count, err := a.rdb.Get(ctx, key).Int64()
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
	a.rdb.HSet(ctx, key,
		"account_id", tx.AccountID,
		"score", fmt.Sprintf("%.2f", score),
		"level", level,
		"timestamp", tx.Timestamp.Format(time.RFC3339),
	)
	a.rdb.Expire(ctx, key, 24*time.Hour)
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
