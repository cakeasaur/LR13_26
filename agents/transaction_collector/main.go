package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/nats-io/nats.go"
	"github.com/sleepysweety/fraud-detection/agents/shared"
)

var (
	processedCount atomic.Int64
	rejectedCount  atomic.Int64
	tracer         trace.Tracer
)

func main() {
	var shutdown func()
	var err error

	tracer, shutdown, err = shared.InitTracer("transaction-collector")
	if err != nil {
		log.Printf("[CollectorAgent] WARN: трассировка недоступна: %v", err)
		tracer = trace.NewNoopTracerProvider().Tracer("")
		shutdown = func() {}
	}
	defer shutdown()

	natsURL := getEnv("NATS_URL", nats.DefaultURL)
	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(10),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("[CollectorAgent] Ошибка подключения к NATS: %v", err)
	}
	defer nc.Close()

	log.Printf("[CollectorAgent] Подключён к NATS: %s", natsURL)

	sub, err := nc.QueueSubscribe(shared.SubjectTransactionsIncoming, "collectors", func(msg *nats.Msg) {
		handleTransaction(nc, msg)
	})
	if err != nil {
		log.Fatalf("[CollectorAgent] Ошибка подписки: %v", err)
	}
	defer sub.Unsubscribe()

	log.Printf("[CollectorAgent] Слушает тему: %s", shared.SubjectTransactionsIncoming)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("[CollectorAgent] Завершение. Обработано: %d, отклонено: %d",
		processedCount.Load(), rejectedCount.Load())
}

func handleTransaction(nc *nats.Conn, msg *nats.Msg) {
	_, span := tracer.Start(context.Background(), "transaction.collect")
	defer span.End()

	var tx shared.Transaction
	if err := json.Unmarshal(msg.Data, &tx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid json")
		log.Printf("[CollectorAgent] ERROR: невалидный JSON: %v", err)
		rejectedCount.Add(1)
		return
	}

	span.SetAttributes(
		attribute.String("tx.id", tx.ID),
		attribute.String("tx.account_id", tx.AccountID),
		attribute.Float64("tx.amount", tx.Amount),
		attribute.String("tx.currency", tx.Currency),
	)

	result := validate(tx)

	if !result.Valid {
		span.SetAttributes(attribute.String("tx.reject_reason", result.Reason))
		span.SetStatus(codes.Error, result.Reason)
		log.Printf("[CollectorAgent] REJECT txID=%s reason=%s", tx.ID, result.Reason)
		rejectedCount.Add(1)
		return
	}

	data, err := json.Marshal(result)
	if err != nil {
		span.RecordError(err)
		log.Printf("[CollectorAgent] ERROR: ошибка сериализации: %v", err)
		return
	}

	if err := nc.Publish(shared.SubjectTransactionsValidated, data); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		log.Printf("[CollectorAgent] ERROR: ошибка публикации: %v", err)
		return
	}

	span.SetStatus(codes.Ok, "validated")
	processedCount.Add(1)
	log.Printf("[CollectorAgent] INFO: txID=%s accountID=%s amount=%.2f %s → validated",
		tx.ID, tx.AccountID, tx.Amount, tx.Currency)
}

func validate(tx shared.Transaction) shared.ValidationResult {
	if tx.ID == "" {
		return shared.ValidationResult{Transaction: tx, Valid: false, Reason: "missing transaction id"}
	}
	if tx.AccountID == "" {
		return shared.ValidationResult{Transaction: tx, Valid: false, Reason: "missing account id"}
	}
	if tx.Amount <= 0 {
		return shared.ValidationResult{Transaction: tx, Valid: false, Reason: "amount must be positive"}
	}
	if tx.Amount > 1_000_000 {
		return shared.ValidationResult{Transaction: tx, Valid: false, Reason: "amount exceeds maximum limit"}
	}
	if tx.Currency == "" {
		return shared.ValidationResult{Transaction: tx, Valid: false, Reason: "missing currency"}
	}
	if !isValidCurrency(tx.Currency) {
		return shared.ValidationResult{Transaction: tx, Valid: false, Reason: "unsupported currency: " + tx.Currency}
	}
	if tx.Timestamp.IsZero() {
		tx.Timestamp = time.Now()
	}
	if time.Since(tx.Timestamp) > 24*time.Hour {
		return shared.ValidationResult{Transaction: tx, Valid: false, Reason: "transaction too old"}
	}
	return shared.ValidationResult{Transaction: tx, Valid: true}
}

func isValidCurrency(c string) bool {
	supported := []string{"USD", "EUR", "RUB", "GBP", "CNY", "JPY"}
	c = strings.ToUpper(c)
	for _, s := range supported {
		if s == c {
			return true
		}
	}
	return false
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
