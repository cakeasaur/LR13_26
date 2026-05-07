package main

import (
	"testing"
	"time"

	"github.com/sleepysweety/fraud-detection/agents/shared"
)

func TestIsValidCurrency(t *testing.T) {
	tests := []struct {
		currency string
		want     bool
	}{
		{"USD", true},
		{"EUR", true},
		{"RUB", true},
		{"GBP", true},
		{"CNY", true},
		{"JPY", true},
		{"usd", true}, // lowercase normalised
		{"BTC", false},
		{"XXX", false},
		{"",    false},
	}
	for _, tt := range tests {
		got := isValidCurrency(tt.currency)
		if got != tt.want {
			t.Errorf("isValidCurrency(%q) = %v, want %v", tt.currency, got, tt.want)
		}
	}
}

func TestValidate(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		tx      shared.Transaction
		wantOK  bool
		wantMsg string
	}{
		{
			name:   "valid transaction",
			tx:     shared.Transaction{ID: "tx1", AccountID: "acc1", Amount: 100, Currency: "USD", Timestamp: now},
			wantOK: true,
		},
		{
			name:    "missing id",
			tx:      shared.Transaction{AccountID: "acc1", Amount: 100, Currency: "USD", Timestamp: now},
			wantOK:  false,
			wantMsg: "missing transaction id",
		},
		{
			name:    "missing account",
			tx:      shared.Transaction{ID: "tx1", Amount: 100, Currency: "USD", Timestamp: now},
			wantOK:  false,
			wantMsg: "missing account id",
		},
		{
			name:    "zero amount",
			tx:      shared.Transaction{ID: "tx1", AccountID: "acc1", Amount: 0, Currency: "USD", Timestamp: now},
			wantOK:  false,
			wantMsg: "amount must be positive",
		},
		{
			name:    "negative amount",
			tx:      shared.Transaction{ID: "tx1", AccountID: "acc1", Amount: -50, Currency: "USD", Timestamp: now},
			wantOK:  false,
			wantMsg: "amount must be positive",
		},
		{
			name:    "amount exceeds limit",
			tx:      shared.Transaction{ID: "tx1", AccountID: "acc1", Amount: 1_000_001, Currency: "USD", Timestamp: now},
			wantOK:  false,
			wantMsg: "amount exceeds maximum limit",
		},
		{
			name:    "missing currency",
			tx:      shared.Transaction{ID: "tx1", AccountID: "acc1", Amount: 100, Timestamp: now},
			wantOK:  false,
			wantMsg: "missing currency",
		},
		{
			name:    "unsupported currency",
			tx:      shared.Transaction{ID: "tx1", AccountID: "acc1", Amount: 100, Currency: "BTC", Timestamp: now},
			wantOK:  false,
			wantMsg: "unsupported currency: BTC",
		},
		{
			name:    "timestamp too old",
			tx:      shared.Transaction{ID: "tx1", AccountID: "acc1", Amount: 100, Currency: "USD", Timestamp: now.Add(-25 * time.Hour)},
			wantOK:  false,
			wantMsg: "transaction too old",
		},
		{
			name:   "zero timestamp gets filled",
			tx:     shared.Transaction{ID: "tx1", AccountID: "acc1", Amount: 100, Currency: "USD"},
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validate(tt.tx)
			if result.Valid != tt.wantOK {
				t.Errorf("valid = %v, want %v (reason: %s)", result.Valid, tt.wantOK, result.Reason)
			}
			if tt.wantMsg != "" && result.Reason != tt.wantMsg {
				t.Errorf("reason = %q, want %q", result.Reason, tt.wantMsg)
			}
		})
	}
}
