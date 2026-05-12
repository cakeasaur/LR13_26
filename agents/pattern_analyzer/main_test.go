package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/sleepysweety/fraud-detection/agents/shared"
)

func TestDetectPatterns(t *testing.T) {
	day := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)   // днём — без unusual_hour
	night := time.Date(2026, 5, 7, 3, 0, 0, 0, time.UTC)  // ночью — unusual_hour
	dawn := time.Date(2026, 5, 7, 5, 0, 0, 0, time.UTC)   // граница — днём

	tests := []struct {
		name      string
		tx        shared.Transaction
		freqScore float64
		amountDev float64
		want      []string
	}{
		{
			name:      "clean transaction — no flags",
			tx:        shared.Transaction{Amount: 100, Timestamp: day},
			freqScore: 2,
			amountDev: 0.5,
			want:      nil,
		},
		{
			name:      "high frequency triggers flag",
			tx:        shared.Transaction{Amount: 100, Timestamp: day},
			freqScore: 6,
			amountDev: 0,
			want:      []string{"high_frequency:6_per_min"},
		},
		{
			name:      "frequency boundary not triggered at exactly 5",
			tx:        shared.Transaction{Amount: 100, Timestamp: day},
			freqScore: 5,
			amountDev: 0,
			want:      nil,
		},
		{
			name:      "amount deviation triggers flag",
			tx:        shared.Transaction{Amount: 100, Timestamp: day},
			freqScore: 0,
			amountDev: 4.2,
			want:      []string{"amount_deviation:4.2x"},
		},
		{
			name:      "amount deviation boundary not triggered at exactly 3",
			tx:        shared.Transaction{Amount: 100, Timestamp: day},
			freqScore: 0,
			amountDev: 3.0,
			want:      nil,
		},
		{
			name:      "night hour triggers unusual_hour",
			tx:        shared.Transaction{Amount: 100, Timestamp: night},
			freqScore: 0,
			amountDev: 0,
			want:      []string{"unusual_hour"},
		},
		{
			name:      "hour 5 is daytime, no unusual_hour flag",
			tx:        shared.Transaction{Amount: 100, Timestamp: dawn},
			freqScore: 0,
			amountDev: 0,
			want:      nil,
		},
		{
			name:      "large amount triggers flag",
			tx:        shared.Transaction{Amount: 15000, Timestamp: day},
			freqScore: 0,
			amountDev: 0,
			want:      []string{"large_amount"},
		},
		{
			name:      "amount boundary 10000 not triggered",
			tx:        shared.Transaction{Amount: 10000, Timestamp: day},
			freqScore: 0,
			amountDev: 0,
			want:      nil,
		},
		{
			name:      "multiple flags accumulate in fixed order",
			tx:        shared.Transaction{Amount: 50000, Timestamp: night},
			freqScore: 10,
			amountDev: 5.0,
			want: []string{
				"high_frequency:10_per_min",
				"amount_deviation:5.0x",
				"unusual_hour",
				"large_amount",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectPatterns(tt.tx, tt.freqScore, tt.amountDev)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("detectPatterns = %v, want %v", got, tt.want)
			}
		})
	}
}
