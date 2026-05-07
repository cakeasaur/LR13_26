package main

import (
	"testing"
	"time"
)

func TestRiskLevel(t *testing.T) {
	tests := []struct {
		score float64
		want  string
	}{
		{0,    "LOW"},
		{29.9, "LOW"},
		{30,   "MEDIUM"},
		{54.9, "MEDIUM"},
		{55,   "HIGH"},
		{74.9, "HIGH"},
		{75,   "CRITICAL"},
		{100,  "CRITICAL"},
	}
	for _, tt := range tests {
		got := riskLevel(tt.score)
		if got != tt.want {
			t.Errorf("riskLevel(%.1f) = %q, want %q", tt.score, got, tt.want)
		}
	}
}

func TestNormalize(t *testing.T) {
	tests := []struct {
		value, min, max float64
		want            float64
	}{
		{0, 0, 100, 0},
		{100, 0, 100, 1},
		{50, 0, 100, 0.5},
		{-10, 0, 100, 0},   // clamp below min
		{150, 0, 100, 1},   // clamp above max
		{5, 5, 5, 0},       // min == max → 0
	}
	for _, tt := range tests {
		got := normalize(tt.value, tt.min, tt.max)
		if got != tt.want {
			t.Errorf("normalize(%v, %v, %v) = %v, want %v", tt.value, tt.min, tt.max, got, tt.want)
		}
	}
}

func TestTimeRisk(t *testing.T) {
	tests := []struct {
		hour int
		want float64
	}{
		{1,  70}, // 01:00 UTC — ночь
		{3,  70}, // 03:00 UTC
		{4,  70}, // 04:00 UTC
		{5,  10}, // 05:00 UTC — граница, дневное
		{12, 10}, // днём
		{22, 40}, // вечер
		{23, 40},
		{0,  40}, // полночь
	}
	for _, tt := range tests {
		ts := time.Date(2026, 1, 1, tt.hour, 0, 0, 0, time.UTC)
		got := timeRisk(ts)
		if got != tt.want {
			t.Errorf("timeRisk(hour=%d) = %.0f, want %.0f", tt.hour, got, tt.want)
		}
	}
}

func TestGeoRisk(t *testing.T) {
	a := &Assessor{}
	tests := []struct {
		country string
		want    float64
	}{
		{"NG", 90},
		{"RO", 75},
		{"CN", 40},
		{"RU", 40},
		{"US", 10}, // default
		{"DE", 10},
		{"",   10},
	}
	for _, tt := range tests {
		got := a.geoRisk(tt.country)
		if got != tt.want {
			t.Errorf("geoRisk(%q) = %.0f, want %.0f", tt.country, got, tt.want)
		}
	}
}
