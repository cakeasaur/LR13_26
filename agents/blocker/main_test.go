package main

import (
	"reflect"
	"testing"
)

func TestDecideAction(t *testing.T) {
	tests := []struct {
		name           string
		riskLevel      string
		riskScore      float64
		prevBlockCount int64
		wantAction     string
		wantReasons    []string
	}{
		{
			name:        "critical always blocks",
			riskLevel:   "CRITICAL",
			riskScore:   88.5,
			wantAction:  "BLOCK",
			wantReasons: []string{"critical_risk_score:88.5"},
		},
		{
			name:        "high without prior blocks goes to review",
			riskLevel:   "HIGH",
			riskScore:   65.0,
			wantAction:  "REVIEW",
			wantReasons: []string{"high_risk_score:65.0"},
		},
		{
			name:           "high with prior blocks escalates to block",
			riskLevel:      "HIGH",
			riskScore:      65.0,
			prevBlockCount: 2,
			wantAction:     "BLOCK",
			wantReasons:    []string{"repeated_suspicious_activity"},
		},
		{
			name:        "medium goes to review",
			riskLevel:   "MEDIUM",
			riskScore:   42.0,
			wantAction:  "REVIEW",
			wantReasons: []string{"medium_risk_score:42.0"},
		},
		{
			name:        "low allows",
			riskLevel:   "LOW",
			riskScore:   12.0,
			wantAction:  "ALLOW",
			wantReasons: nil,
		},
		{
			name:        "unknown level allows",
			riskLevel:   "",
			riskScore:   0,
			wantAction:  "ALLOW",
			wantReasons: nil,
		},
		{
			name:           "high with single prior block also blocks",
			riskLevel:      "HIGH",
			riskScore:      70.0,
			prevBlockCount: 1,
			wantAction:     "BLOCK",
			wantReasons:    []string{"repeated_suspicious_activity"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAction, gotReasons := decideAction(tt.riskLevel, tt.riskScore, tt.prevBlockCount)
			if gotAction != tt.wantAction {
				t.Errorf("action = %q, want %q", gotAction, tt.wantAction)
			}
			if !reflect.DeepEqual(gotReasons, tt.wantReasons) {
				t.Errorf("reasons = %v, want %v", gotReasons, tt.wantReasons)
			}
		})
	}
}
