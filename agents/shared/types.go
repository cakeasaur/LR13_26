package shared

import "time"

// NATS subjects
const (
	SubjectTransactionsIncoming  = "transactions.incoming"
	SubjectTransactionsValidated = "transactions.validated"
	SubjectTransactionsAnalyzed  = "transactions.analyzed"
	SubjectTransactionsRisk      = "transactions.risk"
	SubjectTransactionsDecision = "transactions.decision"
)

type Transaction struct {
	ID          string    `json:"id"`
	AccountID   string    `json:"account_id"`
	Amount      float64   `json:"amount"`
	Currency    string    `json:"currency"`
	MerchantID  string    `json:"merchant_id"`
	CountryCode string    `json:"country_code"`
	IPAddress   string    `json:"ip_address"`
	Timestamp   time.Time `json:"timestamp"`
}

type ValidationResult struct {
	Transaction Transaction `json:"transaction"`
	Valid        bool        `json:"valid"`
	Reason       string      `json:"reason,omitempty"`
}

type PatternResult struct {
	Transaction     Transaction `json:"transaction"`
	Suspicious      bool        `json:"suspicious"`
	Patterns        []string    `json:"patterns"`
	FrequencyScore  float64     `json:"frequency_score"`
	AmountDeviation float64     `json:"amount_deviation"`
}

type RiskResult struct {
	Transaction Transaction `json:"transaction"`
	Patterns    PatternResult `json:"patterns"`
	RiskScore   float64     `json:"risk_score"`
	RiskLevel   string      `json:"risk_level"` // LOW, MEDIUM, HIGH, CRITICAL
}

type Decision struct {
	TransactionID string      `json:"transaction_id"`
	AccountID     string      `json:"account_id"`
	Action        string      `json:"action"` // ALLOW, BLOCK, REVIEW
	RiskScore     float64     `json:"risk_score"`
	RiskLevel     string      `json:"risk_level"`
	Reasons       []string    `json:"reasons"`
	Timestamp     time.Time   `json:"timestamp"`
}
