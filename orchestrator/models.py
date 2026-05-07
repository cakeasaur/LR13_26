from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class Transaction:
    id: str
    account_id: str
    amount: float
    currency: str
    merchant_id: str = ""
    country_code: str = ""
    ip_address: str = ""
    timestamp: Optional[str] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "account_id": self.account_id,
            "amount": self.amount,
            "currency": self.currency,
            "merchant_id": self.merchant_id,
            "country_code": self.country_code,
            "ip_address": self.ip_address,
            "timestamp": self.timestamp,
        }


@dataclass
class Decision:
    transaction_id: str
    account_id: str
    action: str          # ALLOW, BLOCK, REVIEW
    risk_score: float
    risk_level: str      # LOW, MEDIUM, HIGH, CRITICAL
    reasons: list = field(default_factory=list)
    timestamp: str = ""
