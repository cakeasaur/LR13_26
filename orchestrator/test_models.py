from datetime import datetime

from models import Decision, Transaction


def test_transaction_required_fields():
    tx = Transaction(id="tx-1", account_id="acc-1", amount=250.0, currency="USD")
    assert tx.id == "tx-1"
    assert tx.account_id == "acc-1"
    assert tx.amount == 250.0
    assert tx.currency == "USD"


def test_transaction_timestamp_auto_set():
    tx = Transaction(id="tx-1", account_id="acc-1", amount=100.0, currency="EUR")
    assert tx.timestamp is not None
    assert tx.timestamp.endswith("Z")
    datetime.fromisoformat(tx.timestamp.replace("Z", "+00:00"))  # должен парситься


def test_transaction_timestamp_explicit():
    ts = "2026-05-07T10:00:00Z"
    tx = Transaction(id="tx-1", account_id="acc-1", amount=100.0, currency="EUR", timestamp=ts)
    assert tx.timestamp == ts


def test_transaction_optional_defaults():
    tx = Transaction(id="tx-1", account_id="acc-1", amount=100.0, currency="USD")
    assert tx.merchant_id == ""
    assert tx.country_code == ""
    assert tx.ip_address == ""


def test_transaction_to_dict_keys():
    tx = Transaction(id="tx-1", account_id="acc-1", amount=99.9, currency="RUB",
                     merchant_id="shop", country_code="RU", ip_address="1.2.3.4")
    d = tx.to_dict()
    assert set(d.keys()) == {"id", "account_id", "amount", "currency",
                              "merchant_id", "country_code", "ip_address", "timestamp"}


def test_transaction_to_dict_values():
    tx = Transaction(id="tx-42", account_id="acc-99", amount=500.0, currency="GBP")
    d = tx.to_dict()
    assert d["id"] == "tx-42"
    assert d["account_id"] == "acc-99"
    assert d["amount"] == 500.0
    assert d["currency"] == "GBP"


def test_decision_defaults():
    d = Decision(transaction_id="tx-1", account_id="acc-1",
                 action="ALLOW", risk_score=15.0, risk_level="LOW")
    assert d.reasons == []
    assert d.timestamp == ""


def test_decision_with_reasons():
    d = Decision(transaction_id="tx-1", account_id="acc-1",
                 action="BLOCK", risk_score=90.0, risk_level="CRITICAL",
                 reasons=["high_frequency", "large_amount"])
    assert len(d.reasons) == 2
    assert "high_frequency" in d.reasons
