"""
Точка входа оркестратора. Демонстрирует работу pipeline:
отправляет тестовые транзакции и выводит решения.
"""
import asyncio
import logging
import uuid
from datetime import datetime, timezone

from orchestrator import Orchestrator
from models import Transaction

log = logging.getLogger(__name__)


def make_tx(**kwargs) -> Transaction:
    return Transaction(
        id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat(),
        **kwargs,
    )


async def run_demo():
    orch = Orchestrator()
    await orch.connect()

    test_cases = [
        make_tx(account_id="acc_001", amount=150.0,    currency="USD", country_code="US", merchant_id="shop_01"),
        make_tx(account_id="acc_002", amount=50000.0,  currency="USD", country_code="NG", merchant_id="shop_02"),
        make_tx(account_id="acc_003", amount=9999.99,  currency="EUR", country_code="RO", merchant_id="shop_03"),
        make_tx(account_id="acc_001", amount=155.0,    currency="USD", country_code="US", merchant_id="shop_01"),
        make_tx(account_id="acc_004", amount=-100.0,   currency="RUB", country_code="RU", merchant_id="shop_04"),
        make_tx(account_id="acc_005", amount=0,        currency="EUR", country_code="DE", merchant_id="shop_05"),
    ]

    print("\n" + "="*60)
    print("  FRAUD DETECTION SYSTEM — DEMO PIPELINE")
    print("="*60 + "\n")

    for tx in test_cases:
        decision = await orch.submit(tx)
        if decision:
            icon = {"ALLOW": "✅", "REVIEW": "⚠️", "BLOCK": "🚫"}.get(decision.action, "❓")
            print(f"{icon}  txID={decision.transaction_id[:8]}... "
                  f"account={decision.account_id} "
                  f"score={decision.risk_score:.1f} "
                  f"level={decision.risk_level} "
                  f"action={decision.action}")
            if decision.reasons:
                print(f"     reasons: {', '.join(decision.reasons)}")
        else:
            print(f"⏱️  txID={tx.id[:8]}... — нет ответа (агенты не запущены?)")
        print()

    await orch.disconnect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    asyncio.run(run_demo())
