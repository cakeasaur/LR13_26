import asyncio
import json
import logging
import os
from typing import Dict, Optional

import nats
from nats.aio.client import Client as NATS

from models import Transaction, Decision

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Orchestrator] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

SUBJECT_INCOMING = "transactions.incoming"
SUBJECT_DECISION = "transactions.decision"

MAX_RETRIES = 3
TASK_TIMEOUT = 30  # секунд


class Orchestrator:
    def __init__(self):
        self.nc: Optional[NATS] = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._sub = None

    async def connect(self):
        nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
        self.nc = await nats.connect(
            nats_url,
            reconnect_time_wait=2,
            max_reconnect_attempts=10,
        )
        self._sub = await self.nc.subscribe(SUBJECT_DECISION, cb=self._on_decision)
        log.info("Подключён к NATS: %s", nats_url)

    async def disconnect(self):
        if self._sub:
            await self._sub.unsubscribe()
        if self.nc:
            await self.nc.drain()

    async def submit(self, tx: Transaction, retries: int = MAX_RETRIES) -> Optional[Decision]:
        """Отправляет транзакцию в pipeline и ожидает решение."""
        for attempt in range(1, retries + 1):
            try:
                return await self._send_once(tx)
            except asyncio.TimeoutError:
                log.warning("Таймаут для txID=%s (попытка %d/%d)", tx.id, attempt, retries)
                if attempt == retries:
                    log.error("Транзакция txID=%s не обработана после %d попыток", tx.id, retries)
                    return None
                await asyncio.sleep(1)
        return None

    async def _send_once(self, tx: Transaction) -> Decision:
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[tx.id] = future

        payload = json.dumps(tx.to_dict()).encode()
        await self.nc.publish(SUBJECT_INCOMING, payload)
        log.info("Отправлена txID=%s accountID=%s amount=%.2f %s",
                 tx.id, tx.account_id, tx.amount, tx.currency)

        try:
            raw = await asyncio.wait_for(future, timeout=TASK_TIMEOUT)
            data = json.loads(raw)
            return Decision(
                transaction_id=data["transaction_id"],
                account_id=data["account_id"],
                action=data["action"],
                risk_score=data["risk_score"],
                risk_level=data["risk_level"],
                reasons=data.get("reasons", []),
                timestamp=data.get("timestamp", ""),
            )
        finally:
            self._pending.pop(tx.id, None)

    async def _on_decision(self, msg):
        try:
            data = json.loads(msg.data.decode())
            tx_id = data.get("transaction_id")
            if tx_id and tx_id in self._pending:
                future = self._pending[tx_id]
                if not future.done():
                    future.set_result(msg.data.decode())
                log.info("Решение получено: txID=%s action=%s score=%.1f level=%s",
                         tx_id, data.get("action"), data.get("risk_score", 0), data.get("risk_level"))
        except Exception as e:
            log.error("Ошибка обработки решения: %s", e)
