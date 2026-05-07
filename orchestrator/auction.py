import asyncio
import json
import logging
import os
import uuid

import nats
import redis.asyncio as aioredis

log = logging.getLogger(__name__)

SUBJECT_VALIDATED = "transactions.validated"
SUBJECT_AUCTION   = "transactions.auction"
WORKER_PREFIX     = "transactions.worker."

BID_TIMEOUT = 0.3      # секунд на сбор ставок (первая попытка)
MAX_RETRIES = 3        # перепровести аукцион при отсутствии ставок
DEAD_LETTER_KEY = "auction:dead_letter"  # потерянные транзакции


class AuctionCoordinator:
    def __init__(self, nc, redis_client: aioredis.Redis):
        self.nc = nc
        self.redis = redis_client

    async def run(self):
        await self.nc.subscribe(SUBJECT_VALIDATED, cb=self._on_validated)
        log.info("AuctionCoordinator слушает %s", SUBJECT_VALIDATED)
        while True:
            await asyncio.sleep(1)

    async def _collect_bids(self, payload: bytes, timeout: float) -> list[dict]:
        inbox = f"_INBOX.{uuid.uuid4().hex}"
        bids: list[dict] = []

        async def _collect(bid_msg):
            try:
                bids.append(json.loads(bid_msg.data.decode()))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.debug("Невалидная ставка: %s", e)

        sub = await self.nc.subscribe(inbox, cb=_collect)
        try:
            await self.nc.publish(SUBJECT_AUCTION, payload, reply=inbox)
            await asyncio.sleep(timeout)
        finally:
            await sub.unsubscribe()
        return bids

    async def _on_validated(self, msg):
        try:
            tx_id = json.loads(msg.data.decode()).get("transaction", {}).get("id", "?")
        except (json.JSONDecodeError, UnicodeDecodeError):
            tx_id = "?"

        bids: list[dict] = []
        for attempt in range(1, MAX_RETRIES + 1):
            timeout = BID_TIMEOUT * attempt   # 0.3s, 0.6s, 0.9s
            bids = await self._collect_bids(msg.data, timeout)
            if bids:
                break
            log.warning(
                "Аукцион: нет ставок (попытка %d/%d, timeout=%.1fs) txID=%s",
                attempt, MAX_RETRIES, timeout, tx_id,
            )

        if not bids:
            log.error("Аукцион: транзакция txID=%s в dead letter", tx_id)
            await self.redis.lpush(DEAD_LETTER_KEY, msg.data)
            await self.redis.ltrim(DEAD_LETTER_KEY, 0, 999)
            await self.redis.incr("auction:dead_letter:count")
            return

        winner = min(bids, key=lambda b: b.get("load", 999))
        worker_id = winner["worker_id"]

        await self.nc.publish(WORKER_PREFIX + worker_id, msg.data)

        log.info(
            "Аукцион: txID=%s победитель=%s load=%d участников=%d",
            tx_id, worker_id[:8], winner.get("load", 0), len(bids),
        )

        await self.redis.incr("auction:total")
        await self.redis.incr(f"auction:wins:{worker_id[:8]}")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [Auction] %(levelname)s: %(message)s",
    )
    nats_url  = os.getenv("NATS_URL",  "nats://localhost:4222")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

    nc = await nats.connect(
        nats_url,
        reconnect_time_wait=2,
        max_reconnect_attempts=10,
    )
    r = aioredis.from_url(redis_url)

    coordinator = AuctionCoordinator(nc, r)
    try:
        await coordinator.run()
    finally:
        await nc.drain()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
