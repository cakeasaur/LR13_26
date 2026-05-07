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

BID_TIMEOUT = 0.3   # секунд на сбор ставок


class AuctionCoordinator:
    def __init__(self, nc, redis_client: aioredis.Redis):
        self.nc = nc
        self.redis = redis_client

    async def run(self):
        await self.nc.subscribe(SUBJECT_VALIDATED, cb=self._on_validated)
        log.info("AuctionCoordinator слушает %s", SUBJECT_VALIDATED)
        while True:
            await asyncio.sleep(1)

    async def _on_validated(self, msg):
        inbox = f"_INBOX.{uuid.uuid4().hex}"
        bids: list[dict] = []

        async def _collect(bid_msg):
            try:
                bids.append(json.loads(bid_msg.data.decode()))
            except Exception:
                pass

        sub = await self.nc.subscribe(inbox, cb=_collect)
        try:
            await self.nc.publish(SUBJECT_AUCTION, msg.data, reply=inbox)
            await asyncio.sleep(BID_TIMEOUT)
        finally:
            await sub.unsubscribe()

        if not bids:
            log.warning("Аукцион: нет участников, транзакция потеряна (запущен ли pattern_analyzer?)")
            return

        winner = min(bids, key=lambda b: b.get("load", 999))
        worker_id = winner["worker_id"]

        await self.nc.publish(WORKER_PREFIX + worker_id, msg.data)

        try:
            tx_id = json.loads(msg.data.decode()).get("transaction", {}).get("id", "?")
        except Exception:
            tx_id = "?"

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
