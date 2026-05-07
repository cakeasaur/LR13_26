import asyncio
import logging
import os
import signal
import subprocess

import redis.asyncio as aioredis

log = logging.getLogger(__name__)

SCALE_UP_THRESHOLD = 5
SCALE_DOWN_THRESHOLD = 2
MAX_EXTRA_INSTANCES = 2  # сверх базового (запущенного вручную)
POLL_INTERVAL = 5  # секунд

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

AGENTS = {
    "transaction_collector": os.path.join(_BASE_DIR, "bin", "transaction_collector"),
    "pattern_analyzer":      os.path.join(_BASE_DIR, "bin", "pattern_analyzer"),
    "risk_assessor":         os.path.join(_BASE_DIR, "bin", "risk_assessor"),
    "blocker":               os.path.join(_BASE_DIR, "bin", "blocker"),
}


class Autoscaler:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client
        self._extra: dict[str, list[subprocess.Popen]] = {name: [] for name in AGENTS}

    async def _pending(self) -> int:
        val = await self.redis.get("autoscale:pending")
        return max(0, int(val) if val else 0)

    def _alive(self, name: str) -> list[subprocess.Popen]:
        alive = [p for p in self._extra[name] if p.poll() is None]
        self._extra[name] = alive
        return alive

    def _spawn(self, name: str):
        proc = subprocess.Popen(
            [AGENTS[name]],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._extra[name].append(proc)
        log.info("scale-up  %s  pid=%d  (extra=%d)", name, proc.pid, len(self._extra[name]))

    def _kill_one(self, name: str):
        alive = self._alive(name)
        if not alive:
            return
        proc = alive[-1]
        proc.send_signal(signal.SIGTERM)
        self._extra[name] = alive[:-1]
        log.info("scale-down %s  pid=%d  (extra=%d)", name, proc.pid, len(self._extra[name]))

    async def _save_counts(self):
        for name in AGENTS:
            count = len(self._alive(name))
            await self.redis.set(f"autoscale:instances:{name}", count)

    async def tick(self):
        pending = await self._pending()
        log.info("pending=%d", pending)

        for name in AGENTS:
            extra = len(self._alive(name))
            if pending > SCALE_UP_THRESHOLD and extra < MAX_EXTRA_INSTANCES:
                self._spawn(name)
            elif pending < SCALE_DOWN_THRESHOLD and extra > 0:
                self._kill_one(name)

        await self._save_counts()

    async def run(self):
        log.info(
            "Autoscaler запущен  up_threshold=%d  down_threshold=%d  max_extra=%d",
            SCALE_UP_THRESHOLD, SCALE_DOWN_THRESHOLD, MAX_EXTRA_INSTANCES,
        )
        while True:
            try:
                await self.tick()
            except Exception as exc:
                log.error("Ошибка тика: %s", exc)
            await asyncio.sleep(POLL_INTERVAL)

    def shutdown(self):
        for name in AGENTS:
            for proc in self._extra[name]:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
        log.info("Autoscaler остановлен, все доп. процессы завершены")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [Autoscaler] %(levelname)s: %(message)s",
    )
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    r = aioredis.from_url(redis_url)
    scaler = Autoscaler(r)
    try:
        await scaler.run()
    except KeyboardInterrupt:
        scaler.shutdown()
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
