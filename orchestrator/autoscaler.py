import asyncio
import logging
import os

import docker
import redis.asyncio as aioredis
from docker.errors import DockerException, NotFound

log = logging.getLogger(__name__)

SCALE_UP_THRESHOLD = 5
SCALE_DOWN_THRESHOLD = 2
MAX_EXTRA_INSTANCES = 2  # сверх базового контейнера (запущенного compose'ом)
POLL_INTERVAL = 5  # секунд

DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "lr13_26_fraud-network")
NATS_URL  = os.getenv("NATS_URL",  "nats://nats:4222")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
REDIS_ADDR = os.getenv("REDIS_ADDR", "redis:6379")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4317")

# Имя Docker-образа совпадает с тем, что собирает docker-compose build
AGENT_IMAGES = {
    "transaction_collector": "lr13_26-transaction_collector",
    "pattern_analyzer":      "lr13_26-pattern_analyzer",
    "risk_assessor":         "lr13_26-risk_assessor",
    "blocker":               "lr13_26-blocker",
}

_AGENT_ENV = {
    "NATS_URL": NATS_URL,
    "REDIS_ADDR": REDIS_ADDR,
    "OTEL_EXPORTER_OTLP_ENDPOINT": OTEL_ENDPOINT,
}


class Autoscaler:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client
        try:
            self._docker = docker.from_env()
            self._docker.ping()
            log.info("Docker daemon доступен")
        except DockerException as exc:
            log.error("Не удалось подключиться к Docker daemon: %s", exc)
            self._docker = None
        # extra_ids[name] = [container_id, ...]
        self._extra: dict[str, list[str]] = {name: [] for name in AGENT_IMAGES}

    # ── Redis helpers ──────────────────────────────────────────────────────────

    async def _pending(self) -> int:
        val = await self.redis.get("autoscale:pending")
        return max(0, int(val) if val else 0)

    async def _save_counts(self):
        for name in AGENT_IMAGES:
            await self.redis.set(
                f"autoscale:instances:{name}",
                len(await asyncio.to_thread(self._alive, name)),
            )

    # ── Container helpers ──────────────────────────────────────────────────────

    def _alive(self, name: str) -> list[str]:
        """Возвращает IDs живых extra-контейнеров для агента, очищая список."""
        if self._docker is None:
            return []
        alive = []
        for cid in self._extra[name]:
            try:
                c = self._docker.containers.get(cid)
                if c.status in ("running", "created"):
                    alive.append(cid)
            except NotFound:
                pass  # контейнер уже удалён
        self._extra[name] = alive
        return alive

    def _spawn(self, name: str):
        if self._docker is None:
            log.error("Docker недоступен — scale-up невозможен")
            return
        image = AGENT_IMAGES[name]
        try:
            container = self._docker.containers.run(
                image=image,
                environment=_AGENT_ENV,
                network=DOCKER_NETWORK,
                detach=True,
                remove=True,  # авто-удаление при остановке
                labels={"fraud-detection.autoscaled": "true", "fraud-detection.agent": name},
            )
            self._extra[name].append(container.id)
            log.info("scale-up  %s  id=%s  (extra=%d)", name, container.short_id, len(self._extra[name]))
        except DockerException as exc:
            log.error("Ошибка запуска контейнера %s: %s", name, exc)

    def _kill_one(self, name: str):
        if self._docker is None:
            return
        alive = self._alive(name)
        if not alive:
            return
        cid = alive[-1]
        try:
            c = self._docker.containers.get(cid)
            c.stop(timeout=5)
            self._extra[name] = alive[:-1]
            log.info("scale-down %s  id=%s  (extra=%d)", name, cid[:12], len(self._extra[name]))
        except (NotFound, DockerException) as exc:
            log.warning("Ошибка остановки контейнера %s: %s", cid[:12], exc)
            self._extra[name] = alive[:-1]

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def tick(self):
        pending = await self._pending()
        log.info("pending=%d", pending)

        for name in AGENT_IMAGES:
            extra = len(await asyncio.to_thread(self._alive, name))
            if pending > SCALE_UP_THRESHOLD and extra < MAX_EXTRA_INSTANCES:
                await asyncio.to_thread(self._spawn, name)
            elif pending < SCALE_DOWN_THRESHOLD and extra > 0:
                await asyncio.to_thread(self._kill_one, name)

        await self._save_counts()

    async def run(self):
        log.info(
            "Autoscaler запущен  up_threshold=%d  down_threshold=%d  max_extra=%d",
            SCALE_UP_THRESHOLD, SCALE_DOWN_THRESHOLD, MAX_EXTRA_INSTANCES,
        )
        while True:
            try:
                await self.tick()
            except Exception:
                log.exception("Ошибка тика")
            await asyncio.sleep(POLL_INTERVAL)

    def shutdown(self):
        if self._docker is None:
            return
        for name in AGENT_IMAGES:
            for cid in self._extra[name]:
                try:
                    c = self._docker.containers.get(cid)
                    c.stop(timeout=5)
                except (NotFound, DockerException):
                    pass
        log.info("Autoscaler остановлен, все доп. контейнеры завершены")


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
    finally:
        scaler.shutdown()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
