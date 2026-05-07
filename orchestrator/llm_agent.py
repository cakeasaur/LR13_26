import asyncio
import json
import logging
import os

import httpx
import nats
import redis.asyncio as aioredis

log = logging.getLogger(__name__)

SUBJECT_DECISION = "transactions.decision"
OLLAMA_URL       = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL     = os.getenv("OLLAMA_MODEL", "llama3.2")
OLLAMA_TIMEOUT   = 60  # секунд
EXPLANATION_TTL  = 24 * 3600  # секунд


def build_prompt(decision: dict) -> str:
    reasons = ", ".join(decision.get("reasons", [])) or "none"
    return (
        "You are a fraud detection analyst. Write a concise 2-3 sentence explanation "
        "of the following decision for a customer service representative.\n\n"
        f"Transaction ID : {decision['transaction_id']}\n"
        f"Account        : {decision['account_id']}\n"
        f"Decision       : {decision['action']}\n"
        f"Risk level     : {decision['risk_level']} (score {decision['risk_score']:.1f}/100)\n"
        f"Risk factors   : {reasons}\n\n"
        "Be professional and factual. Do not invent details not listed above."
    )


async def call_ollama(prompt: str) -> str:
    async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
        resp = await client.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        )
        resp.raise_for_status()
        return resp.json()["response"].strip()


class LLMAgent:
    def __init__(self, nc, redis_client: aioredis.Redis):
        self.nc    = nc
        self.redis = redis_client

    async def run(self):
        await self.nc.subscribe(SUBJECT_DECISION, cb=self._on_decision)
        log.info("LLMAgent слушает %s  model=%s", SUBJECT_DECISION, OLLAMA_MODEL)
        while True:
            await asyncio.sleep(1)

    async def _on_decision(self, msg):
        # Fire-and-forget: не блокируем NATS-колбек
        asyncio.create_task(self._explain(msg.data))

    async def _explain(self, raw: bytes):
        try:
            decision = json.loads(raw.decode())
        except Exception as exc:
            log.error("LLMAgent: невалидный JSON: %s", exc)
            return

        tx_id = decision.get("transaction_id", "?")

        # Пропускаем, если объяснение уже есть
        if await self.redis.exists(f"explanation:{tx_id}"):
            return

        try:
            prompt = build_prompt(decision)
            explanation = await call_ollama(prompt)
        except httpx.TimeoutException:
            log.warning("LLMAgent: таймаут Ollama для txID=%s", tx_id)
            return
        except Exception as exc:
            log.error("LLMAgent: ошибка Ollama для txID=%s: %s", tx_id, exc)
            return

        await self.redis.set(f"explanation:{tx_id}", explanation, ex=EXPLANATION_TTL)
        log.info("LLMAgent: txID=%s action=%s → объяснение сохранено (%d символов)",
                 tx_id, decision.get("action"), len(explanation))


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [LLMAgent] %(levelname)s: %(message)s",
    )
    nats_url  = os.getenv("NATS_URL",  "nats://localhost:4222")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

    nc = await nats.connect(
        nats_url,
        reconnect_time_wait=2,
        max_reconnect_attempts=10,
    )
    r = aioredis.from_url(redis_url)

    agent = LLMAgent(nc, r)
    try:
        await agent.run()
    finally:
        await nc.drain()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
