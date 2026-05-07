import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from orchestrator import Orchestrator

log = logging.getLogger(__name__)

orch: Optional[Orchestrator] = None
rdb: Optional[aioredis.Redis] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global orch, rdb
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    rdb = aioredis.from_url(redis_url, decode_responses=True)
    orch = Orchestrator(redis=rdb)
    await orch.connect()
    log.info("API запущен")
    yield
    await orch.disconnect()
    await rdb.aclose()
    log.info("API остановлен")


app = FastAPI(
    title="Fraud Detection API",
    description="REST API для системы борьбы с мошенничеством",
    version="1.0.0",
    lifespan=lifespan,
)


class TransactionRequest(BaseModel):
    account_id: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0)
    currency: str = Field(..., min_length=3, max_length=3)
    merchant_id: str = ""
    country_code: str = ""
    ip_address: str = ""

    @field_validator("currency")
    @classmethod
    def currency_upper(cls, v):
        return v.upper()


class DecisionResponse(BaseModel):
    transaction_id: str
    account_id: str
    action: str
    risk_score: float
    risk_level: str
    reasons: List[str]
    timestamp: str


class StatsResponse(BaseModel):
    allowed: int
    blocked: int
    review: int
    total: int


@app.post("/transactions", response_model=DecisionResponse, summary="Отправить транзакцию на проверку")
async def submit_transaction(req: TransactionRequest):
    from models import Transaction

    tx = Transaction(
        id=str(uuid.uuid4()),
        account_id=req.account_id,
        amount=req.amount,
        currency=req.currency,
        merchant_id=req.merchant_id,
        country_code=req.country_code,
        ip_address=req.ip_address,
        timestamp=datetime.utcnow().isoformat() + "Z",
    )

    decision = await orch.submit(tx)
    if decision is None:
        raise HTTPException(status_code=504, detail="Агенты не ответили. Проверьте, что все сервисы запущены.")

    return DecisionResponse(
        transaction_id=decision.transaction_id,
        account_id=decision.account_id,
        action=decision.action,
        risk_score=decision.risk_score,
        risk_level=decision.risk_level,
        reasons=decision.reasons,
        timestamp=decision.timestamp,
    )


@app.get("/transactions/{tx_id}", response_model=DecisionResponse, summary="Получить решение по транзакции")
async def get_decision(tx_id: str):
    key = f"decision:{tx_id}"
    raw = await rdb.get(key)
    if not raw:
        raise HTTPException(status_code=404, detail="Транзакция не найдена")

    data = json.loads(raw)
    return DecisionResponse(**data)


@app.get("/stats", response_model=StatsResponse, summary="Статистика решений")
async def get_stats():
    allowed = int(await rdb.get("stats:action:ALLOW") or 0)
    blocked = int(await rdb.get("stats:action:BLOCK") or 0)
    review  = int(await rdb.get("stats:action:REVIEW") or 0)
    return StatsResponse(
        allowed=allowed,
        blocked=blocked,
        review=review,
        total=allowed + blocked + review,
    )


@app.get("/autoscale", summary="Статус автомасштабирования")
async def autoscale_status():
    pending = int(await rdb.get("autoscale:pending") or 0)
    agents = ["transaction_collector", "pattern_analyzer", "risk_assessor", "blocker"]
    instances = {}
    for name in agents:
        instances[name] = int(await rdb.get(f"autoscale:instances:{name}") or 0)
    return {"pending": pending, "instances": instances}


@app.get("/health", summary="Health check")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}
