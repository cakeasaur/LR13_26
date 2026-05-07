import json
import os
import time
from datetime import datetime

import pandas as pd
import redis
import streamlit as st

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
RECENT_LIMIT = 50

ACTION_EMOJI = {"ALLOW": "✅", "BLOCK": "🚫", "REVIEW": "⚠️"}
LEVEL_EMOJI  = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}


@st.cache_resource
def get_redis() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)


def fetch_stats(r: redis.Redis) -> dict:
    allowed  = int(r.get("stats:action:ALLOW")    or 0)
    blocked  = int(r.get("stats:action:BLOCK")    or 0)
    review   = int(r.get("stats:action:REVIEW")   or 0)
    low      = int(r.get("stats:level:LOW")       or 0)
    medium   = int(r.get("stats:level:MEDIUM")    or 0)
    high     = int(r.get("stats:level:HIGH")      or 0)
    critical = int(r.get("stats:level:CRITICAL")  or 0)
    return dict(allowed=allowed, blocked=blocked, review=review,
                low=low, medium=medium, high=high, critical=critical)


def fetch_recent(r: redis.Redis) -> list[dict]:
    rows = r.lrange("decisions:recent", 0, RECENT_LIMIT - 1)
    result = []
    for raw in rows:
        try:
            result.append(json.loads(raw))
        except Exception:
            pass
    return result


def fetch_autoscale(r: redis.Redis) -> dict:
    pending = max(0, int(r.get("autoscale:pending") or 0))
    agents  = ["transaction_collector", "pattern_analyzer", "risk_assessor", "blocker"]
    instances = {name: int(r.get(f"autoscale:instances:{name}") or 0) for name in agents}
    return dict(pending=pending, instances=instances)


def fetch_auction(r: redis.Redis) -> dict:
    total = int(r.get("auction:total") or 0)
    keys  = r.keys("auction:wins:*")
    wins  = {k.split(":")[-1]: int(r.get(k) or 0) for k in keys}
    return dict(total=total, wins=wins)


def decisions_to_df(decisions: list[dict]) -> pd.DataFrame:
    rows = []
    for d in decisions:
        ts = d.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).strftime("%H:%M:%S")
        except Exception:
            pass
        action = d.get("action", "")
        level  = d.get("risk_level", "")
        rows.append({
            "Время":    ts,
            "Аккаунт":  d.get("account_id", "")[:12],
            "Решение":  f"{ACTION_EMOJI.get(action, '')} {action}",
            "Уровень":  f"{LEVEL_EMOJI.get(level, '')} {level}",
            "Риск":     round(d.get("risk_score", 0), 1),
            "_tx_id":   d.get("transaction_id", ""),
        })
    return pd.DataFrame(rows)


# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fraud Detection",
    page_icon="🛡️",
    layout="wide",
)

r = get_redis()

# ── Header ───────────────────────────────────────────────────────────────────
col_title, col_refresh = st.columns([5, 1])
with col_title:
    st.title("🛡️ Fraud Detection Dashboard")
with col_refresh:
    st.write("")
    if st.button("🔄 Обновить", use_container_width=True):
        st.rerun()

st.caption(f"Последнее обновление: {datetime.now().strftime('%H:%M:%S')}")
st.divider()

# ── Fetch data ────────────────────────────────────────────────────────────────
stats     = fetch_stats(r)
decisions = fetch_recent(r)
autoscale = fetch_autoscale(r)
auction   = fetch_auction(r)

total = stats["allowed"] + stats["blocked"] + stats["review"]

# ── KPI metrics ───────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("✅ Разрешено",    stats["allowed"])
c2.metric("🚫 Заблокировано", stats["blocked"])
c3.metric("⚠️ На проверке",  stats["review"])
c4.metric("📊 Всего",         total)

st.divider()

# ── Risk levels ───────────────────────────────────────────────────────────────
st.subheader("Распределение рисков")
r1, r2, r3, r4 = st.columns(4)
r1.metric("🟢 LOW",      stats["low"])
r2.metric("🟡 MEDIUM",   stats["medium"])
r3.metric("🟠 HIGH",     stats["high"])
r4.metric("🔴 CRITICAL", stats["critical"])

st.divider()

# ── Main content: transactions + sidebar ─────────────────────────────────────
left, right = st.columns([3, 1])

with left:
    st.subheader(f"Последние транзакции (показано {min(len(decisions), RECENT_LIMIT)})")

    if not decisions:
        st.info("Транзакций пока нет. Запустите агентов и отправьте тестовую транзакцию.")
    else:
        df = decisions_to_df(decisions)
        display_df = df.drop(columns=["_tx_id"])

        event = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        selected_rows = event.selection.rows if event.selection else []
        if selected_rows:
            idx    = selected_rows[0]
            tx_id  = df.iloc[idx]["_tx_id"]
            action = decisions[idx].get("action", "")
            reasons = decisions[idx].get("reasons", [])

            st.subheader("Детали транзакции")
            st.code(tx_id, language=None)

            if reasons:
                st.write("**Факторы риска:**", ", ".join(reasons))

            explanation = r.get(f"explanation:{tx_id}")
            if explanation:
                st.success(f"**LLM-объяснение:**\n\n{explanation}")
            else:
                st.warning("Объяснение ещё не готово (LLM-агент обрабатывает или не запущен)")

with right:
    st.subheader("Агенты")

    st.write(f"**Очередь:** {autoscale['pending']} ожидают")
    st.write("**Доп. инстансы (autoscaler):**")
    agent_labels = {
        "transaction_collector": "Collector",
        "pattern_analyzer":      "Analyzer",
        "risk_assessor":         "Assessor",
        "blocker":               "Blocker",
    }
    for name, label in agent_labels.items():
        count = autoscale["instances"].get(name, 0)
        st.write(f"  {label}: +{count}")

    st.divider()

    st.subheader("Аукцион")
    st.write(f"**Всего аукционов:** {auction['total']}")
    if auction["wins"]:
        st.write("**Победы по воркерам:**")
        for worker, count in sorted(auction["wins"].items(), key=lambda x: -x[1]):
            st.write(f"  `{worker}`: {count}")
    else:
        st.write("Нет данных")

# ── Auto-refresh ──────────────────────────────────────────────────────────────
st.divider()
auto = st.checkbox("Авто-обновление каждые 5 секунд", value=False)
if auto:
    time.sleep(5)
    st.rerun()
