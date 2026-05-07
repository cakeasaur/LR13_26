# Промптлог — ЛР №13, Вариант 26

**Инструмент:** Claude Code (claude-sonnet-4.6)  
**Проект:** Мультиагентная система борьбы с мошенничеством  

---

## 1. Инициализация проекта

**Промпт:**
> Нужно реализовать лабораторную работу по мультиагентным системам, вариант 26 — система борьбы с мошенничеством. Стек: Go для агентов, Python для оркестратора, NATS как брокер сообщений, Redis для хранения состояния, Jaeger для трассировки, всё в Docker Compose. Создай базовую структуру проекта.

**Что сделал инструмент:**
- Создал структуру проекта: `agents/`, `orchestrator/`, `docker-compose.yml`, `go.mod`
- Настроил инфраструктуру: NATS (4222/8222), Redis (6379), Jaeger (16686/4317) в Docker Compose
- Создал `requirements.txt` с зависимостями Python

**Исправлено вручную:** —

---

## 2. Разработка Go-агентов

**Промпт:**
> Реализуй четыре агента на Go: TransactionCollector (валидация входящих транзакций), PatternAnalyzer (поиск подозрительных паттернов через Redis), RiskAssessor (вычисление риск-скора по нескольким факторам), Blocker (принятие решения ALLOW/REVIEW/BLOCK). Агенты общаются через NATS, используют QueueSubscribe для балансировки нагрузки.

**Что сделал инструмент:**
- Реализовал 4 микросервиса с pipeline: `transactions.incoming → validated → analyzed → risk → decision`
- `PatternAnalyzer`: частота транзакций (Redis sorted set), отклонение суммы (Redis list), time-of-day, large_amount
- `RiskAssessor`: взвешенная формула (частота×0.3 + сумма×0.25 + паттерны×0.25 + гео×0.1 + время×0.1)
- `Blocker`: пороги LOW/MEDIUM/HIGH/CRITICAL → ALLOW/REVIEW/BLOCK с учётом истории блокировок

**Промпт:**
> Перед тем как продолжать, проверь код на ошибки — посмотри логику, компиляцию, потенциальные проблемы.

**Что сделал инструмент:**
- Обнаружил: бинарники Go собирались в корень проекта → создал `Makefile` с `go build -o bin/...`
- Обнаружил: `fmt.Sscanf` ненадёжен для парсинга float из Redis → заменил на `strconv.ParseFloat`
- Добавил `.gitignore` для бинарников

**Исправлено вручную:** в `docker-compose.yml` вручную исправил дублирующийся маппинг порта 6379 у Redis — инструмент прописал его дважды, контейнер не поднимался с ошибкой `port is already allocated`.

---

## 3. Python-оркестратор и REST API

**Промпт:**
> Напиши Python-оркестратор, который отправляет транзакцию в NATS pipeline и ожидает результат. Добавь retry логику и timeout. Также нужен REST API на FastAPI: POST /transactions, GET /stats, GET /health. Состояние храни в Redis.

**Что сделал инструмент:**
- `orchestrator.py`: `asyncio.Future` для ожидания решения, retry ×3, timeout 30 с
- `api.py`: FastAPI с lifespan, эндпоинты `POST /transactions`, `GET /transactions/{id}`, `GET /stats`, `GET /health`
- `models.py`: dataclasses `Transaction` (с `to_dict()`), `Decision`

**Промпт:**
> Сделай аудит кода — проверь на баги, deprecated API, потенциальные race condition.

**Что сделал инструмент:**
- Исправил race condition: счётчики `int64` в Go → `atomic.Int64` (NATS callbacks в горутинах)
- Исправил `asyncio.get_event_loop()` → `get_running_loop()` (deprecated в Python 3.13)
- Исправил `%v` в Python-логировании → `%s` (Go-специфичный форматтер)
- Убрал `import json` внутри функции → вынес на уровень модуля
- Исправил захардкоженный Redis URL → `os.getenv("REDIS_URL", "redis://localhost:6379")`

**Исправлено вручную:** —

---

## 4. Jaeger + OpenTelemetry

**Промпт:**
> Добавь распределённую трассировку через OpenTelemetry и Jaeger. Все четыре Go-агента должны создавать spans с атрибутами транзакции. Если Jaeger недоступен — агент должен продолжать работу без трассировки.

**Что сделал инструмент:**
- `shared/telemetry.go`: `InitTracer()` с OTLP gRPC экспортом на `localhost:4317`
- Spans в каждом агенте с атрибутами: `tx.id`, `tx.amount`, `tx.risk_score`, `tx.action` и др.
- Дочерние spans для `check.frequency`, `check.amount_deviation` в PatternAnalyzer
- Fallback на noop-tracer при недоступности Jaeger

**Промпт:**
> Аудит. Проверь deprecated вызовы и ошибки компиляции.

**Что сделал инструмент:**
- Исправил `grpc.DialContext()` → `grpc.NewClient()` (deprecated в gRPC v1.81)
- Убрал неиспользуемый параметр `context.Context` из `validate()` → `_, span := tracer.Start(...)`
- Исправил ошибку компиляции: `ctx declared and not used`

**Промпт:**
> Запустил NATS, но веб-интерфейс мониторинга на порту 8222 не открывается — браузер говорит что страница недоступна.

**Что сделал инструмент:**
- Диагностировал: NATS не запускал HTTP-сервер мониторинга (отсутствовал флаг `-m 8222`)
- Добавил флаг в `command` секцию `docker-compose.yml`, пересобрал контейнер

**Исправлено вручную:** —

---

## 5. Автомасштабирование

**Промпт:**
> Реализуй автомасштабирование: когда накапливается очередь необработанных транзакций, система должна автоматически запускать дополнительные экземпляры агентов. При снижении нагрузки — останавливать лишние. Логику масштабирования вынеси в отдельный Python-процесс.

**Что сделал инструмент:**
- `autoscaler.py`: поллинг `autoscale:pending` в Redis каждые 5 с; scale-up при > 5 (max +2 инстанса), scale-down при < 2
- Трекинг в `orchestrator.py`: INCR pending при публикации, DECR в `finally` (учитывает таймаут)
- `GET /autoscale` эндпоинт, запись `autoscale:instances:{name}` для дашборда
- `make run-autoscaler` в Makefile

**Промпт:**
> Сделай аудит автоскейлера.

**Что сделал инструмент:**
- Исправил: `except KeyboardInterrupt` не перехватывается внутри `asyncio.run()` — при Ctrl+C дочерние процессы оставались висеть. Заменил на `finally` блок

**Исправлено вручную:** путь к бинарнику в `autoscaler.py` был задан относительным (`./bin/pattern_analyzer`) — при запуске из директории `orchestrator/` процесс не находился. Заменил на абсолютный через `os.path.abspath`.

---

## 6. Аукцион задач

**Промпт:**
> Добавь механизм аукциона задач: вместо простой балансировки через QueueSubscribe, агенты PatternAnalyzer должны конкурировать за каждую транзакцию — делать ставки на основе текущей нагрузки, а координатор выбирать наименее загруженного.

**Что сделал инструмент:**
- `auction.py`: `AuctionCoordinator` перехватывает `transactions.validated`, публикует bid-request с reply-subject, собирает ставки 300 мс, выбирает победителя по минимальной нагрузке
- Переработал `pattern_analyzer/main.go`: UUID воркера при старте, `atomic.Int64` счётчик in-flight транзакций, подписки на `transactions.auction` (bid) и `transactions.worker.{id}` (выигранные задачи)
- Добавил `SubjectTransactionsAuction`, `SubjectTransactionsWorkerPrefix` в `shared/types.go`
- Исправил `.gitignore`: паттерн `pattern_analyzer` блокировал директорию с исходниками

**Исправлено вручную:** —

---

## 7. LLM-агент

**Промпт:**
> Нужен LLM-агент, который будет объяснять решения системы человеческим языком. Установи Ollama с быстрой моделью. Агент подписывается на решения и генерирует краткое объяснение для оператора — почему транзакция заблокирована или пропущена.

**Что сделал инструмент:**
- Установил Ollama через Homebrew, скачал `llama3.2` (2.0 GB)
- `llm_agent.py`: подписка на `transactions.decision`, fire-and-forget через `asyncio.create_task` (не блокирует NATS), вызов Ollama `/api/generate`, сохранение в Redis `explanation:{tx_id}` TTL 24ч
- Idempotency check: пропускает если объяснение уже существует
- `GET /transactions/{id}/explanation` в FastAPI

**Промпт:**
> Аудит.

**Что сделал инструмент:**
- Перенёс `from models import Transaction` из тела функции `submit_transaction` на уровень модуля `api.py`

**Исправлено вручную:** —

---

## 8. Веб-панель Streamlit

**Промпт:**
> Сделай веб-дашборд на Streamlit. Должны быть метрики ALLOW/BLOCK/REVIEW, таблица последних транзакций с возможностью кликнуть на строку и увидеть LLM-объяснение, статус автоскейлера и статистику аукциона.

**Что сделал инструмент:**
- `dashboard.py`: KPI-метрики (ALLOW/BLOCK/REVIEW/Total + риск-уровни), интерактивная таблица последних 50 транзакций (`on_select="rerun"`), LLM-объяснение и факторы риска по клику
- Правая колонка: очередь autoscaler, доп. инстансы по агентам, статистика побед в аукционе
- Авто-обновление каждые 5 с (опциональный чекбокс), `make run-dashboard`

**Промпт:**
> Аудит дашборда.

**Что сделал инструмент:**
- Убрал `st.cache_data.clear()` — no-op, так как ни одна функция не декорирована `@st.cache_data`
- Добавил `max(0, ...)` для `autoscale:pending` — защита от отрицательных значений при рестарте

**Исправлено вручную:** —

---

## 9. Тесты

**Промпт:**
> Напиши тесты: для Go-агентов используй стандартный `testing` с табличными кейсами, для Python — pytest. Покрой основную бизнес-логику: валидацию транзакций, риск-скоринг, логику масштабирования, построение LLM-промптов. Внешние зависимости мокировать.

**Что сделал инструмент:**
- Go: `TestValidate` (10 кейсов граничных значений), `TestIsValidCurrency`, `TestRiskLevel`, `TestNormalize`, `TestTimeRisk`, `TestGeoRisk` — все table-driven
- Python: `test_models.py` (8 тестов dataclasses), `test_llm_agent.py` (9 тестов `build_prompt`), `test_autoscaler.py` (8 тестов с AsyncMock Redis)
- Установил `pytest-asyncio`, добавил `conftest.py`, исправил `datetime.utcnow()` → `datetime.now(timezone.utc)`

**Промпт:**
> Аудит тестов.

**Что сделал инструмент:**
- Убрал неиспользуемый `import subprocess` в `test_autoscaler.py`

**Исправлено вручную:** —

---

## 10. Диаграмма архитектуры и документация

**Промпт:**
> Обнови README: добавь диаграмму архитектуры на Mermaid, таблицы с описанием компонентов, инструкцию по запуску всех сервисов, описание API-эндпоинтов с примером запроса и ответа.

**Что сделал инструмент:**
- `flowchart TD` с тремя subgraph: Python-сервисы, Go-агенты, Инфраструктура; показаны все NATS-темы, Redis-ключи, OTel-трассировка
- Таблицы агентов (вход/выход/задача), Python-сервисов, инфраструктуры
- Секции: быстрый старт, API-эндпоинты с примером curl/JSON, мониторинг, структура проекта

**Промпт:**
> Аудит README и диаграммы — проверь соответствие реальному коду.

**Что сделал инструмент:**
- Исправил лейбл `NATS →|"validated"| TC` → `"transactions.incoming"` (TC подписан на incoming, не validated)
- Исправил `Auction → PA` напрямую → `Auction → NATS → PA` (роутинг через брокер)
- Исправил направление `Auto → Redis` → `Redis → Auto` (autoscaler читает pending, не пишет)
- Убрал упоминание JetStream — в проекте используется core NATS (QueueSubscribe)
- Исправил версию Go: `1.22+` → `1.26+` (по `go.mod`)
- Убрал ложную пометку "тесты" у директории `pattern_analyzer`

**Исправлено вручную:** —

---

## Итог

| Показатель | Значение |
|---|---|
| Коммитов | 20 |
| Go-агентов | 4 |
| Python-сервисов | 6 (API, Orchestrator, Auction, Autoscaler, LLM, Dashboard) |
| Тестов | 39 (14 Go + 25 Python) |
| Строк кода | ~2 000 |
| Ручных правок | 2 |
