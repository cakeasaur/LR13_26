# Система борьбы с мошенничеством (Fraud Detection System)

Мультиагентная распределённая система для анализа и блокировки мошеннических транзакций.

## 🏗️ Архитектура

### Агенты (Go)
- **TransactionCollector** — сбор и валидация транзакций
- **PatternAnalyzer** — анализ подозрительных паттернов
- **RiskAssessor** — вычисление risk score
- **Blocker** — принятие решений о блокировке

### Оркестратор (Python)
- Управление pipeline
- REST API
- Веб-интерфейс (Streamlit)

### Инфраструктура
- **NATS** — message broker
- **Redis** — хранение состояния и истории
- **Jaeger** — distributed tracing

## 🚀 Быстрый старт

### Требования
- Go 1.26+
- Python 3.9+
- Docker & Docker Compose

### Установка

```bash
# Запустить инфраструктуру
docker-compose up -d

# Проверить статус
docker-compose ps
```

### Доступные сервисы
- NATS: `nats://localhost:4222`
- NATS WebUI: http://localhost:8222
- Redis: `localhost:6379`
- Jaeger UI: http://localhost:16686

## 📁 Структура проекта

```
fraud-detection-system/
├── agents/          # Go агенты
├── orchestrator/    # Python оркестратор и API
├── docker/         # Docker конфигурации
├── tests/          # Тесты
├── docs/           # Документация
└── docker-compose.yml
```

## 📋 Задания

### Блок 1: Основная система (обязательно)
- [ ] 4 микросервиса на Go (NATS)
- [ ] Pipeline: транзакция → анализ → риск → блокировка
- [ ] Redis для состояния

### Блок 2: Продвинутые функции
- [ ] Jaeger + OpenTelemetry
- [ ] Автомасштабирование
- [ ] Аукцион задач
- [ ] LLM-агент
- [ ] Веб-панель (Streamlit)

## 🔗 Ссылки

- [NATS Documentation](https://docs.nats.io/)
- [Go NATS Client](https://github.com/nats-io/nats.go)
- [OpenTelemetry Go](https://opentelemetry.io/docs/instrumentation/go/)
