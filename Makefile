BIN_DIR := bin

.PHONY: all build clean run-infra stop-infra run-agents run-api run-demo

all: build

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/transaction_collector ./agents/transaction_collector
	go build -o $(BIN_DIR)/pattern_analyzer ./agents/pattern_analyzer
	go build -o $(BIN_DIR)/risk_assessor ./agents/risk_assessor
	go build -o $(BIN_DIR)/blocker ./agents/blocker
	@echo "✅ Все агенты собраны в $(BIN_DIR)/"

clean:
	rm -rf $(BIN_DIR)

run-infra:
	docker-compose up -d

stop-infra:
	docker-compose down

run-agents: build
	$(BIN_DIR)/transaction_collector &
	$(BIN_DIR)/pattern_analyzer &
	$(BIN_DIR)/risk_assessor &
	$(BIN_DIR)/blocker &
	@echo "✅ Все агенты запущены в фоне"

run-api:
	cd orchestrator && uvicorn api:app --host 0.0.0.0 --port 8080 --reload

run-demo:
	cd orchestrator && python3 main.py
