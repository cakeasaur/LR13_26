BIN_DIR := bin

.PHONY: all build clean run-infra stop-infra

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
