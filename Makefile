COMPOSE ?= docker-compose
ORDER_ID ?= order-001
SERVICES = kafka kafka-init orchestrator worker-payment worker-inventory worker-notification order-status

.PHONY: help fmt build vet test lint check up down logs ps create-order rebuild

help:
	@echo "Targets disponiveis:"
	@echo "  make fmt           - formata todo o projeto com gofmt"
	@echo "  make build         - compila todos os pacotes Go"
	@echo "  make vet           - roda go vet no modulo"
	@echo "  make test          - roda todos os testes com cobertura"
	@echo "  make lint          - roda golangci-lint se estiver disponivel"
	@echo "  make check         - executa fmt, build, vet, test e lint em sequencia"
	@echo "  make up            - sobe Kafka, orquestrador, workers e auditoria em background"
	@echo "  make down          - derruba a stack Docker"
	@echo "  make logs          - segue os logs da stack"
	@echo "  make ps            - lista os servicos da stack"
	@echo "  make create-order  - publica um pedido usando ORDER_ID=<id>"
	@echo "  make rebuild       - rebuild da stack antes de subir"

fmt:
	gofmt -w .

build:
	go build ./...

vet:
	go vet ./...

test:
	go test ./... -cover

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not available"; \
	fi

check: fmt build vet test lint

up:
	$(COMPOSE) up -d --build $(SERVICES)

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f $(SERVICES)

ps:
	$(COMPOSE) ps

create-order:
	$(COMPOSE) run --rm create-order $(ORDER_ID)

rebuild:
	$(COMPOSE) build orchestrator worker-payment worker-inventory worker-notification order-status create-order