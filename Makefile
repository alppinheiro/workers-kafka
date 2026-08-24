COMPOSE ?= docker-compose
ORDER_ID ?= order-001
SERVICES = kafka kafka-init postgres migrations postgres-read migrations-read jaeger prometheus grafana orchestrator worker-payment worker-inventory worker-notification order-status projector outbox-relay metrics-exporter

.PHONY: help fmt build vet test lint check ci integration up down logs ps create-order inspect rebuild

help:
	@echo "Targets disponiveis:"
	@echo "  make fmt           - formata todo o projeto com gofmt"
	@echo "  make build         - compila todos os pacotes Go"
	@echo "  make vet           - roda go vet no modulo"
	@echo "  make test          - roda todos os testes com cobertura"
	@echo "  make lint          - roda golangci-lint se estiver disponivel"
	@echo "  make check         - executa fmt, build, vet, test e lint em sequencia"
	@echo "  make ci            - pipeline do CI local: check + integration (Testcontainers)"
	@echo "  make up            - sobe Kafka, Postgres (escrita/leitura), migrations, orquestrador, workers, projector e auditoria em background"
	@echo "  make down          - derruba a stack Docker"
	@echo "  make logs          - segue os logs da stack"
	@echo "  make ps            - lista os servicos da stack"
	@echo "  make create-order  - publica um pedido usando ORDER_ID=<id>"
	@echo "  make inspect       - consulta o read model (order_views) de um pedido no banco de leitura"
	@echo "  make autoscale     - roda o autoscaler (lag -> docker-compose scale) no host"
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

# Pipeline do CI local: qualidade + unitários + integração real (Testcontainers).
# Usado para iterar antes do push; o GitHub Actions executa os mesmos passos.
ci: check integration

# Testes de integração com Testcontainers (Kafka + Postgres reais em containers).
# Requer Docker. Detecta o socket: usa DOCKER_HOST do ambiente se definido; senão,
# o socket do colima (macOS) se existir; caso contrário, o Docker nativo.
integration:
	@DOCKER_HOST="$${DOCKER_HOST:-}"; \
	if [ -z "$$DOCKER_HOST" ] && [ -S "$$HOME/.colima/default/docker.sock" ]; then \
		DOCKER_HOST="unix://$$HOME/.colima/default/docker.sock"; \
		TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="/var/run/docker.sock"; \
	fi; \
	TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="$${TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE:-/var/run/docker.sock}" \
	DOCKER_HOST="$$DOCKER_HOST" \
	go test -tags integration -v ./internal/infrastructure/kafka/... ./internal/infrastructure/persistence/postgres/...

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

inspect:
	$(COMPOSE) exec postgres-read psql -U saga -d saga_read -c "SELECT order_id, current_status, last_event_type, last_event_at, transaction_id, notification_error, payment_refund_failed FROM order_views WHERE order_id='$(ORDER_ID)'"

autoscale:
	KAFKA_BROKERS=localhost:9094 go run ./cmd/autoscaler

rebuild:
	$(COMPOSE) build orchestrator worker-payment worker-inventory worker-notification order-status projector outbox-relay metrics-exporter create-order