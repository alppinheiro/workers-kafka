COMPOSE ?= docker-compose
ORDER_ID ?= order-001
SERVICES = kafka kafka-init postgres migrations postgres-read migrations-read jaeger prometheus grafana orchestrator worker-payment worker-inventory worker-notification order-status projector outbox-relay metrics-exporter

.PHONY: help fmt build vet test lint check ci integration up down logs ps create-order inspect rebuild k8s-up k8s-down k8s-logs k8s-smoke aws-up aws-down

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
	@echo "  make k8s-up        - sobe cluster kind + Kafka + Postgres + Helm chart (Fase 9)"
	@echo "  make k8s-down      - derruba o cluster kind e remove os recursos"
	@echo "  make k8s-logs      - segue os logs de um deployment (SVC=<nome>)"
	@echo "  make k8s-smoke     - smoke e2e no cluster (ORDER_ID=<id>) - scripts/k8s-smoke.sh"
	@echo "  make aws-up        - terraform apply (VPC+EKS+RDS na AWS - Fase 10, requer aws configure)"
	@echo "  make aws-down      - terraform destroy (custo ≈ zero quando parado)"

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

# ============================================================================
# Fase 9 — Kubernetes local (kind + Helm + KEDA)
# Requer: kind, kubectl, helm (brew install kind kubernetes-cli helm)
# ============================================================================
K8S_NAMESPACE ?= order-saga
K8S_IMG_TAG ?= latest

k8s-up:
	kind create cluster --config deploy/k8s/kind-config.yaml
	kubectl create namespace $(K8S_NAMESPACE) 2>/dev/null || true
	# Operador KEDA (autoscaling por lag)
	helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
	helm repo update kedacore
	helm upgrade --install keda kedacore/keda --namespace keda --create-namespace --wait
	# Infra: Postgres (escrita/leitura) + Kafka (apache/kafka KRaft, tópicos via Job kafka-init)
	kubectl apply -f deploy/k8s/postgres.yaml
	kubectl apply -f deploy/k8s/kafka.yaml
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgres -n $(K8S_NAMESPACE) --timeout=120s
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgres-read -n $(K8S_NAMESPACE) --timeout=120s
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n $(K8S_NAMESPACE) --timeout=180s
	kubectl wait --for=condition=complete job/kafka-init -n $(K8S_NAMESPACE) --timeout=120s
	# Migrations (ConfigMaps com os .sql de escrita e leitura) + Helm chart da aplicação
	kubectl create configmap order-saga-migrations --from-file=migrations/ -n $(K8S_NAMESPACE) \
		-o yaml --dry-run=client | kubectl apply -f -
	kubectl create configmap order-saga-migrations-read --from-file=migrations-read/ -n $(K8S_NAMESPACE) \
		-o yaml --dry-run=client | kubectl apply -f -
	helm upgrade --install order-saga deploy/helm/order-saga -n $(K8S_NAMESPACE) \
		--set image.tag=$(K8S_IMG_TAG)
	kubectl wait --for=condition=complete job/order-saga-migrations -n $(K8S_NAMESPACE) --timeout=120s
	kubectl rollout status deployment -n $(K8S_NAMESPACE) --timeout=180s

k8s-down:
	helm uninstall order-saga -n $(K8S_NAMESPACE) 2>/dev/null || true
	helm uninstall keda -n keda 2>/dev/null || true
	kind delete cluster --name order-saga 2>/dev/null || true

k8s-logs:
	kubectl logs -f deployment/order-saga-$(SVC) -n $(K8S_NAMESPACE)

k8s-smoke:
	bash scripts/k8s-smoke.sh $(ORDER_ID)

# Fase 10 — Cloud AWS (requer: brew install terraform awscli argocd; aws configure)
# aws-up cria a infra e roda o bootstrap (kubeconfig + saga_read + Secret + ArgoCD + KEDA).
# aws-bootstrap pode rodar sozinho se a infra já existir.
aws-up:
	cd terraform && terraform init && terraform apply -auto-approve
	bash scripts/aws-bootstrap.sh

aws-bootstrap:
	bash scripts/aws-bootstrap.sh

aws-down:
	cd terraform && terraform destroy -auto-approve