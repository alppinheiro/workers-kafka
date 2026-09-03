COMPOSE ?= docker-compose
ORDER_ID ?= order-001
SERVICES = kafka kafka-init postgres migrations postgres-read migrations-read jaeger prometheus grafana orchestrator worker-payment worker-inventory worker-notification order-status projector outbox-relay metrics-exporter

.PHONY: help fmt build vet test lint check ci integration up down logs ps create-order inspect rebuild k8s-up k8s-images k8s-refresh k8s-down k8s-logs k8s-smoke aws-up aws-down

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
	@echo "  make k8s-images    - atualiza as imagens da app p/ GHCR :latest e reinicia (rollout restart)"
	@echo "  make k8s-refresh   - alias do k8s-images (use apos religar o cluster para sempre subir atualizado)"
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
# Nome do cluster kind (vem do kind-config.yaml) e contexto kubectl correspondente.
K8S_CLUSTER ?= $(shell awk '/^name:/{print $$2; exit}' deploy/k8s/kind-config.yaml)
K8S_CONTEXT ?= kind-$(K8S_CLUSTER)
# Deployments da aplicacao (nao inclui infra kafka/postgres). Sao os que usam as
# imagens ghcr.io/alppinheiro/workers-kafka-<svc> e os consumidores kafka-go.
K8S_APP_DEPLOYMENTS = order-saga-orchestrator order-saga-worker-payment order-saga-worker-inventory order-saga-worker-notification order-saga-projector order-saga-outbox-relay order-saga-order-status order-saga-metrics-exporter
# Repositorio de imagens publicado pelo CI (Fase 8) em ghcr.io/<owner>.
K8S_IMG_REPO ?= ghcr.io/alppinheiro
# Apos o rollout restart, aguarda os consumers estabilizarem antes de retornar OK.
# Motivo: o reader kafka-go pode iniciar "travado" (stall) e o watchdog anti-stall
# reconecta em ~45-90s (documentado no BENCHMARK.md); sem esse warm-up, um smoke
# disparado logo apos o refresh pode validar durante a janela de stall.
# Use K8S_WARMUP=0 para pular a espera.
K8S_WARMUP ?= 75

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
	# Jaeger all-in-one (destino OTLP dos traces - Fase D; Service order-saga-otel)
	kubectl apply -f deploy/k8s/otel.yaml
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
		--set image.tag=$(K8S_IMG_TAG) \
		--set image.pullPolicy=Always
	kubectl wait --for=condition=complete job/order-saga-migrations -n $(K8S_NAMESPACE) --timeout=120s
	kubectl rollout status deployment -n $(K8S_NAMESPACE) --timeout=180s

# Forca a atualizacao das imagens da app no cluster SEM recriar a infra/cluster.
# Util depois de religar o Colima/kind (o node cacheia imagens antigas) ou quando
# o CI publicar um :latest novo no GHCR: seta imagePullPolicy=Always, faz rollout
# restart (kubelet re-puxa do GHCR) e aguarda os Deployments ficarem saudaveis.
# Guarda de seguranca: so roda no cluster kind (nunca na AWS/EKS).
k8s-images:
	@test "$$(kubectl config current-context)" = "$(K8S_CONTEXT)" || { echo "ERRO: contexto atual nao e $(K8S_CONTEXT) (kubectl use-context $(K8S_CONTEXT))"; exit 1; }
	@echo "== atualizando imagens da app ($(K8S_IMG_REPO)/workers-kafka-<svc>:$(K8S_IMG_TAG)) =="
	@for d in $(K8S_APP_DEPLOYMENTS); do \
		kubectl -n $(K8S_NAMESPACE) patch deployment $$d --type=json \
			-p '[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Always"}]' >/dev/null || exit 1; \
		echo "  $$d -> imagePullPolicy=Always"; \
	done
	@echo "== rollout restart (re-pull das imagens) =="
	kubectl -n $(K8S_NAMESPACE) rollout restart deployment $(K8S_APP_DEPLOYMENTS)
	kubectl -n $(K8S_NAMESPACE) rollout status deployment $(K8S_APP_DEPLOYMENTS) --timeout=240s
	@if [ "$(K8S_WARMUP)" -gt 0 ] 2>/dev/null; then \
		echo "== warm-up de $(K8S_WARMUP)s (estabilizacao dos consumers apos o restart) =="; \
		sleep $(K8S_WARMUP); \
	fi
	@echo "OK: imagens da app atualizadas para $(K8S_IMG_TAG) e Deployments saudaveis."

# Alias semanticamente mais claro: use apos "subir" (religar Colima/cluster).
k8s-refresh: k8s-images

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
	bash scripts/aws-cleanup.sh <<< "y"