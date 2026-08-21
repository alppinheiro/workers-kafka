# Contexto e Instruções do Projeto: Order Saga Microservices (Go + Kafka)

## 1. Visão Geral do Projeto
Projeto de estudo em Go para simular o ciclo de vida de um pedido usando **Saga Orquestrada** e **workers assíncronos via Kafka**.

## 2. Escopo Atual e Restrições (Fase Atual)
- **Incluso:** Arquitetura central, contratos, simuladores de APIs externas, testes unitários (Fase 1) e persistência do estado da saga + rastreabilidade com PostgreSQL (Fase 2 em execução — ver `PHASE_2_PLAN.md`).
  - Banco de escrita `postgres` (tabelas `sagas` e `saga_events`, com payloads request/response dos gateways)
  - Banco de leitura `postgres-read` (read model `order_views` projetado via Kafka pelo serviço `projector`)
- **Fora de Escopo Nesta Fase:**
  - Observabilidade e métricas
  - API REST de consulta de pedido
  - Outbox Pattern, DLQ e idempotência completa (Fase 3)
  - Testes de integração com testcontainers (pendência da Fase 1)
  - Concorrência explícita / Goroutines (adiadas para fase de comparação de desempenho)

## 3. Estrutura Obrigatória de Pacotes
```text
cmd/                          # Pontos de entrada da aplicação (workers e orchestrator)
internal/domain/              # Entidades, status do pedido, enums e regras de negócio centrais
internal/application/         # Casos de uso, orquestrador da saga e coordenação
internal/infrastructure/
  ├── kafka/                  # Producer, consumer, serialização JSON e tópicos
  ├── external/               # Simuladores das APIs (Pagamento, Estoque, Notificação)
  └── persistence/
      ├── postgres/           # Banco de escrita: SagaRepository, EventLogRepository
      └── postgres_read/      # Banco de leitura: OrderViewRepository (read model)
internal/application/
  ├── orchestrator/           # Coordenação da saga (estado persistido)
  ├── worker/                 # Workers por etapa (logam request/response dos gateways)
  └── projector/              # Projeção de eventos Kafka no read model
internal/interfaces/          # Handlers, adapters e pontos de integração