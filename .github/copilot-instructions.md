# Contexto e Instruções do Projeto: Order Saga Microservices (Go + Kafka)

## 1. Visão Geral do Projeto
Projeto de estudo em Go para simular o ciclo de vida de um pedido usando **Saga Orquestrada** e **workers assíncronos via Kafka**.

## 2. Escopo Atual e Restrições (Fase Atual)
- **Incluso:** Arquitetura central, contratos, simuladores de APIs externas, orquestração em memória/eventos.
- **Fora de Escopo Nesta Fase:**
  - Testes unitários (`go test` congelado por enquanto)
  - Containers/Docker/Docker-Compose
  - Observabilidade e métricas
  - Banco de dados/Persistência (estado do orquestrador é mantido em memória)
  - Concorrência explícita / Goroutines (adiadas para fase de comparação de desempenho)

## 3. Estrutura Obrigatória de Pacotes
```text
cmd/                          # Pontos de entrada da aplicação (workers e orchestrator)
internal/domain/              # Entidades, status do pedido, enums e regras de negócio centrais
internal/application/         # Casos de uso, orquestrador da saga e coordenação
internal/infrastructure/
  ├── kafka/                  # Producer, consumer, serialização JSON e tópicos
  └── external/               # Simuladores das APIs (Pagamento, Estoque, Notificação)
internal/interfaces/          # Handlers, adapters e pontos de integração