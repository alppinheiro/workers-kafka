# Fase 1: Testes Automatizados (A Rede de Segurança)

## Objetivo

Garantir que a lógica de negócio não regrida durante a evolução do projeto (persistência, DLQ, observabilidade e concorrência).

## Progresso

### ✅ Testes Unitários (concluído)

| Pacote | Cobertura |
|---|---|
| `internal/application/worker` | 100% |
| `internal/domain` | 100% |
| `internal/infrastructure/external` | 100% |
| `internal/interfaces` | 100% |
| `internal/application/orchestrator` | 98.4% |
| `internal/infrastructure/kafka` | 41.1% (parte exige Kafka real) |

**Arquivos criados:**

- `internal/application/orchestrator/orchestrator_test.go` + `mocks_test.go`
  - Transições de estado completas (sucesso, retry, falha, compensação)
  - Validação de eventos fora de ordem, saga desconhecida e status inválidos
  - Testes das funções puras: `nextCommand`, `expectedStatusForResult`, `validateResultStatus`, `cloneMetadata`, `terminalEvent`
  - Fluxos completos ponta a ponta (sucesso e compensação)
- `internal/application/worker/` (payment, inventory, notification, coordinator)
  - Todos os caminhos de cada worker: sucesso, falha, retry e eventos ignorados
- `internal/domain/` — constantes, JSON tags e geração de IDs
- `internal/infrastructure/external/` — simuladores e cenários determinísticos
- `internal/interfaces/` — `WithLogging` e `formatMetadata`
- `internal/infrastructure/kafka/` — roteamento de tópicos, env, erros de fetch

### ⏳ Testes de Integração com Kafka real (pendente)

Utilizar `testcontainers-go` para subir um broker Kafka em container e validar:

- `Consumer.Consume` lendo e processando eventos reais
- `Producer.Publish` com sucesso (serialização e envio)
- Fluxo ponta a ponta (create-order → orchestrator → workers → status)

## Comandos

```bash
make test    # roda todos os testes com cobertura
make check   # fmt, build, vet, test e lint
```

