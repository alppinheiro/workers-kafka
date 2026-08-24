# Technical Review: Order Saga Microservices

> Análise independente do projeto após a conclusão das Fases 1–9.  
> Objetivo: avaliar a correção dos conceitos aplicados, identificar pontos críticos de falha
> e sugerir alternativas que seriam adotadas em um ambiente de produção real.

---

## 1. O que foi aplicado corretamente

### 1.1 Orquestrated Saga Pattern
A escolha de **Saga Orquestrada** (em vez de Coreografada) está correta para este domínio.
Com compensação de pagamento, validação de sequência entre etapas e estado centralizado, a
coreografia tornaria o rastreamento de falhas muito mais difícil. O orquestrador como ponto
único de controle é o padrão que a indústria (Temporal, AWS Step Functions, Conductor) adota
para fluxos com mais de 3 etapas e compensações.

### 1.2 Transação Atômica (Unit of Work)
O `SagaUnitOfWork` (`internal/infrastructure/uow`) é o ponto mais maduro do projeto.
Gravar **estado da saga + journal + outbox em um único `pgx.Tx`** elimina a janela temporal
onde o estado foi salvo mas o evento ainda não foi enfileirado (ou vice-versa).
Esse padrão é exatamente o que diferencia implementações amadoras de produção — a idempotência
só precisaria cobrir redeliveries normais do Kafka, não inconsistências de escrita.

### 1.3 Outbox Pattern com `FOR UPDATE SKIP LOCKED`
Implementação correta e com detalhe importante: o `ClaimPending` usa `FOR UPDATE SKIP LOCKED`,
o que permite **múltiplas instâncias do relay** sem processamento duplo de uma mesma linha.
É a técnica recomendada para dequeue distribuído no Postgres (alternativa ao Redis Streams / SQS).

### 1.4 Idempotência por `event_id`
A verificação `EventLogRepository.Has(event_id, component)` antes de processar é a implementação
correta de **exactly-once semântico** sobre um transporte at-least-once (Kafka).
Registrar no journal antes de processar — e rolar a transação se algo falhar — garante que a
marca de "já processado" e o efeito colateral estejam atomicamente ligados.

### 1.5 Propagação de Trace (W3C `traceparent`)
Propagar o `traceparent` via **Kafka headers** no producer e reconstruí-lo no consumer é o
padrão OpenTelemetry para mensageria. O detalhe fino de preservar o `traceparent` na coluna
da outbox e reinjetá-lo no relay é o que mantém a cadeia de trace contínua mesmo com o hop
indireto — poucas implementações de estudo chegam a esse nível.

### 1.6 `ErrNonRetryable` + DLQ
Separar erros transitórios (rede, timeout) de definitivos (evento fora de ordem, schema inválido)
é a correta diferenciação entre **retry** e **dead letter**. Mover para DLQ e commitar o offset
evita que uma poison pill paralise a partição inteira.

### 1.7 Consumer Groups e ordenação por `order_id`
Usar `order_id` como **key do Kafka** garante que todos os eventos de um mesmo pedido vão para
a mesma partição, preservando a ordem por pedido sem coordenação distribuída extra.
Com 4 partições e `Hash` balancer, o particionamento é determinístico.

### 1.8 Separação de camadas (Clean Architecture)
As interfaces em `internal/application/ports.go` estão corretas como contrato entre camadas.
O domain não conhece Kafka, o orquestrador não conhece `pgx`, os workers não conhecem tópicos.
Isso foi validado nos testes unitários com fakes sem dependências de infra.

### 1.9 Compensação transacional
A sequência `INVENTORY_FAIL → startCompensation → PAYMENT_REFUND_PENDING → PAYMENT_COMPENSATE`
é a implementação correta de uma saga de compensação. Persistir o `transaction_id` no estado
da saga (necessário para acionar o estorno) e só iniciar a compensação se `transaction_id != ""`
são proteções corretas.

### 1.10 Helm + KEDA
O `ScaledObject` escalando por `consumerGroupLag` é o padrão de mercado para workers Kafka no
Kubernetes (é exatamente o que o KEDA foi projetado para fazer). Validar isso em kind antes do
cloud é o caminho certo.

---

## 2. Pontos Críticos de Falha

### 2.1 `RequiredAcks: RequireOne` no producer (risco de perda de dados)

```go
// internal/infrastructure/kafka/producer.go
RequiredAcks: kafkago.RequireOne,  // ⚠️ só o leader confirma
```

`RequireOne` significa que o broker líder confirma o write antes de replicar aos followers.
Se o leader cair nesse instante, o evento **é perdido silenciosamente**.  
Para o path **outbox-relay**, a perda seria tolerável em teoria (o relay tentaria novamente),
mas como o `published_at` só é marcado após confirmação do broker, a lógica de retry do relay
dependeria de detectar a falha — o que não acontece se o `WriteMessages` retornar `nil` com o
evento não replicado.

**Mitigação recomendada:**
```go
RequiredAcks: kafkago.RequireAll,  // -1: leader + todos os ISR (in-sync replicas)
```

### 2.2 Race condition nos simuladores com goroutines concorrentes

```go
// internal/infrastructure/external/payment.go
type PaymentSimulator struct {
    attempts map[string]int  // ⚠️ sem mutex
    rng      *rand.Rand      // ⚠️ rand.Rand não é thread-safe
}
```

Com `SAGA_WORKERS > 1`, múltiplas goroutines chamam `Process` concorrentemente.
O mapa `attempts` e o `rng` compartilhados sem `sync.Mutex` resultam em **race condition** —
detectável com `go test -race` e que pode causar pânico em runtime (`concurrent map write`).  
Em cenários de estudo não é crítico, mas o correto seria `sync.Mutex` ou `sync.Map` + `rand.New`
por goroutine (ou usar `math/rand` global do Go 1.20+, que é thread-safe).

### 2.3 Estado em memória dos simuladores quebra scale horizontal

O `attempts[orderID]` controla o cenário `retry-once` (falha na 1ª tentativa, sucesso na 2ª).
Com múltiplas instâncias do `worker-payment`, cada instância tem seu próprio mapa em memória.
Um retry processado por uma instância diferente da do 1º attempt sempre virá com `attempt = 1`,
tornando o `retry-once` **imprevisível em multi-instância**. É aceitável para estudo, mas
deve ser documentado claramente como limitação.

### 2.4 Sampling 100% no OpenTelemetry

```go
// internal/infrastructure/telemetry/telemetry.go
sdktrace.WithSampler(sdktrace.AlwaysSample()),
```

Em produção real, 100% de sampling com volume alto de pedidos gera:
- Overhead de CPU (serialização, export OTLP por trace)
- Volume massivo no Jaeger/Tempo (storage)
- Latência adicional no caminho crítico

O padrão de produção é `ParentBased(TraceIDRatioBased(0.01))` (1%) ou
head-based sampling configurável via `OTEL_TRACES_SAMPLER` env var.

### 2.5 Sem circuit breaker nos gateways externos

Os workers chamam os gateways diretamente sem isolamento de falha. Se o gateway de pagamento
entrar em estado de degradação lenta (timeout alto, sem retornar erro imediatamente), cada
goroutine do worker fica bloqueada segurando o connection pool e o semáforo de concorrência.
O resultado é um **thread starvation** progressivo.

**Padrão correto:** Circuit Breaker na camada de infra/external antes de chamar o gateway.  
Bibliotecas: `sony/gobreaker`, `afex/hystrix-go`, ou o padrão embutido de timeout com `context.WithTimeout`.

### 2.6 Comandos e resultados no mesmo tópico

```
orders.payment → PAYMENT_COMMAND + PAYMENT_RESULT + PAYMENT_COMPENSATE + PAYMENT_COMPENSATE_RESULT
```

O worker de pagamento filtra (ignora) os eventos que não são seus comandos, e o orquestrador
filtra os que não são resultados. Funciona, mas há consequências:

- **Amplificação de mensagens**: cada componente lê 100% das mensagens e descarta boa parte.
- **Acoplamento implícito**: adicionar um novo tipo de evento no tópico exige revisar o filtro
  de todos os consumidores.
- **Dificuldade de escala independente**: não é possível ter mais partições para comandos do
  que para resultados.

**Padrão alternativo de mercado:** tópicos segregados por direção —
`orders.payment.cmd` e `orders.payment.result`. Cada consumer group assina apenas o que precisa.

### 2.7 Sem schema registry / validação de contrato

O campo `schema_version: 1` existe no `Event`, mas não há mecanismo que impeça um producer
com `schema_version: 2` de publicar um payload incompatível — o consumer simplesmente faz
`json.Unmarshal` e campos novos/removidos passam silenciosamente.

Em times maiores, uma quebra de contrato gera falhas em produção difíceis de rastrear.
**Schema Registry** (Confluent, AWS Glue) com Protobuf ou Avro é o padrão de mercado.
Para estudo, ao menos validar o `schema_version` no consumer e enviar para DLQ quando não
reconhecido seria um bom exercício.

### 2.8 Probes de saúde superficiais

O endpoint `/healthz` responde `200 OK` incondicionalmente. Uma probe real deveria verificar:
- Conectividade com o broker Kafka (consegue fazer metadata request?)
- Pool do Postgres (consegue executar `SELECT 1`?)

Um pod que perdeu conexão com o Postgres passa nas probes e continua recebendo tráfego,
mas vai falhar em cada evento que processar — gerando DLQ sem parar.

### 2.9 `log.Printf` em vez de logging estruturado nativo

Os logs seguem o formato `key=value` (quase-structured), mas usam `log.Printf` com interpolação
de string. Isso tem dois problemas:
- Não há parsing automático — ferramentas como Loki, Datadog precisam de um regex customizado.
- Erros com `%v` que contêm aspas ou espaços podem quebrar o parsing key=value.

O Go 1.21 trouxe `log/slog` com saída JSON nativa. Migrar para:
```go
slog.InfoContext(ctx, "decision", "action", "advance", "order_id", orderID, ...)
```
tornaria os logs diretamente ingeríveis por qualquer stack de observabilidade sem parser customizado.

---

## 3. O que eu usaria diferente

### 3.1 Separação de tópicos comando/resultado

Adotaria tópicos separados por direção de mensagem:

```
orders.payment.cmd    → PAYMENT_COMMAND, PAYMENT_COMPENSATE
orders.payment.result → PAYMENT_RESULT, PAYMENT_COMPENSATE_RESULT
```

Vantagens: cada consumer group consome apenas o necessário, escala independente, rastreamento
de lag mais preciso por direção, contratos menores e mais fáceis de versionar.

### 3.2 State machine explícita com tabela de transições

O `switch` aninhado no `handleResult` funciona, mas é difícil de auditar completamente.
Uma tabela de transições torna as regras de negócio visíveis como dado:

```go
var validTransitions = map[domain.OrderStatus][]domain.OrderStatus{
    domain.StatusPending:           {domain.StatusPaymentPending},
    domain.StatusPaymentPending:    {domain.StatusPaymentApproved, domain.StatusRetrying, domain.StatusFailed},
    domain.StatusPaymentApproved:   {domain.StatusInventoryReserved, domain.StatusRetrying, domain.StatusPaymentRefundPending},
    // ...
}
```

Com isso, `canTransitionTo(from, to)` fica testável isoladamente e novos estados adicionados
aparecem obrigatoriamente nessa tabela.

### 3.3 Temporal.io (ou Cadence) como alternativa de orquestração

Para um projeto de estudo avançado, o próximo passo natural seria reimplementar o mesmo fluxo
usando **Temporal.io**: os Workflows são a saga, as Activities são os workers, e o Temporal
cuida de persistência, retry, compensação e observabilidade automaticamente.

A comparação "implementação manual vs Temporal" é extremamente didática e reveladora do
custo de manter os padrões manualmente (outbox, idempotência, UoW).

### 3.4 `pgx/v5` named parameters nas queries

As queries usam `$1, $2, $3` posicionais. Com volume grande de parâmetros (INSERT com 7+
colunas), a ordem se torna frágil. Considerar named parameters onde disponível ou ao menos
uma constante para cada posição.

### 3.5 `go test -race` obrigatório no CI

O job `check` no GitHub Actions deveria incluir:
```yaml
- run: go test -race ./...
```

Detecta race conditions como a do `PaymentSimulator` antes de chegar em produção.
É especialmente crítico com o `SAGA_WORKERS > 1`.

### 3.6 Alertas conectados a notificação

O `rules.yml` define `SagaDLQGrowth` e `SagaConsumerStalled`, mas não há roteamento para
Slack/PagerDuty/email no Alertmanager. O alert sem destinatário é silencioso.
Para o Kubernetes (Fase 9/10), adicionar o `alertmanager.yml` com webhook fecharia o loop
de observabilidade operacional.

### 3.7 Purge da outbox com VACUUM ANALYZE

O relay purga a outbox com `DELETE FROM outbox WHERE published_at < $1`. Correto, mas
após deletes massivos o Postgres mantém dead tuples até o VACUUM processar.
Em alto volume, agendar `VACUUM ANALYZE outbox` após a purge (ou configurar `autovacuum`
agressivo na tabela) evita bloat e degradação do índice `idx_outbox_pending`.

### 3.8 Healthcheck real no outbox-relay

O relay não tem `/healthz`. Se ele travar (stall no loop principal), o Kubernetes não
consegue detectar e reiniciar o pod — a outbox acumularia silenciosamente.
Adicionar um goroutine que atualiza um timestamp "last activity" e um handler `/healthz`
que retorna 503 se o timestamp for mais velho que N segundos fecha essa lacuna.

---

## 4. Avaliação Geral

| Área | Nota | Observação |
|---|---|---|
| Saga Pattern | ✅ Correto | Orquestrada com compensação implementada adequadamente |
| Persistência / UoW | ✅ Excelente | Transação atômica é o ponto mais maduro do projeto |
| Outbox Pattern | ✅ Correto | `SKIP LOCKED` para scale horizontal |
| Idempotência | ✅ Correto | Por `event_id` + componente, cobrindo redelivery |
| Observabilidade (traces) | ✅ Correto | W3C traceparent + propagação via outbox |
| Observabilidade (métricas) | ✅ Correto | Prometheus por serviço + Grafana |
| DLQ / Erros | ✅ Correto | Separação transitório vs definitivo |
| Arquitetura de tópicos | ⚠️ Funciona, pode melhorar | Comando + resultado no mesmo tópico aumenta acoplamento |
| Durabilidade Kafka | ⚠️ Risco | `RequireOne` pode perder eventos em failover do leader |
| Schema Evolution | ⚠️ Parcial | `schema_version` existe mas sem validação ativa |
| Thread safety (simuladores) | ❌ Race condition | `map` e `rand.Rand` sem mutex em goroutines concorrentes |
| Logging estruturado | ⚠️ Parcial | Quase-structured com `Printf`; `slog` seria o correto |
| Circuit breaker | ❌ Ausente | Gateways sem isolamento de falha |
| Sampling OTel | ⚠️ Exagerado para prod | `AlwaysSample()` inaceitável em volume alto |
| Health probes | ⚠️ Superficial | `/healthz` sem verificação de conectividade real |
| Race detector no CI | ❌ Ausente | `go test -race` não está no pipeline |

### Conclusão

Para um **projeto de estudo com técnicas profissionais**, o projeto está acima da média —
os padrões mais difíceis (UoW transacional, outbox, idempotência, trace propagation em dois
hops) foram implementados corretamente. Os pontos críticos não são de lógica de negócio, mas
de **operação em produção real** (durabilidade do broker, thread safety em escala,
observabilidade completa de falhas).

A Fase 10 (AWS/EKS) será o teste definitivo: é ao configurar `RequiredAcks`, ajustar sampling
e lidar com o custo de VACUUM em produção que esses detalhes se tornam obrigatórios.
