# 🛠️ Conceitos de Infraestrutura — Autovacuum, Watchdog e Debezium

> **Por que este documento existe:** o projeto usa mecanismos de infraestrutura que resolvem
> problemas específicos de banco de dados e de resiliência de consumers. Aqui explicamos, para
> cada um, **o que é, como funciona e por que entrou no projeto** — com referência ao código real.

## Sumário

1. [Autovacuum (PostgreSQL)](#1-autovacuum-postgresql)
2. [Watchdog anti-stall (Kafka consumer)](#2-watchdog-anti-stall-kafka-consumer)
3. [Debezium (Change Data Capture)](#3-debezium-change-data-capture)
4. [Como os três se conectam](#4-como-os-três-se-conectam)

---

## 1. Autovacuum (PostgreSQL)

### O que é

Mecanismo **interno do PostgreSQL** (não é um padrão de aplicação) que limpa, em *background*,
as **tuplas mortas** (dead tuples) deixadas por `UPDATE` e `DELETE`. Toda atualização ou exclusão
cria uma versão antiga da linha que continua ocupando espaço físico — o autovacuum remove esse
"papel riscado" e reutiliza o espaço.

### Como funciona

| Etapa | O que acontece |
|---|---|
| 1. `UPDATE`/`DELETE` | PostgreSQL cria tuplas mortas (MVCC: versões antigas preservadas para concorrência) |
| 2. Autovacuum dispara | quando a proporção de tuplas mortas atinge o limiar configurado |
| 3. Limpeza | remove as versões mortas, reaproveita espaço e atualiza estatísticas (`ANALYZE`) |
| 4. Resultado | tabela não "inchava" (bloat) e os scans/índices continuam rápidos |

### Parâmetros principais

| Parâmetro | Default | Projeto (`migrations/000006`) | Efeito |
|---|---|---|---|
| `autovacuum_vacuum_scale_factor` | `0.2` (20%) | `0.05` (5%) | dispara o VACUUM com muito menos tuplas mortas |
| `autovacuum_vacuum_threshold` | `50` | `2000` | número mínimo de tuplas mortas para disparar |
| `autovacuum_analyze_scale_factor` | `0.1` | `0.05` | frequência da atualização de estatísticas |
| `autovacuum_analyze_threshold` | `50` | `2000` | mínimo para atualizar estatísticas |

### Por que entrou no projeto

Foi uma **otimização de banco** identificada no stress test de 120k pedidos
(`docs/STRESS_TEST.md` §3.5):

- A tabela **`outbox`** sofre `INSERT`/`UPDATE`/`DELETE` contínuos (o relay publica e depois marca
  `published_at`) — é a tabela com maior taxa de tuplas mortas.
- A tabela **`sagas`** sofre um `UPDATE` por etapa do fluxo.
- O default `0.2 / 50` era lento sob carga: dead tuples acumulavam e degradavam o
  `idx_outbox_pending` (usado pelo relay a cada ciclo).

A migration `000006_optimize_indexes` tornou o autovacuum **mais agressivo** nessas duas tabelas.

### Limitação (contexto AWS)

O autovacuum é uma ferramenta específica do PostgreSQL. **Não é** uma técnica de arquitetura de
aplicação (saga, outbox, DLQ são técnicas; autovacuum é manutenção de engine). No **Amazon RDS**
ele está habilitado por padrão e é ajustado via **Parameter Group**; ajustes por tabela exigem
`superuser` (nem sempre disponível). Alternativas sem autovacuum ajustado:

| Alternativa | Descrição | Prós | Contras |
|---|---|---|---|
| `VACUUM` manual agendado | `pg_cron` ou Lambda/EventBridge executando `VACUUM ANALYZE` | simples; funciona no RDS | compete com a aplicação; depende de agendamento |
| `VACUUM FULL` | compacta 100% da tabela | elimina o bloat de vez | **trava a tabela** (janela de manutenção) |
| `pg_repack` | reconstrói a tabela sem lock | não bloqueia escrita | extensão nem sempre disponível no RDS |
| Particionamento por data | `DROP TABLE partition` em vez de `DELETE` em massa | **elimina a causa-raiz**; purga instantânea | exige mudança de schema |
| Tabela de arquivo morto | mantém a tabela ativa pequena (só pendentes) | índices rápidos | duas tabelas para operar |
| `REINDEX CONCURRENTLY` | reconstrói o índice inchado | rápido; sem lock | só trata o sintoma |

---

## 2. Watchdog anti-stall (Kafka consumer)

### O que é

Um **guardião** (goroutine) que monitora a saúde do reader do Kafka. Se o reader parar de fazer
*fetch* por um período sem progresso, o watchdog **cancela o contexto do `FetchMessage`
bloqueado**, desbloqueando o loop principal, que **reconecta o reader sozinho** (self-healing).

### Por que entrou no projeto

A biblioteca `kafka-go` tem um travamento conhecido: em certas condições (ex.: tópico recriado,
alteração de partições), o reader **para de consumir sem retornar erro** — o consumer continua
"vivo" no grupo (não rebalanceia) mas **não avança o offset**. O lag cresce silenciosamente.

Validado no teste de resiliência **R2** (`BENCHMARK.md`): tópico `orders.payment` deletado no meio
do fluxo → os consumers **não morrem** (`UnknownTopicOrPartition` virou retry) e o **watchdog
reconecta o reader** em até 45 s.

### Como funciona (código real)

`internal/infrastructure/kafka/consumer.go`:

```go
const (
    stallCheckInterval = 15 * time.Second   // frequência da checagem
    stallTimeout       = 45 * time.Second   // tempo sem NENHUM fetch = travado
)

func (c *Consumer) watchdogStall(reader *kafkago.Reader, cancel context.CancelFunc, stop <-chan struct{}) {
    ticker := time.NewTicker(stallCheckInterval)
    defer ticker.Stop()

    var lastFetches int64 = -1
    lastProgress := time.Now()
    for {
        select {
        case <-stop:
            return
        case <-ticker.C:
            stats := reader.Stats()
            now := time.Now()
            if stats.Fetches > lastFetches {   // houve progresso → tá vivo
                lastFetches = stats.Fetches
                lastProgress = now
                continue
            }
            if stallDetected(stats.Fetches, lastFetches, now.Sub(lastProgress), stallTimeout) {
                slog.Warn("reader travado detectado", ...)
                cancel()                       // cancela o FetchMessage bloqueado
                lastProgress = now
            }
        }
    }
}
```

E o loop principal reage ao cancelamento (`fetchCtx.Err() != nil`) fechando o reader travado e
criando um novo (reconexão automática).

### Analogia

É o **cão de guarda** de sistemas de alta disponibilidade: não deixa o consumer "fingir" que está
vivo enquanto não consome. Igual ao watchdog de hardware que reinicia um servidor travado, mas no
nível de aplicação.

---

## 3. Debezium (Change Data Capture)

### O que é

Plataforma **open source** de **CDC (Change Data Capture)**: conecta-se ao **WAL (Write-Ahead Log)**
do PostgreSQL (ou binlog do MySQL) e publica cada `INSERT`/`UPDATE`/`DELETE` como **evento em tempo
real** no Kafka — sem polling no banco.

### O contexto no projeto: Transactional Outbox

O projeto usa o padrão **Transactional Outbox**: estado da saga + evento a publicar são gravados
**na mesma transação** (tabela `outbox`), garantindo consistência entre banco e Kafka. Um serviço
chamado **`outbox-relay`** lê a outbox e publica no Kafka.

| Abordagem | Como publica | Projeto |
|---|---|---|
| **Polling (Message Relay)** | `SELECT ... FOR UPDATE SKIP LOCKED` a cada ciclo | ✅ `cmd/outbox-relay` (Fase 7.1) — ~485 ev/s |
| **CDC (Debezium)** | lê o WAL do Postgres e publica automaticamente | ❌ mencionado como evolução futura |

### Por que o projeto NÃO usa Debezium (ainda)

Do `PHASE_7_PLAN.md`:

> *"Message Relay (polling) com `ClaimPending` com `FOR UPDATE SKIP LOCKED` — polling publisher
> (escolha determinística e simples)."*

> *"Relay sem polling no banco: CDC (Debezium) lê o WAL → não compete com o banco e escala melhor.
> Alternativa imediata: otimizar o polling."*

| | Polling (implementado) | Debezium (futuro) |
|---|---|---|
| Complexidade | baixa (1 serviço simples) | alta (conectores, schema registry, retenção do WAL) |
| Competição com o banco | sim (faz `SELECT` na outbox) | não (lê o WAL) |
| Escala | suficiente até ~485 ev/s (otimizado) | melhor para volumes maiores |
| Decisão | escolha determinística e simples | adiado; reavaliar quando o polling virar gargalo |

---

## 4. Como os três se conectam

```
┌─────────────────────────────────────────────────────┐
│  PostgreSQL (estado da saga + outbox)               │
│  • autovacuum  → saúde das tabelas de alta escrita  │
│  • outbox      → garantia de entrega ao Kafka       │
└──────────────────────┬──────────────────────────────┘
                       │ relay (polling hoje / Debezium amanhã)
                       ▼
┌─────────────────────────────────────────────────────┐
│  Kafka                                                │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│  Consumers (orquestrador + workers)                  │
│  • watchdog anti-stall → nunca morre em silêncio     │
└─────────────────────────────────────────────────────┘
```

- **Autovacuum** mantém o banco rápido sob carga (limpeza de tuplas mortas).
- **Outbox + relay (ou futuro Debezium)** garante consistência entre banco e Kafka.
- **Watchdog** garante que os consumers sobrevivam a falhas silenciosas do Kafka.

---

## Glossário rápido

| Termo | Definição curta |
|---|---|
| **Dead tuples** | versões antigas de linhas após `UPDATE`/`DELETE` (MVCC) |
| **Bloat** | crescimento físico da tabela causado por dead tuples acumuladas |
| **MVCC** | Multi-Version Concurrency Control — versões de linha para concorrência |
| **WAL** | Write-Ahead Log — log de transações do PostgreSQL |
| **CDC** | Change Data Capture — captura de mudanças do banco em eventos |
| **SKIP LOCKED** | técnica de claim de linhas sem bloquear outras transações |
| **Self-healing** | capacidade de o sistema se recuperar sozinho (ex.: reconexão do reader) |

