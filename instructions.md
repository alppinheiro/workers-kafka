## Contexto geral

Este projeto é de estudo e tem como objetivo construir uma aplicação microservice em Go com workers separados, usando Kafka para processar um grande volume de mensagens.

A ideia principal é simular o ciclo de vida de um pedido com mudança de status e envio dos eventos para diferentes consumidores, representando integrações com serviços externos.

## Visão consolidada

- Arquitetura: saga orquestrada, não coreografada.
- Controle central: um orquestrador/process manager coordena avanço, retry e encerramento do fluxo.
- Fluxo principal: `PENDING -> PAYMENT_PENDING -> PAYMENT_APPROVED -> INVENTORY_RESERVED -> NOTIFIED -> COMPLETED`.
- Saídas de erro: `RETRYING` para falha temporária e `FAILED` para falha definitiva.
- Mensageria: tópicos separados por etapa do fluxo, com `order_id` como chave de particionamento.
- Interação: `event_type` distingue comando, resultado e falha no payload.
- Estado do orquestrador: persistido em PostgreSQL (tabela `sagas`), com recuperação após restart; todos os eventos e payloads de request/response dos gateways registrados em `saga_events` (rastreabilidade).
- Serialização: JSON no início, com schema versionado e evolutivo.
- Kafka em Go: `github.com/segmentio/kafka-go` como escolha inicial, isolado por interfaces.
- Escopo atual: testes unitários implementados (Fase 1); persistência, rastreabilidade e read model implementados (Fase 2); outbox, DLQ e idempotência implementados (Fase 3); observabilidade distribuída com OpenTelemetry + Jaeger implementada (Fase 4); goroutines explícitas/concorrência fora de escopo (Fase 5).

As seções abaixo detalham e complementam esse resumo, evitando repetir decisões já fechadas quando possível.

## Objetivo funcional

Os consumidores simulam chamadas para:
- api de pagamento
- api de inventário/estoque
- api de notificação

Essas integrações devem ser desenhadas com abstrações simples para permitir troca fácil da implementação durante os estudos.

## Decisões já fechadas

- O projeto será feito em Go.
- Os workers devem ficar bem separados para facilitar escala horizontal no futuro.
- Toda transição precisa carregar e atualizar o status do pedido/evento.
- Mockery não é a primeira escolha obrigatória; fakes, stubs e interfaces simples podem ser usados primeiro.
- Persistência do estado da saga: PostgreSQL 16 (banco de escrita) com driver `jackc/pgx/v5`.
- Banco de leitura: segundo PostgreSQL alimentado por projeção via Kafka (CQRS), com read model `order_views` montado pelo serviço `projector`.
- Migrations: `golang-migrate/migrate` executadas por container no `docker-compose`.
- Rastreabilidade: todos os eventos são gravados em `saga_events` (append-only) com `payload`, `request_payload` e `response_payload`.
- Garantia de dados: at-least-once + dedup por `event_id` + reentrega do Kafka; Outbox Pattern fica para a Fase 3.

## Fora de escopo nesta fase

- integração automatizada com testcontainers (pendência da Fase 1)
- observabilidade
- API REST de consulta de pedido (futuro — lê o read model `order_views`)
- Outbox Pattern, DLQ e idempotência completa (Fase 3)
- concorrência explícita com goroutines

## Diretrizes técnicas

- Aplicar boas práticas de Go.
- Usar princípios de SOLID.
- Organizar o código com separação clara de responsabilidades.
- Preferir clean architecture / clean code onde fizer sentido.
- Usar injeção de dependência e inversão de controle.
- Evitar duplicação de código.
- Manter contratos e interfaces pequenos e fáceis de evoluir.
- Utilizar lint desde o início do projeto para manter padrão de qualidade e consistência do código.

## Ferramentas do projeto

- `gofmt` para formatação obrigatória do código Go.
- `goimports` para organização automática de imports.
- `golangci-lint` como ferramenta principal de lint desde o início.
- Interfaces simples, fakes e stubs para simulações iniciais de dependências externas.
- `mockery` somente se houver uma necessidade clara de gerar mocks mais complexos no futuro.
- `go test` será incorporado mais adiante, quando o escopo de testes sair do congelamento atual.
- `docker` e `docker-compose` já podem ser usados para subir Kafka e os binários Go localmente, sem alterar o escopo funcional do fluxo.
- Um `Makefile` simples pode encapsular os comandos mais frequentes de build, validação, subida da stack e publicação manual de pedidos.
- O `Makefile` pode expor um alvo `lint` com fallback explícito quando `golangci-lint` não estiver instalado no ambiente.
- O `Makefile` também pode expor um alvo `check` para consolidar formatação e verificações locais em um único comando.
- O workspace pode incluir `.vscode/launch.json` e `.vscode/tasks.json` para depuração ponta a ponta no VS Code, com sessão composta para orquestrador, workers e consumer de status.
- O mecanismo de debug pode incluir logs explícitos de decisão no orquestrador e cenários controlados por `order_id` nos simuladores externos, para reproduzir falhas e retries de forma determinística.

## Dependências Kafka em Go

- Recomendação principal: usar `github.com/segmentio/kafka-go` para começar, por ter API simples, boa legibilidade e facilitar o início do estudo.
- Alternativa futura: `github.com/IBM/sarama` caso seja necessário explorar recursos mais amplos de cliente Kafka ou cenários mais específicos de produtor e consumer groups.
- A escolha da biblioteca deve ficar isolada atrás de interfaces internas, para evitar acoplamento direto do domínio com o client Kafka.
- Producer, consumer e serialização de mensagens devem depender de contratos internos e não da biblioteca externa diretamente.
- A camada Kafka deve ser tratada como infraestrutura, separada do domínio e da aplicação.

## Estrutura sugerida de pacotes

- `cmd/` para os pontos de entrada da aplicação.
- `internal/domain/` para entidades, status do pedido e regras centrais do negócio.
- `internal/application/` para casos de uso, orquestração do fluxo e coordenação entre etapas.
- `internal/infrastructure/kafka/` para producer, consumer, serialização e configuração de tópicos.
- `internal/infrastructure/external/` para simuladores das APIs de pagamento, estoque e notificação.
- `internal/infrastructure/persistence/postgres/` para repositórios do banco de escrita (`SagaRepository`, `EventLogRepository`).
- `internal/infrastructure/persistence/postgres_read/` para o repositório do read model (`OrderViewRepository`).
- `internal/application/projector/` para o caso de uso que projeta eventos do Kafka no read model.
- `internal/interfaces/` para handlers, adapters e qualquer integração de entrada ou saída que converse com a aplicação.
- `cmd/projector/` para o consumer de projeção (read model).
- Um consumer adicional de auditoria pode existir em `cmd/` para observar eventos finais publicados em `orders.status`.

## Onde a camada Kafka fica no desenho

- Kafka não deve aparecer no domínio.
- A aplicação deve expor interfaces para publicar eventos e consumir mensagens, enquanto a implementação concreta fica na infraestrutura.
- Os workers devem depender de abstrações da aplicação e só conhecer detalhes de Kafka na borda da infraestrutura.
- A serialização das mensagens deve ser definida de forma centralizada para manter compatibilidade entre producer e consumers.
- Cada worker pode ter seu próprio ponto de entrada, mas todos devem compartilhar os contratos do domínio e da aplicação.

## Contrato das mensagens

- Cada mensagem deve representar um evento de negócio, não apenas um payload técnico.
- O evento precisa carregar pelo menos: `event_id`, `order_id`, `status_atual`, `status_anterior`, `event_type`, `created_at` e um campo de `metadata` para extensões futuras.
- O `status_atual` deve indicar em que etapa o pedido está naquele momento.
- O `status_anterior` ajuda a rastrear transições e a debugar o fluxo.
- O `event_type` deve diferenciar o tipo de ação ou etapa do fluxo, por exemplo: criação do pedido, pagamento processado, estoque reservado e notificação enviada.
- O formato deve ser estável e pensado para evolução sem quebrar consumidores existentes.
- A serialização inicial pode ser JSON por simplicidade, desde que fique isolada atrás de interfaces para futura troca se necessário.

## Fluxo de evento sugerido

Este fluxo já está consolidado no topo do documento e segue a sequência `PENDING -> PAYMENT_PENDING -> PAYMENT_APPROVED -> INVENTORY_RESERVED -> NOTIFIED -> COMPLETED`, com suporte a `RETRYING` e `FAILED` quando necessário.

## Tópicos e particionamento

- Cada etapa principal do fluxo pode ter seu próprio tópico para isolar responsabilidades e facilitar escala.
- Uma organização inicial possível é ter tópicos separados para pedido inicial, pagamento, estoque e notificação.
- O `order_id` deve ser usado como chave de particionamento para preservar a ordem dos eventos de um mesmo pedido.
- O número de partições deve ser pensado para permitir paralelismo futuro, mas sem exagero na primeira versão.
- A quantidade exata de partições ainda pode ser ajustada quando houver uma estimativa mais concreta de volume por segundo.
- A estratégia inicial deve priorizar legibilidade e separação clara entre workers.
- Regras de retenção, replicação e compactação podem ser definidas depois, quando a infraestrutura Kafka estiver mais próxima da implementação.
- O padrão recomendado é separar tópicos por etapa do fluxo, e não por tipo de mensagem.
- Cada tópico deve carregar comandos e resultados da mesma etapa com um campo explícito indicando o papel da mensagem.
- A nomenclatura dos tópicos deve deixar claro o vínculo com a etapa, por exemplo: `orders.created`, `orders.payment`, `orders.inventory`, `orders.notification`.
- Dentro do payload, o campo `event_type` deve indicar se a mensagem é um comando de avanço, um resultado de processamento ou um evento de falha.
- Para rastreabilidade do encerramento da saga, eventos terminais podem ser publicados em um tópico próprio de status final, separado das etapas ativas.

## Regras de retry e falha

- O fluxo deve distinguir falha temporária de falha definitiva.
- Falhas temporárias podem gerar o status `RETRYING` antes de uma nova tentativa.
- Falhas definitivas devem resultar em `FAILED`.
- A decisão de retry deve ser simples e previsível na fase inicial, sem políticas complexas.
- O número de tentativas pode ser definido depois, mas o contrato já deve prever essa possibilidade.
- Um evento em retry deve continuar carregando `order_id` e o histórico de status anterior.
- O objetivo desta etapa é manter rastreabilidade do problema sem acoplar a lógica de retry ao domínio do pedido.
- Para depuração manual, cenários determinísticos podem ser disparados pelo próprio `order_id`, por exemplo com padrões como `payment-fail`, `payment-retry-once`, `inventory-fail`, `inventory-retry-once`, `notification-fail` e `notification-retry-once`.

## Serialização das mensagens

- A serialização inicial deve ser JSON pela simplicidade de estudo e legibilidade.
- O formato do payload deve ser estável e versionável desde o começo.
- Campos de timestamp devem seguir um padrão consistente, idealmente em UTC.
- A serialização deve ficar isolada em uma camada própria para facilitar futura troca de formato.
- Producer e consumers devem compartilhar o mesmo contrato de mensagem para evitar divergências.
- Qualquer evolução do schema deve ser feita de forma compatível com versões anteriores sempre que possível.
- A escolha por JSON não impede uma futura migração para um formato mais eficiente, se o estudo evoluir para isso.

## Versionamento do schema

- O contrato da mensagem deve carregar um campo de versão do schema quando isso fizer sentido.
- A versão ajuda a evoluir o payload sem quebrar consumidores antigos.
- Mudanças que removam ou renomeiem campos devem ser evitadas no início.
- A estratégia inicial deve privilegiar adição de novos campos opcionais em vez de alterações destrutivas.
- Se uma mudança incompatível for inevitável, ela deve ser planejada com uma nova versão de contrato.
- Producer e consumers devem saber lidar com pelo menos a versão atual e, quando possível, com uma versão anterior compatível.

## Orquestração do fluxo

- Falta uma peça central para coordenar o caminho completo do pedido entre sucesso, erro e retry.
- Essa peça deve ser tratada como um orquestrador de fluxo, também conhecido como process manager.
- A arquitetura recomendada é uma saga orquestrada, não coreografada.
- O orquestrador central toma as decisões de sequência e de tratamento de falha, enquanto os workers executam apenas sua responsabilidade local.
- O orquestrador não executa regra de negócio de pagamento, estoque ou notificação; ele coordena o avanço entre etapas.
- Ele deve consumir os eventos produzidos pelos workers e decidir qual é a próxima ação do fluxo.
- Em caso de sucesso, ele acompanha a sequência até `COMPLETED`.
- Em caso de falha temporária, ele pode reenfileirar o evento como `RETRYING` ou encaminhar para nova tentativa.
- Em caso de falha definitiva, ele encerra o fluxo como `FAILED`.
- O orquestrador deve ser o ponto único para consolidar o estado lógico do pedido durante o processamento.
- A princípio, essa coordenação pode ser feita apenas por eventos Kafka e contratos internos, sem banco de dados.
- Nesta fase, o reinício do orquestrador pode implicar perda do estado em memória, deixando a recuperação persistente para depois.
- Se necessário no futuro, o orquestrador pode receber uma persistência própria para recuperar o estado após reinício.
- O desenho do fluxo deve considerar que o orquestrador observa entradas e saídas de cada etapa, garantindo rastreabilidade do caminho completo.

### Responsabilidades do orquestrador

- Receber o evento inicial do pedido.
- Publicar o próximo comando ou evento de transição para o worker correto.
- Acompanhar o retorno de cada etapa.
- Registrar o estado lógico atual do fluxo.
- Decidir quando seguir adiante, quando tentar novamente e quando encerrar o caso.
- Evitar que os workers conheçam regras de coordenação global.

### Eventos observados pelo orquestrador

- Evento de criação do pedido com `PENDING`.
- Evento de retorno do worker de pagamento.
- Evento de retorno do worker de estoque.
- Evento de retorno do worker de notificação.
- Evento de falha ou retry de qualquer etapa intermediária.

### Decisões do orquestrador

- Se o pagamento aprovar, avançar para estoque.
- Se o estoque reservar, avançar para notificação.
- Se a notificação concluir, encerrar como `COMPLETED`.
- Se ocorrer falha temporária, sinalizar `RETRYING` e coordenar nova tentativa.
- Se ocorrer falha definitiva, encerrar o fluxo como `FAILED`.
- Se um evento chegar fora de ordem, tratar como inconsistência do fluxo e não seguir adiante sem validação.
- Durante depuração, o orquestrador pode registrar explicitamente decisões como início da saga, avanço de etapa, retry solicitado, limite de retry excedido, falha final e publicação do encerramento.

### Estado do fluxo

- O orquestrador deve manter uma visão do estado atual do pedido durante o ciclo de vida da saga.
- Essa visão pode começar apenas em memória ou via eventos correlacionados, sem banco de dados no início.
- A correlação entre eventos deve usar `order_id` e, quando necessário, um identificador de saga.
- A proposta é manter a coordenação centralizada sem acoplar a implementação do fluxo ao domínio dos workers.

### Linha de eventos da saga

- O orquestrador inicia a saga publicando o evento inicial do pedido.
- O worker de pagamento consome o evento de abertura e publica o resultado da etapa de pagamento.
- O orquestrador consome o retorno do pagamento e decide se o fluxo segue ou encerra.
- Se o pagamento aprovar, o orquestrador aciona a etapa de estoque.
- O worker de estoque consome o evento de avanço e publica o resultado da reserva.
- O orquestrador consome o retorno do estoque e decide se segue para notificação.
- Se o estoque reservar, o orquestrador aciona a etapa de notificação.
- O worker de notificação consome o evento de avanço e publica o encerramento da etapa.
- O orquestrador consome o retorno da notificação e publica um evento terminal de `COMPLETED` quando tudo der certo.
- Se qualquer etapa falhar de forma temporária, o orquestrador pode solicitar nova tentativa.
- Se a falha for definitiva, o orquestrador publica um evento terminal de `FAILED`.

### Padrão de interação

- Os workers respondem a eventos de comando ou avanço.
- O orquestrador reage a eventos de resultado.
- A comunicação deve deixar claro quando uma mensagem é comando e quando é resultado.
- Essa separação ajuda a evitar ambiguidade no fluxo e facilita rastreamento de ponta a ponta.

## Visão consolidada do fluxo

- O pedido nasce no orquestrador com o status inicial `PENDING`.
- O orquestrador publica um evento de comando para o worker de pagamento.
- O worker de pagamento publica um evento de resultado com `PAYMENT_APPROVED`, `RETRYING` ou `FAILED`.
- O orquestrador consome esse resultado e decide se aciona o worker de estoque ou encerra a saga.
- Se o pagamento seguir adiante, o orquestrador publica um comando para o worker de estoque.
- O worker de estoque publica o resultado com `INVENTORY_RESERVED`, `RETRYING` ou `FAILED`.
- O orquestrador consome o resultado do estoque e decide se aciona o worker de notificação ou encerra a saga.
- Se o estoque seguir adiante, o orquestrador publica um comando para o worker de notificação.
- O worker de notificação publica o resultado com `NOTIFIED`, `RETRYING` ou `FAILED`.
- O orquestrador consome o resultado final e encerra o fluxo como `COMPLETED` quando tudo der certo.
- Após esse encerramento, o orquestrador publica o status terminal em um tópico de saída próprio para permitir auditoria ou integração futura.
- Em qualquer ponto de falha definitiva, a saga termina como `FAILED`.
- Em qualquer ponto de falha temporária, a saga pode seguir para `RETRYING` antes de uma nova decisão.
- Se um evento chegar inconsistente, fora de ordem ou sem correlação válida, o orquestrador deve tratar como erro de fluxo.
- Essa visão consolidada deve ser mantida como referência principal do processo antes da implementação.

## Workers sugeridos

### Worker de pagamento

- Consome eventos de pedido em etapa de pagamento.
- Simula a validação ou aprovação do pagamento.
- Publica o próximo evento com o status atualizado.
- Pode retornar falha para simular recusa, timeout ou erro técnico.
- Deve receber um evento em `PAYMENT_PENDING` e decidir se avança para `PAYMENT_APPROVED` ou `FAILED`.
- A decisão pode ser baseada em regras simples de simulação, como aprovação aleatória controlada ou critérios configuráveis.
- Não deve conhecer detalhes do worker de estoque ou de notificação.
- Deve produzir um novo evento com o mesmo `order_id` e o `status_anterior` refletindo a etapa de entrada.
- Em caso de falha temporária, deve sinalizar `RETRYING` antes da marcação final de erro, se o fluxo assim exigir.
- A responsabilidade principal é validar o contrato de entrada, simular a regra de pagamento e publicar o próximo estado.
- O worker deve rejeitar comandos cujo `status_atual` não seja `PAYMENT_PENDING`.
- Em modo de depuração, o simulador pode ser forçado via `order_id` a aprovar, falhar de forma definitiva ou falhar temporariamente uma vez antes de aprovar.

### Worker de estoque

- Consome eventos após a aprovação do pagamento.
- Simula reserva ou validação de estoque.
- Publica o evento seguinte com o status de estoque reservado ou falha.
- Deve ser independente do worker de pagamento para facilitar escala separada.
- Deve receber um evento em `PAYMENT_APPROVED` e decidir se avança para `INVENTORY_RESERVED` ou `FAILED`.
- A simulação pode representar reserva bem-sucedida, estoque indisponível ou falha técnica.
- Não deve conter lógica de pagamento nem de notificação.
- Deve manter o mesmo `order_id` e atualizar corretamente `status_anterior` e `status_atual`.
- Em caso de falha temporária, pode produzir `RETRYING` antes do erro final, seguindo a regra geral do fluxo.
- A responsabilidade principal é validar a etapa de estoque e emitir o próximo evento da cadeia.
- O worker deve rejeitar comandos cujo `status_atual` não seja `PAYMENT_APPROVED`.
- Em modo de depuração, o simulador pode ser forçado via `order_id` a reservar com sucesso, falhar de forma definitiva ou falhar temporariamente uma vez antes de reservar.

### Worker de notificação

- Consome eventos após a reserva de estoque.
- Simula envio de notificação ao cliente.
- Publica o encerramento do fluxo quando a notificação for concluída.
- Deve ser isolado para permitir ajuste futuro de canais de notificação.
- Deve receber um evento em `INVENTORY_RESERVED` e decidir se avança para `NOTIFIED` ou `FAILED`.
- A simulação pode representar envio por e-mail, SMS, push ou qualquer outro canal abstrato.
- Não deve conhecer regras internas de pagamento ou de estoque.
- Deve preservar o `order_id` e atualizar o `status_anterior` com o estado recebido.
- Pode produzir `RETRYING` em caso de falha temporária antes da finalização com erro.
- A responsabilidade principal é encerrar o fluxo com sucesso ou registrar a falha de notificação.
- O worker deve rejeitar comandos cujo `status_atual` não seja `INVENTORY_RESERVED`.
- Em modo de depuração, o simulador pode ser forçado via `order_id` a notificar com sucesso, falhar de forma definitiva ou falhar temporariamente uma vez antes de concluir.

### Coordenador do fluxo

- Pode existir como aplicação ou caso de uso central para orquestrar a criação inicial do pedido.
- Responsável por iniciar o evento com status `PENDING`.
- Não deve conter lógica de infraestrutura de Kafka.
- Deve depender apenas dos contratos da aplicação e do domínio.
- Deve ser o ponto de entrada lógico para criar o pedido e publicar o evento inicial do fluxo.
- Pode receber os dados mínimos do pedido e montar o evento de abertura com `order_id`, `status_atual` igual a `PENDING` e `event_type` correspondente à criação.
- Não deve executar regras de pagamento, estoque ou notificação.
- Não deve conhecer detalhes de particionamento, tópicos ou serialização concreta.
- Sua responsabilidade principal é disparar o primeiro evento de forma consistente e rastreável.

## Evolução sugerida

### Fase 1: base do projeto
- Criar a estrutura inicial do módulo Go.
- Definir domínio, entidades e enums de status.
- Organizar pacotes de aplicação, domínio e infraestrutura.
- Criar os contratos para producer, consumer e simuladores externos.

### Fase 2: fluxo Kafka
- Implementar o producer de pedidos/eventos.
- Implementar os consumers separados por responsabilidade.
- Definir tópicos e partições conforme necessidade do fluxo.
- Garantir que cada transição publique o próximo evento com o status correto.

### Fase 3: simuladores externos
- Criar simuladores para pagamento, estoque e notificação.
- Permitir que esses simuladores sejam substituídos por implementações reais no futuro.

### Fase 4: persistência e rastreabilidade
- Persistir o estado da saga em PostgreSQL (banco de escrita) com recuperação após restart.
- Registrar todos os eventos e payloads de request/response dos gateways em `saga_events`.
- Criar banco de leitura com read model `order_views` alimentado por projeção via Kafka (serviço `projector`).
- Garantir at-least-once + dedup por `event_id` + reentrega do Kafka.

### Fase 5: comparação de desempenho
- Introduzir goroutines e concorrência explícita.
- Comparar comportamento e desempenho com a versão sequencial.

## Histórico das definições

- O projeto começou apenas com este arquivo de instruções.
- A análise inicial indicou que já existe base suficiente para começar a implementação estrutural.
- Foi decidido adiar goroutines para uma fase posterior, para medir a diferença de desempenho depois.
- O foco imediato passou a ser arquitetura, contratos e separação dos workers antes de qualquer otimização de paralelismo.
- A implementação atual usa os tópicos `orders.created`, `orders.payment`, `orders.inventory`, `orders.notification` e `orders.status`, com roteamento centralizado por `event_type` na infraestrutura Kafka.
- O orquestrador valida a etapa esperada de cada resultado antes de avançar a saga, tratando evento fora de ordem como erro de fluxo.
- Eventos terminais `ORDER_COMPLETED` e `ORDER_FAILED` são publicados em `orders.status` para rastreabilidade externa do encerramento.
- Os workers validam `event_type` e também o `status_atual` esperado antes de executar a simulação local.
- A infraestrutura local ganhou `Dockerfile` único com build parametrizado por entrypoint e `docker-compose.yml` com broker Kafka, inicialização de tópicos, workers, orquestrador e consumer de auditoria.
- A execução local também pode ser feita via `Makefile`, centralizando os comandos `up`, `down`, `logs`, `build`, `vet` e `create-order`.
- O workflow local via `Makefile` também contempla `lint`, que deve degradar graciosamente quando a ferramenta não estiver disponível no PATH.
- O workflow local via `Makefile` pode usar `check` como atalho para `fmt`, `build`, `vet` e `lint` em sequência.
- O fluxo de debug ponta a ponta no VS Code pode usar Kafka em Docker e os binários Go em modo debug local, com `KAFKA_BROKERS=localhost:9094` para todos os processos instrumentados pelo editor.
- A sessão composta de debug deve subir `kafka` e `kafka-init` antes do attach/launch dos processos e permitir disparar um pedido manual com `create-order` usando um `order_id` informado em prompt.
- Os logs de debug agora devem permitir acompanhar tanto a publicação quanto o consumo dos eventos e também as decisões internas do orquestrador, sempre correlacionando por `order_id` e `saga_id`.
- Fase 2 (persistência e rastreabilidade) em execução conforme `PHASE_2_PLAN.md`: estado da saga persistido em `sagas`, todos os eventos com payloads request/response em `saga_events`, read model `order_views` no banco de leitura projetado via Kafka pelo serviço `projector`.
- Banco de escrita em `postgres:5433` (db `saga`) e banco de leitura em `postgres-read:5434` (db `saga_read`), com migrations `golang-migrate/migrate` via docker.
- Garantia de dados: at-least-once + dedup por `event_id` + reentrega do Kafka; Outbox Pattern adiado para a Fase 3.
- Consulta do read model nesta fase via SQL (psql); API REST de consulta de pedido fica para uma fase futura.
- Fase 3 (resiliência) concluída conforme `PHASE_3_PLAN.md`: idempotência por `event_id` (orquestrador e workers), DLQ por tópico (`orders.*.dlq`) com erros definitivos (`ErrNonRetryable`), e Outbox Pattern (tabela `outbox` + `OutboxPublisher` + serviço `outbox-relay`).
- A ordem de escrita nos handlers é: Save/Append (estado + journal) → Outbox; a reentrega do Kafka + idempotência cobrem os gaps. A transação atômica única (estado + journal + outbox) é refinamento futuro documentado.
- Fase 4 (observabilidade) concluída conforme `PHASE_4_PLAN.md`: OpenTelemetry com exporter OTLP para o Jaeger, propagação W3C `traceparent` via Kafka headers (inclusive através da outbox, com coluna `traceparent` reconstruída pelo `outbox-relay`), e um span por evento consumido (`consume <EVENT_TYPE>`).
- Teste de carga (2000 pedidos): ingestão ~47.000 eventos/s; `outbox-relay` otimizado para publicar em lote (`PublishBatch`). O gargalo restante são os consumers single-threaded → Fase 5 (concorrência com goroutines e mais partições).
- Fase 5 (escalabilidade de produção) concluída conforme `PHASE_5_PLAN.md` e `BENCHMARK.md`: 4 partições por tópico, `SAGA_WORKERS` (Readers concorrentes no mesmo consumer group), escala horizontal via `--scale` (consumer groups), outbox-relay com `FOR UPDATE SKIP LOCKED` (claims) e autoscaler por lag (análogo local ao KEDA/HPA). Benchmark: 3.000 pedidos/60 s com 1 réplica deixam 2.012 na fila; com 3 réplicas + 2 relays, 164 (~12×).
- CI/CD (planejado): GitHub Actions com `make check` + testes de integração (services postgres/kafka) + push de imagem para ECR; deploy futuro em EKS via Helm (publicação AWS: EKS/ECS, MSK, RDS).

## Observação

Quando começarmos a implementar, o próximo agente deve priorizar a leitura deste arquivo como fonte única de contexto funcional e de decisões já tomadas.

Toda decisão nova de planejamento, ajuste de escopo ou definição técnica deve ser registrada aqui para manter o histórico do projeto atualizado.

O fluxo de trabalho deve permanecer em modo de planejamento até que a base esteja bem definida e o usuário sinalize que é hora de partir para a implementação.

Recuperação persistente do estado do orquestrador em caso de reinício fica para uma etapa posterior.