CREATE TABLE IF NOT EXISTS saga_events (
    id               BIGSERIAL PRIMARY KEY,
    order_id         VARCHAR(255) NOT NULL,
    saga_id          VARCHAR(255) NOT NULL,
    event_id         VARCHAR(255) NOT NULL,
    event_type       VARCHAR(50)  NOT NULL,
    component        VARCHAR(50)  NOT NULL,
    direction        VARCHAR(20)  NOT NULL,
    status_anterior  VARCHAR(50),
    status_atual     VARCHAR(50),
    payload          JSONB,
    request_payload  JSONB,
    response_payload JSONB,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (event_id, component, direction)  -- 1 visão por componente/evento/direção
);

CREATE INDEX IF NOT EXISTS idx_saga_events_order ON saga_events(order_id, created_at);
