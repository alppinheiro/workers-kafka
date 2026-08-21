CREATE TABLE IF NOT EXISTS sagas (
    order_id        VARCHAR(255) PRIMARY KEY,
    saga_id         VARCHAR(255) NOT NULL,
    current_status  VARCHAR(50)  NOT NULL,
    previous_status VARCHAR(50),
    retry_count     INTEGER      NOT NULL DEFAULT 0,
    transaction_id  VARCHAR(255),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
