CREATE TABLE IF NOT EXISTS order_views (
    order_id              VARCHAR(255) PRIMARY KEY,
    current_status        VARCHAR(50)  NOT NULL,
    last_event_type       VARCHAR(50),
    last_event_at         TIMESTAMPTZ,
    transaction_id        VARCHAR(255),
    notification_error    BOOLEAN NOT NULL DEFAULT false,
    payment_refund_failed BOOLEAN NOT NULL DEFAULT false,
    timeline              JSONB NOT NULL DEFAULT '[]',
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
