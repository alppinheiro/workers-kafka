DROP INDEX IF EXISTS idx_sagas_status;
DROP INDEX IF EXISTS idx_outbox_published_at;

ALTER TABLE outbox RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold
);

ALTER TABLE sagas RESET (
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold
);
