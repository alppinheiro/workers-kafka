-- Otimização de banco identificada no stress de 120k (docs/STRESS_TEST.md §3.5):
-- 1) metrics-exporter faz GROUP BY current_status a cada 10s — sem índice era seq scan
--    na tabela inteira de sagas (120k+ linhas) a cada coleta.
-- 2) o purge da outbox (DELETE ... WHERE published_at IS NOT NULL) varria a tabela toda
--    a cada hora — o índice parcial idx_outbox_pending só cobre published_at IS NULL.

CREATE INDEX IF NOT EXISTS idx_sagas_status ON sagas(current_status);

CREATE INDEX IF NOT EXISTS idx_outbox_published_at ON outbox(published_at);

-- Autovacuum mais agressivo nas tabelas de alta escrita (outbox: INSERT/UPDATE/DELETE
-- contínuos; sagas: UPDATE por etapa). Default scale_factor 0.2/threshold 50 é lento
-- sob carga — dead tuples acumulam e degradam os scans.
ALTER TABLE outbox SET (
    autovacuum_vacuum_scale_factor  = 0.05,
    autovacuum_vacuum_threshold     = 2000,
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold    = 2000
);

ALTER TABLE sagas SET (
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold    = 2000
);
