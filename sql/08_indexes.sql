-- ============================================================
-- 08_indexes.sql
-- Projet : RATP Data Engineering Pipeline
-- Objectif :
--   créer les index utiles aux recherches, jointures
--   et contrôles Data Quality
-- ============================================================

-- ============================================================
-- 1. Recherche et jointure sur la clé métier event_id
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_bronze_ratp_events_event_id
ON bronze.ratp_events (event_id);

-- ============================================================
-- 2. Idempotence d'ingestion :
-- recherche par fichier source + event_id
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_bronze_source_file_event_id
ON bronze.ratp_events (source_file, event_id);

-- ============================================================
-- 3. Recherche des anomalies DQ par event_id / règle
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_dq_audit_event_id_rule
ON dq.ratp_events_audit (event_id, rule_code);

-- ============================================================
-- 4. Recherche dans la quarantaine
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_dq_quarantine_event_id_rule
ON dq.ratp_events_quarantine (event_id, rule_code);

-- ============================================================
-- 5. Agrégations Silver par date / ligne
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_silver_event_timestamp_line
ON silver.ratp_events (event_timestamp, line);

-- ============================================================
-- 6. Mise à jour des statistiques du planner PostgreSQL
-- ============================================================

ANALYZE bronze.ratp_events;
ANALYZE dq.ratp_events_audit;
ANALYZE dq.ratp_events_quarantine;
ANALYZE silver.ratp_events;
ANALYZE gold.daily_line_metrics;

-- ============================================================
-- 7. Vérification des index du projet
-- ============================================================

SELECT
    schemaname,
    tablename,
    indexname
FROM pg_indexes
WHERE schemaname IN (
    'bronze',
    'dq',
    'silver',
    'gold',
    'ops'
)
ORDER BY
    schemaname,
    tablename,
    indexname;