-- ============================================================
-- 02_bronze.sql
-- Projet : RATP Data Engineering Pipeline
-- Objectif :
--   créer la table Bronze contenant les données proches
--   de la source ainsi que les métadonnées d'ingestion
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS bronze.ratp_events (
    bronze_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    event_id TEXT,
    event_timestamp TEXT,
    line TEXT,
    station TEXT,
    event_type TEXT,
    passenger_count TEXT,

    source_file TEXT NOT NULL,
    batch_id TEXT NOT NULL,

    ingested_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);

COMMIT;

-- ============================================================
-- Vérification de la structure
-- ============================================================

SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'bronze'
  AND table_name = 'ratp_events'
ORDER BY ordinal_position;

-- ============================================================
-- Vérification de la volumétrie actuelle
-- ============================================================

SELECT
    COUNT(*) AS bronze_row_count,
    MAX(bronze_id) AS max_bronze_id
FROM bronze.ratp_events;