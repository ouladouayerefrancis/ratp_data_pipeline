-- ============================================================
-- 04_silver.sql
-- Projet : RATP Data Engineering Pipeline
-- Objectif :
--   créer Silver
--   révoquer les événements devenus conflictuels
--   promouvoir uniquement les lignes Bronze autorisées
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Table Silver
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.ratp_events (
    event_id BIGINT PRIMARY KEY,

    event_timestamp TIMESTAMP NOT NULL,
    line TEXT NOT NULL,
    station TEXT NOT NULL,
    event_type TEXT NOT NULL,

    passenger_count INTEGER NOT NULL
        CHECK (passenger_count >= 0),

    source_bronze_id BIGINT NOT NULL UNIQUE,
    source_file TEXT NOT NULL,
    batch_id TEXT NOT NULL,

    promoted_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 2. Révocation
--
-- Si un event_id déjà présent dans Silver devient ensuite
-- conflictuel (DQ003), il doit être retiré de Silver.
-- ============================================================

DELETE FROM silver.ratp_events AS s
WHERE EXISTS (
    SELECT 1
    FROM dq.ratp_events_quarantine AS q
    WHERE q.event_id = s.event_id::TEXT
      AND q.rule_code = 'DQ003'
);

-- ============================================================
-- 3. Promotion Bronze -> Silver
--
-- Une ligne Bronze peut passer uniquement si aucune anomalie
-- DQ n'est enregistrée pour son bronze_id.
-- ============================================================

INSERT INTO silver.ratp_events (
    event_id,
    event_timestamp,
    line,
    station,
    event_type,
    passenger_count,
    source_bronze_id,
    source_file,
    batch_id
)
SELECT
    b.event_id::BIGINT,
    b.event_timestamp::TIMESTAMP,
    b.line,
    b.station,
    b.event_type,
    b.passenger_count::INTEGER,
    b.bronze_id,
    b.source_file,
    b.batch_id
FROM bronze.ratp_events AS b
WHERE NOT EXISTS (
    SELECT 1
    FROM dq.ratp_events_audit AS a
    WHERE a.bronze_id = b.bronze_id
)
ON CONFLICT (event_id) DO NOTHING;

COMMIT;

-- ============================================================
-- 4. Vérification Silver
-- ============================================================

SELECT
    event_id,
    event_timestamp,
    line,
    station,
    passenger_count,
    source_bronze_id
FROM silver.ratp_events
ORDER BY event_id;

SELECT
    COUNT(*) AS silver_row_count,
    SUM(passenger_count) AS silver_passenger_count
FROM silver.ratp_events;