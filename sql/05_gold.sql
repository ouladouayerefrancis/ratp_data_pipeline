-- ============================================================
-- 05_gold.sql
-- Projet : RATP Data Engineering Pipeline
-- Objectif :
--   créer et synchroniser la couche Gold
--   à partir de Silver
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Table Gold
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.daily_line_metrics (
    event_date DATE NOT NULL,
    line TEXT NOT NULL,

    total_events INTEGER NOT NULL,
    total_passengers BIGINT NOT NULL,

    validation_events INTEGER NOT NULL,
    incident_events INTEGER NOT NULL,

    refreshed_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (event_date, line)
);

-- ============================================================
-- 2. Agrégation Silver -> Gold
-- ============================================================

INSERT INTO gold.daily_line_metrics (
    event_date,
    line,
    total_events,
    total_passengers,
    validation_events,
    incident_events
)
SELECT
    s.event_timestamp::DATE AS event_date,
    s.line,
    COUNT(*)::INTEGER AS total_events,
    SUM(s.passenger_count) AS total_passengers,

    COUNT(*) FILTER (
        WHERE s.event_type = 'validation'
    )::INTEGER AS validation_events,

    COUNT(*) FILTER (
        WHERE s.event_type = 'incident'
    )::INTEGER AS incident_events

FROM silver.ratp_events AS s

GROUP BY
    s.event_timestamp::DATE,
    s.line

ON CONFLICT (event_date, line)
DO UPDATE SET
    total_events = EXCLUDED.total_events,
    total_passengers = EXCLUDED.total_passengers,
    validation_events = EXCLUDED.validation_events,
    incident_events = EXCLUDED.incident_events,
    refreshed_at = CURRENT_TIMESTAMP;

-- ============================================================
-- 3. Suppression des agrégats Gold devenus obsolètes
--
-- Exemple :
-- si toutes les lignes Silver d'une date/ligne disparaissent
-- après une révocation DQ, Gold ne doit pas conserver
-- une ancienne agrégation.
-- ============================================================

DELETE FROM gold.daily_line_metrics AS g
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.ratp_events AS s
    WHERE s.event_timestamp::DATE = g.event_date
      AND s.line = g.line
);

COMMIT;

-- ============================================================
-- 4. Vérification Gold
-- ============================================================

SELECT
    event_date,
    line,
    total_events,
    total_passengers,
    validation_events,
    incident_events
FROM gold.daily_line_metrics
ORDER BY event_date, line;

-- ============================================================
-- 5. Contrôle de volumétrie
-- ============================================================

SELECT
    COUNT(*) AS gold_row_count,
    SUM(total_events) AS gold_event_count,
    SUM(total_passengers) AS gold_passenger_count
FROM gold.daily_line_metrics;