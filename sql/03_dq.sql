-- ============================================================
-- 03_dq.sql
-- Projet : RATP Data Engineering Pipeline
-- Objectif :
--   créer l'audit DQ et la quarantaine
--   appliquer DQ001, DQ002 et DQ003
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Table d'audit Data Quality
-- ============================================================

CREATE TABLE IF NOT EXISTS dq.ratp_events_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bronze_id BIGINT NOT NULL,
    event_id TEXT,
    rule_code TEXT NOT NULL,
    dq_status TEXT NOT NULL,
    details TEXT,
    source_file TEXT,
    batch_id TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (bronze_id, rule_code)
);

-- ============================================================
-- 2. Table de quarantaine
-- ============================================================

CREATE TABLE IF NOT EXISTS dq.ratp_events_quarantine (
    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bronze_id BIGINT NOT NULL,
    event_id TEXT,
    rule_code TEXT NOT NULL,
    dq_status TEXT NOT NULL,
    details TEXT,
    source_file TEXT,
    batch_id TEXT,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (bronze_id, rule_code)
);

-- ============================================================
-- 3. DQ001
-- passenger_count doit être un entier positif ou nul
-- ============================================================

INSERT INTO dq.ratp_events_audit (
    bronze_id,
    event_id,
    rule_code,
    dq_status,
    details,
    source_file,
    batch_id
)
SELECT
    b.bronze_id,
    b.event_id,
    'DQ001',
    'INVALID_PASSENGER_COUNT',
    'passenger_count doit etre un entier positif ou nul',
    b.source_file,
    b.batch_id
FROM bronze.ratp_events AS b
WHERE b.passenger_count IS NULL
   OR BTRIM(b.passenger_count) = ''
   OR BTRIM(b.passenger_count) !~ '^[0-9]+$'
ON CONFLICT (bronze_id, rule_code) DO NOTHING;

-- ============================================================
-- 4. DQ002
-- Détection des doublons exacts
-- ============================================================

WITH ranked_rows AS (
    SELECT
        b.bronze_id,
        b.event_id,
        b.source_file,
        b.batch_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                b.event_id,
                b.event_timestamp,
                b.line,
                b.station,
                b.event_type,
                b.passenger_count
            ORDER BY b.bronze_id
        ) AS duplicate_rank
    FROM bronze.ratp_events AS b
)
INSERT INTO dq.ratp_events_audit (
    bronze_id,
    event_id,
    rule_code,
    dq_status,
    details,
    source_file,
    batch_id
)
SELECT
    bronze_id,
    event_id,
    'DQ002',
    'EXACT_DUPLICATE',
    'Copie exacte supplementaire d''une ligne deja presente',
    source_file,
    batch_id
FROM ranked_rows
WHERE duplicate_rank > 1
ON CONFLICT (bronze_id, rule_code) DO NOTHING;

-- ============================================================
-- 5. DQ003
-- Même event_id avec plusieurs versions métier différentes
-- ============================================================

WITH conflicting_keys AS (
    SELECT
        event_id
    FROM bronze.ratp_events
    GROUP BY event_id
    HAVING COUNT(
        DISTINCT ROW(
            event_timestamp,
            line,
            station,
            event_type,
            passenger_count
        )
    ) > 1
)
INSERT INTO dq.ratp_events_audit (
    bronze_id,
    event_id,
    rule_code,
    dq_status,
    details,
    source_file,
    batch_id
)
SELECT
    b.bronze_id,
    b.event_id,
    'DQ003',
    'CONFLICTING_DUPLICATE',
    'Plusieurs versions contradictoires pour le meme event_id',
    b.source_file,
    b.batch_id
FROM bronze.ratp_events AS b
INNER JOIN conflicting_keys AS c
    ON b.event_id = c.event_id
ON CONFLICT (bronze_id, rule_code) DO NOTHING;

-- ============================================================
-- 6. Alimentation de la quarantaine
-- DQ001 et DQ003 seulement
-- ============================================================

INSERT INTO dq.ratp_events_quarantine (
    bronze_id,
    event_id,
    rule_code,
    dq_status,
    details,
    source_file,
    batch_id
)
SELECT
    a.bronze_id,
    a.event_id,
    a.rule_code,
    a.dq_status,
    a.details,
    a.source_file,
    a.batch_id
FROM dq.ratp_events_audit AS a
WHERE a.rule_code IN ('DQ001', 'DQ003')
ON CONFLICT (bronze_id, rule_code) DO NOTHING;

COMMIT;

-- ============================================================
-- Vérification de l'audit
-- ============================================================

SELECT
    rule_code,
    dq_status,
    COUNT(*) AS anomaly_count
FROM dq.ratp_events_audit
GROUP BY
    rule_code,
    dq_status
ORDER BY rule_code;

-- ============================================================
-- Vérification de la quarantaine
-- ============================================================

SELECT
    rule_code,
    COUNT(*) AS quarantine_count
FROM dq.ratp_events_quarantine
GROUP BY rule_code
ORDER BY rule_code;