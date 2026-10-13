-- ============================================================
-- 07_reconciliation.sql
-- Projet : RATP Data Engineering Pipeline
-- Objectif :
--   vérifier la cohérence Bronze / DQ / Silver / Gold
--   autoriser le checkpoint uniquement si tous les contrôles
--   sont PASS
-- ============================================================

-- ============================================================
-- 1. Réconciliation Bronze
--
-- Catégories exclusives :
--   - lignes éligibles Silver
--   - doublons exacts uniquement
--   - lignes en quarantaine
-- ============================================================

WITH reconciliation AS (
    SELECT
        COUNT(*) AS bronze_count,

        COUNT(*) FILTER (
            WHERE NOT EXISTS (
                SELECT 1
                FROM dq.ratp_events_audit AS a
                WHERE a.bronze_id = b.bronze_id
            )
        ) AS eligible_silver_count,

        COUNT(*) FILTER (
            WHERE EXISTS (
                SELECT 1
                FROM dq.ratp_events_audit AS a
                WHERE a.bronze_id = b.bronze_id
                  AND a.rule_code = 'DQ002'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM dq.ratp_events_quarantine AS q
                WHERE q.bronze_id = b.bronze_id
            )
        ) AS exact_duplicate_count,

        COUNT(*) FILTER (
            WHERE EXISTS (
                SELECT 1
                FROM dq.ratp_events_quarantine AS q
                WHERE q.bronze_id = b.bronze_id
            )
        ) AS quarantine_count

    FROM bronze.ratp_events AS b
)
SELECT
    bronze_count,
    eligible_silver_count,
    exact_duplicate_count,
    quarantine_count,

    eligible_silver_count
        + exact_duplicate_count
        + quarantine_count AS reconciled_count,

    CASE
        WHEN bronze_count =
             eligible_silver_count
             + exact_duplicate_count
             + quarantine_count
        THEN 'PASS'
        ELSE 'FAIL'
    END AS bronze_reconciliation

FROM reconciliation;

-- ============================================================
-- 2. Réconciliation Silver / Gold
-- ============================================================

SELECT
    (SELECT COUNT(*)
     FROM silver.ratp_events) AS silver_events,

    COALESCE(
        (SELECT SUM(total_events)
         FROM gold.daily_line_metrics),
        0
    ) AS gold_events,

    (SELECT SUM(passenger_count)
     FROM silver.ratp_events) AS silver_passengers,

    COALESCE(
        (SELECT SUM(total_passengers)
         FROM gold.daily_line_metrics),
        0
    ) AS gold_passengers;

-- ============================================================
-- 3. Quality Gate final
-- ============================================================

WITH metrics AS (
    SELECT
        (SELECT COUNT(*)
         FROM bronze.ratp_events) AS bronze_count,

        (
            SELECT COUNT(*)
            FROM bronze.ratp_events AS b
            WHERE NOT EXISTS (
                SELECT 1
                FROM dq.ratp_events_audit AS a
                WHERE a.bronze_id = b.bronze_id
            )
        ) AS eligible_silver_count,

        (
            SELECT COUNT(*)
            FROM bronze.ratp_events AS b
            WHERE EXISTS (
                SELECT 1
                FROM dq.ratp_events_audit AS a
                WHERE a.bronze_id = b.bronze_id
                  AND a.rule_code = 'DQ002'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM dq.ratp_events_quarantine AS q
                WHERE q.bronze_id = b.bronze_id
            )
        ) AS exact_duplicate_count,

        (
            SELECT COUNT(*)
            FROM bronze.ratp_events AS b
            WHERE EXISTS (
                SELECT 1
                FROM dq.ratp_events_quarantine AS q
                WHERE q.bronze_id = b.bronze_id
            )
        ) AS quarantine_count,

        (SELECT COUNT(*)
         FROM silver.ratp_events) AS silver_count,

        COALESCE(
            (SELECT SUM(total_events)
             FROM gold.daily_line_metrics),
            0
        ) AS gold_event_count,

        COALESCE(
            (SELECT SUM(passenger_count)
             FROM silver.ratp_events),
            0
        ) AS silver_passenger_count,

        COALESCE(
            (SELECT SUM(total_passengers)
             FROM gold.daily_line_metrics),
            0
        ) AS gold_passenger_count
)
SELECT
    CASE
        WHEN bronze_count =
             eligible_silver_count
             + exact_duplicate_count
             + quarantine_count

         AND silver_count = eligible_silver_count

         AND silver_count = gold_event_count

         AND silver_passenger_count = gold_passenger_count

        THEN 'PASS'
        ELSE 'FAIL'
    END AS pipeline_quality_gate
FROM metrics;

-- ============================================================
-- 4. Avancement sécurisé du watermark
--
-- Le watermark avance uniquement si tous les contrôles
-- précédents sont cohérents.
-- ============================================================

WITH metrics AS (
    SELECT
        (SELECT COUNT(*)
         FROM bronze.ratp_events) AS bronze_count,

        (
            SELECT COUNT(*)
            FROM bronze.ratp_events AS b
            WHERE NOT EXISTS (
                SELECT 1
                FROM dq.ratp_events_audit AS a
                WHERE a.bronze_id = b.bronze_id
            )
        ) AS eligible_silver_count,

        (
            SELECT COUNT(*)
            FROM bronze.ratp_events AS b
            WHERE EXISTS (
                SELECT 1
                FROM dq.ratp_events_audit AS a
                WHERE a.bronze_id = b.bronze_id
                  AND a.rule_code = 'DQ002'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM dq.ratp_events_quarantine AS q
                WHERE q.bronze_id = b.bronze_id
            )
        ) AS exact_duplicate_count,

        (
            SELECT COUNT(*)
            FROM bronze.ratp_events AS b
            WHERE EXISTS (
                SELECT 1
                FROM dq.ratp_events_quarantine AS q
                WHERE q.bronze_id = b.bronze_id
            )
        ) AS quarantine_count,

        (SELECT COUNT(*)
         FROM silver.ratp_events) AS silver_count,

        COALESCE(
            (SELECT SUM(total_events)
             FROM gold.daily_line_metrics),
            0
        ) AS gold_event_count,

        COALESCE(
            (SELECT SUM(passenger_count)
             FROM silver.ratp_events),
            0
        ) AS silver_passenger_count,

        COALESCE(
            (SELECT SUM(total_passengers)
             FROM gold.daily_line_metrics),
            0
        ) AS gold_passenger_count
),
gate AS (
    SELECT
        bronze_count =
            eligible_silver_count
            + exact_duplicate_count
            + quarantine_count

        AND silver_count = eligible_silver_count

        AND silver_count = gold_event_count

        AND silver_passenger_count = gold_passenger_count
        AS gate_pass

    FROM metrics
)
UPDATE ops.pipeline_watermark
SET
    last_bronze_id = (
        SELECT COALESCE(MAX(bronze_id), 0)
        FROM bronze.ratp_events
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE pipeline_name = 'ratp_events_pipeline'
  AND (SELECT gate_pass FROM gate);

-- ============================================================
-- 5. Vérification finale du checkpoint
-- ============================================================

SELECT
    pipeline_name,
    last_bronze_id,
    updated_at
FROM ops.pipeline_watermark
WHERE pipeline_name = 'ratp_events_pipeline';