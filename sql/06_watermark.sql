-- ============================================================
-- 06_watermark.sql
-- Projet : RATP Data Engineering Pipeline
-- Objectif :
--   créer et initialiser le watermark du pipeline
--   afficher l'état du traitement incrémental
--
-- Important :
--   ce script n'avance PAS automatiquement le watermark.
--   Le checkpoint final est validé seulement après les
--   contrôles de réconciliation.
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Table de watermark
-- ============================================================

CREATE TABLE IF NOT EXISTS ops.pipeline_watermark (
    pipeline_name TEXT PRIMARY KEY,
    last_bronze_id BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 2. Initialisation
--
-- Si le pipeline n'existe pas encore, son watermark démarre à 0.
-- Si le pipeline existe déjà, on ne modifie rien.
-- ============================================================

INSERT INTO ops.pipeline_watermark (
    pipeline_name,
    last_bronze_id
)
VALUES (
    'ratp_events_pipeline',
    0
)
ON CONFLICT (pipeline_name) DO NOTHING;

COMMIT;

-- ============================================================
-- 3. État actuel du watermark
-- ============================================================

SELECT
    pipeline_name,
    last_bronze_id,
    updated_at
FROM ops.pipeline_watermark
WHERE pipeline_name = 'ratp_events_pipeline';

-- ============================================================
-- 4. Dernier bronze_id disponible
-- ============================================================

SELECT
    MAX(bronze_id) AS max_bronze_id
FROM bronze.ratp_events;

-- ============================================================
-- 5. Lignes encore non checkpointées
-- ============================================================

SELECT
    b.bronze_id,
    b.event_id,
    b.batch_id
FROM bronze.ratp_events AS b
WHERE b.bronze_id > (
    SELECT w.last_bronze_id
    FROM ops.pipeline_watermark AS w
    WHERE w.pipeline_name = 'ratp_events_pipeline'
)
ORDER BY b.bronze_id;

-- ============================================================
-- 6. Nombre de lignes encore non checkpointées
-- ============================================================

SELECT
    COUNT(*) AS rows_after_watermark
FROM bronze.ratp_events AS b
WHERE b.bronze_id > (
    SELECT w.last_bronze_id
    FROM ops.pipeline_watermark AS w
    WHERE w.pipeline_name = 'ratp_events_pipeline'
);