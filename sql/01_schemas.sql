-- ============================================================
-- 01_schemas.sql
-- Projet : RATP Data Engineering Pipeline
-- Objectif : créer les schémas logiques du pipeline
-- ============================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION ratp_de;

CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION ratp_de;

CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION ratp_de;

CREATE SCHEMA IF NOT EXISTS dq AUTHORIZATION ratp_de;

CREATE SCHEMA IF NOT EXISTS ops AUTHORIZATION ratp_de;

COMMIT;

-- ============================================================
-- Vérification
-- ============================================================

SELECT
    schema_name
FROM information_schema.schemata
WHERE schema_name IN (
    'bronze',
    'silver',
    'gold',
    'dq',
    'ops'
)
ORDER BY schema_name;