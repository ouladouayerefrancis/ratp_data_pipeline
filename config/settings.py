# config/settings.py
# Configuration centralisée du pipeline RATP

from pathlib import Path


# ============================================================
# RACINE DU PROJET
# ============================================================

BASE_DIR = (
    Path(__file__)
    .resolve()
    .parents[1]
)


# ============================================================
# DOSSIER DATA
# ============================================================

DATA_DIR = (
    BASE_DIR
    / "data"
)


# ============================================================
# COUCHES DU PIPELINE
# ============================================================

RAW_DIR = (
    DATA_DIR
    / "raw"
)

BRONZE_DIR = (
    DATA_DIR
    / "bronze"
)

DQ_AUDIT_DIR = (
    DATA_DIR
    / "dq_audit"
)

QUARANTINE_DIR = (
    DATA_DIR
    / "quarantine"
)

SILVER_DIR = (
    DATA_DIR
    / "silver"
)

GOLD_DIR = (
    DATA_DIR
    / "gold"
)


# ============================================================
# DOSSIER DE LOGS
# ============================================================

LOG_DIR = (
    BASE_DIR
    / "logs"
)


# ============================================================
# PARAMÈTRES DU BATCH ACTUEL
# ============================================================

BATCH_ID = "2026-10-12"

RAW_FILE = (
    RAW_DIR
    / "ratp_events_20261012.csv"
)

BRONZE_FILE = (
    BRONZE_DIR
    / "ratp_events_bronze_v2.csv"
)