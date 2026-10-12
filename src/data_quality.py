# src/data_quality.py
# Contrôles Data Quality appliqués à la couche Bronze

import pandas as pd


# ============================================================
# CONFIGURATION DATA QUALITY
# ============================================================

KEY_COLUMN = "event_id"

BUSINESS_COLUMNS = [
    "event_timestamp",
    "line",
    "station",
    "event_type",
    "passenger_count",
]


# ============================================================
# 1. CLASSIFICATION DES DOUBLONS
# ============================================================

def classify_duplicates(
    df_bronze: pd.DataFrame,
):
    """
    Classe les lignes Bronze dans trois catégories exclusives :

    - PASS
    - EXACT_DUPLICATE
    - CONFLICTING_DUPLICATE

    Le DataFrame Bronze n'est jamais modifié.
    """

    required_columns = (
        [KEY_COLUMN]
        + BUSINESS_COLUMNS
    )

    missing_columns = [
        column
        for column in required_columns
        if column not in df_bronze.columns
    ]

    if missing_columns:
        raise ValueError(
            "Colonnes Bronze manquantes pour les contrôles DQ : "
            f"{missing_columns}"
        )

    # ========================================================
    # ÉTAPE 1 — VERSIONS MÉTIER DISTINCTES
    # ========================================================

    distinct_versions = (
        df_bronze[
            required_columns
        ]
        .drop_duplicates()
    )

    # ========================================================
    # ÉTAPE 2 — NOMBRE DE VERSIONS PAR EVENT_ID
    # ========================================================

    version_counts = (
        distinct_versions
        .groupby(KEY_COLUMN)
        .size()
    )

    # ========================================================
    # ÉTAPE 3 — CLÉS CONFLICTUELLES
    # ========================================================

    conflict_keys = (
        version_counts[
            version_counts > 1
        ]
        .index
    )

    # ========================================================
    # ÉTAPE 4 — CONFLICTING_DUPLICATE
    # ========================================================

    conflict_mask = (
        df_bronze[KEY_COLUMN]
        .isin(conflict_keys)
    )

    df_conflicts = (
        df_bronze[
            conflict_mask
        ]
        .copy()
    )

    # ========================================================
    # ÉTAPE 5 — DONNÉES HORS CONFLITS
    # ========================================================

    df_without_conflicts = (
        df_bronze[
            ~conflict_mask
        ]
        .copy()
    )

    # ========================================================
    # ÉTAPE 6 — EXACT_DUPLICATE
    # ========================================================

    exact_duplicate_mask = (
        df_without_conflicts
        .duplicated(
            subset=required_columns,
            keep="first",
        )
    )

    df_exact_duplicates = (
        df_without_conflicts[
            exact_duplicate_mask
        ]
        .copy()
    )

    # ========================================================
    # ÉTAPE 7 — PASS
    # ========================================================

    df_pass = (
        df_without_conflicts[
            ~exact_duplicate_mask
        ]
        .copy()
    )

    return (
        df_pass,
        df_exact_duplicates,
        df_conflicts,
    )


# ============================================================
# 2. AUDIT DQ ET QUARANTAINE
# ============================================================

def build_dq_outputs(
    df_exact_duplicates: pd.DataFrame,
    df_conflicts: pd.DataFrame,
):
    """
    Construit :

    - le journal d'audit Data Quality ;
    - la quarantaine des conflits métier.
    """

    # ========================================================
    # DQ001 — EXACT_DUPLICATE
    # ========================================================

    df_dq_exact = (
        df_exact_duplicates
        .copy()
    )

    df_dq_exact["dq_rule_id"] = "DQ001"
    df_dq_exact["dq_status"] = "FAIL"
    df_dq_exact["dq_reason"] = "EXACT_DUPLICATE"
    df_dq_exact["dq_action"] = "EXCLUDED_FROM_SILVER"
    df_dq_exact["dq_detected_at"] = pd.Timestamp.now()

    # ========================================================
    # DQ002 — CONFLICTING_DUPLICATE
    # ========================================================

    df_dq_conflicts = (
        df_conflicts
        .copy()
    )

    df_dq_conflicts["dq_rule_id"] = "DQ002"
    df_dq_conflicts["dq_status"] = "FAIL"
    df_dq_conflicts["dq_reason"] = "CONFLICTING_DUPLICATE"
    df_dq_conflicts["dq_action"] = "QUARANTINE"
    df_dq_conflicts["dq_detected_at"] = pd.Timestamp.now()

    # ========================================================
    # AUDIT COMPLET
    # ========================================================

    df_dq_audit = pd.concat(
        [
            df_dq_exact,
            df_dq_conflicts,
        ],
        ignore_index=True,
    )

    # ========================================================
    # QUARANTAINE
    # ========================================================

    # Seuls les conflits nécessitant une investigation / RCA
    # sont envoyés en quarantaine.
    df_quarantine = (
        df_dq_conflicts
        .copy()
    )

    return (
        df_dq_audit,
        df_quarantine,
    )