# tests/test_data_quality.py
# Tests automatisés des règles Data Quality

import pandas as pd

from src.data_quality import (
    classify_duplicates,
    build_dq_outputs,
)


# ============================================================
# FONCTION UTILITAIRE
# ============================================================

def build_test_bronze() -> pd.DataFrame:
    """
    Construit un petit Bronze de test contenant :

    - des lignes conformes ;
    - un doublon exact ;
    - un doublon conflictuel.
    """

    return pd.DataFrame(
        {
            "event_id": [
                1001,
                1002,
                1002,
                1003,
                1003,
            ],

            "event_timestamp": [
                "2026-10-01 08:00:00",
                "2026-10-01 08:05:00",
                "2026-10-01 08:05:00",
                "2026-10-01 08:10:00",
                "2026-10-01 08:10:00",
            ],

            "line": [
                "A",
                "B",
                "B",
                "C",
                "C",
            ],

            "station": [
                "Nation",
                "Châtelet",
                "Châtelet",
                "La Défense",
                "La Défense",
            ],

            "event_type": [
                "validation",
                "validation",
                "validation",
                "incident",
                "incident",
            ],

            "passenger_count": [
                100,
                150,
                150,
                0,
                999,
            ],

            "batch_id": [
                "2026-10-01",
                "2026-10-01",
                "2026-10-01",
                "2026-10-01",
                "2026-10-02",
            ],

            "source_file": [
                "batch_1.csv",
                "batch_1.csv",
                "batch_1.csv",
                "batch_1.csv",
                "batch_2.csv",
            ],

            "ingestion_timestamp": [
                "2026-10-01 09:00:00",
                "2026-10-01 09:00:00",
                "2026-10-01 09:00:00",
                "2026-10-01 09:00:00",
                "2026-10-02 09:00:00",
            ],
        }
    )


# ============================================================
# TEST 1 — DÉTECTION DU DOUBLON EXACT
# ============================================================

def test_exact_duplicate_detection():

    df_bronze = build_test_bronze()

    (
        df_pass,
        df_exact_duplicates,
        df_conflicts,
    ) = classify_duplicates(
        df_bronze
    )

    assert len(df_exact_duplicates) == 1

    assert (
        df_exact_duplicates["event_id"]
        .iloc[0]
        == 1002
    )


# ============================================================
# TEST 2 — DÉTECTION DU CONFLIT
# ============================================================

def test_conflicting_duplicate_detection():

    df_bronze = build_test_bronze()

    (
        df_pass,
        df_exact_duplicates,
        df_conflicts,
    ) = classify_duplicates(
        df_bronze
    )

    assert len(df_conflicts) == 2

    assert set(
        df_conflicts["event_id"]
    ) == {1003}


# ============================================================
# TEST 3 — LIGNES PASS
# ============================================================

def test_pass_rows():

    df_bronze = build_test_bronze()

    (
        df_pass,
        df_exact_duplicates,
        df_conflicts,
    ) = classify_duplicates(
        df_bronze
    )

    assert len(df_pass) == 2

    assert set(
        df_pass["event_id"]
    ) == {
        1001,
        1002,
    }


# ============================================================
# TEST 4 — RÉCONCILIATION BRONZE / DQ
# ============================================================

def test_bronze_dq_reconciliation():

    df_bronze = build_test_bronze()

    (
        df_pass,
        df_exact_duplicates,
        df_conflicts,
    ) = classify_duplicates(
        df_bronze
    )

    total_explained = (
        len(df_pass)
        + len(df_exact_duplicates)
        + len(df_conflicts)
    )

    assert total_explained == len(
        df_bronze
    )


# ============================================================
# TEST 5 — AUDIT DQ / QUARANTAINE
# ============================================================

def test_dq_outputs():

    df_bronze = build_test_bronze()

    (
        df_pass,
        df_exact_duplicates,
        df_conflicts,
    ) = classify_duplicates(
        df_bronze
    )

    (
        df_dq_audit,
        df_quarantine,
    ) = build_dq_outputs(
        df_exact_duplicates=df_exact_duplicates,
        df_conflicts=df_conflicts,
    )

    # 1 EXACT_DUPLICATE
    # +
    # 2 lignes CONFLICTING_DUPLICATE
    assert len(df_dq_audit) == 3

    # Seuls les conflits vont en quarantaine
    assert len(df_quarantine) == 2

    assert set(
        df_dq_audit["dq_rule_id"]
    ) == {
        "DQ001",
        "DQ002",
    }

    assert (
        df_quarantine["dq_reason"]
        == "CONFLICTING_DUPLICATE"
    ).all()