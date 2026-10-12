# tests/test_gold.py
# Tests automatisés de la construction et de la validation GOLD

import pandas as pd
import pytest

from src.gold import (
    build_gold_daily_line,
    validate_gold,
)


# ============================================================
# FONCTION UTILITAIRE
# ============================================================

def build_test_silver() -> pd.DataFrame:
    """
    Construit un jeu de données Silver synthétique.

    Nous avons :
    - 4 événements Silver ;
    - 2 dates ;
    - 2 lignes RATP ;
    - validations et incidents.
    """

    return pd.DataFrame(
        {
            "event_id": [
                1001,
                1002,
                1003,
                1004,
            ],

            "event_timestamp": [
                "2026-10-01 08:00:00",
                "2026-10-01 09:00:00",
                "2026-10-01 10:00:00",
                "2026-10-02 08:00:00",
            ],

            "line": [
                "A",
                "A",
                "B",
                "A",
            ],

            "station": [
                "Nation",
                "Châtelet",
                "Gare du Nord",
                "Nation",
            ],

            "event_type": [
                "validation",
                "incident",
                "validation",
                "validation",
            ],

            "passenger_count": [
                100,
                0,
                150,
                120,
            ],

            "batch_id": [
                "2026-10-01",
                "2026-10-01",
                "2026-10-01",
                "2026-10-02",
            ],

            "source_file": [
                "batch_1.csv",
                "batch_1.csv",
                "batch_1.csv",
                "batch_2.csv",
            ],

            "ingestion_timestamp": [
                "2026-10-01 11:00:00",
                "2026-10-01 11:00:00",
                "2026-10-01 11:00:00",
                "2026-10-02 09:00:00",
            ],
        }
    )


# ============================================================
# TEST 1 — CONSTRUCTION ET AGRÉGATION GOLD
# ============================================================

def test_build_gold_daily_line():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_silver = build_test_silver()

    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    df_gold = build_gold_daily_line(
        df_silver
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    # Grain attendu :
    #
    # 2026-10-01 + A
    # 2026-10-01 + B
    # 2026-10-02 + A

    assert len(df_gold) == 3


# ============================================================
# TEST 2 — KPI GOLD
# ============================================================

def test_gold_kpis_are_correct():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_silver = build_test_silver()

    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    df_gold = build_gold_daily_line(
        df_silver
    )

    # Sélection :
    # 2026-10-01 / ligne A

    row = df_gold[
        (
            df_gold["event_date"]
            .astype(str)
            == "2026-10-01"
        )
        &
        (
            df_gold["line"]
            == "A"
        )
    ].iloc[0]

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert row["total_events"] == 2

    assert row["total_passengers"] == 100

    assert row["validation_count"] == 1

    assert row["incident_count"] == 1

    assert row["station_count"] == 2

    assert row["incident_rate"] == pytest.approx(
        0.5
    )


# ============================================================
# TEST 3 — GRAIN GOLD UNIQUE
# ============================================================

def test_gold_grain_is_unique():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_silver = build_test_silver()

    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    df_gold = build_gold_daily_line(
        df_silver
    )

    duplicate_grain_count = (
        df_gold
        .duplicated(
            subset=[
                "event_date",
                "line",
            ]
        )
        .sum()
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert duplicate_grain_count == 0


# ============================================================
# TEST 4 — TOUS LES CONTRÔLES GOLD PASSENT
# ============================================================

def test_validate_gold_passes():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_silver = build_test_silver()

    df_gold = build_gold_daily_line(
        df_silver
    )

    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    checks = validate_gold(
        df_gold=df_gold,
        silver_row_count=len(
            df_silver
        ),
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert all(
        checks.values()
    )

    assert (
        checks["unique_gold_grain"]
        is True
    )

    assert (
        checks["no_null_gold_keys"]
        is True
    )

    assert (
        checks["valid_incident_rate"]
        is True
    )

    assert (
        checks["silver_gold_reconciliation"]
        is True
    )


# ============================================================
# TEST 5 — ÉCHEC DE RÉCONCILIATION SILVER / GOLD
# ============================================================

def test_gold_detects_reconciliation_failure():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_silver = build_test_silver()

    df_gold = build_gold_daily_line(
        df_silver
    )

    # Nous simulons volontairement
    # un mauvais nombre de lignes Silver.

    wrong_silver_row_count = 999

    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    checks = validate_gold(
        df_gold=df_gold,
        silver_row_count=wrong_silver_row_count,
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert (
        checks["silver_gold_reconciliation"]
        is False
    )


# ============================================================
# TEST 6 — DÉTECTION D'UN GRAIN GOLD DUPLIQUÉ
# ============================================================

def test_gold_detects_duplicate_grain():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_silver = build_test_silver()

    df_gold = build_gold_daily_line(
        df_silver
    )

    # Nous ajoutons volontairement
    # une copie de la première ligne Gold.

    duplicate_row = (
        df_gold
        .iloc[
            [0]
        ]
        .copy()
    )

    df_gold_corrupted = pd.concat(
        [
            df_gold,
            duplicate_row,
        ],
        ignore_index=True,
    )

    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    checks = validate_gold(
        df_gold=df_gold_corrupted,
        silver_row_count=len(
            df_silver
        ),
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert (
        checks["unique_gold_grain"]
        is False
    )


# ============================================================
# TEST 7 — COLONNE SILVER OBLIGATOIRE MANQUANTE
# ============================================================

def test_gold_blocks_missing_required_column():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_silver = build_test_silver()

    df_silver = df_silver.drop(
        columns=[
            "station",
        ]
    )

    # --------------------------------------------------------
    # ACT + ASSERT
    # --------------------------------------------------------

    with pytest.raises(
        ValueError,
        match="Colonnes Silver manquantes",
    ):

        build_gold_daily_line(
            df_silver
        )