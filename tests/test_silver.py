# tests/test_silver.py
# Tests automatisés de la construction de la couche SILVER

import pandas as pd
import pytest

from src.silver import build_silver


# ============================================================
# FONCTION UTILITAIRE
# ============================================================

def build_valid_pass_data() -> pd.DataFrame:
    """
    Construit un petit jeu de données synthétique valide
    pouvant être promu vers Silver.
    """

    return pd.DataFrame(
        {
            "event_id": [
                1001,
                1002,
            ],

            "event_timestamp": [
                "2026-10-01 08:00:00",
                "2026-10-01 08:05:00",
            ],

            "line": [
                "A",
                "B",
            ],

            "station": [
                "Nation",
                "Châtelet",
            ],

            "event_type": [
                "validation",
                "incident",
            ],

            "passenger_count": [
                100,
                0,
            ],

            "batch_id": [
                "2026-10-01",
                "2026-10-01",
            ],

            "source_file": [
                "ratp_test.csv",
                "ratp_test.csv",
            ],

            "ingestion_timestamp": [
                "2026-10-01 09:00:00",
                "2026-10-01 09:00:00",
            ],
        }
    )


# ============================================================
# TEST 1 — CONSTRUCTION SILVER VALIDE
# ============================================================

def test_build_valid_silver():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_pass = build_valid_pass_data()

    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    df_silver = build_silver(
        df_pass
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert len(df_silver) == 2

    assert (
        df_silver["event_id"]
        .nunique()
        == 2
    )

    assert (
        df_silver["event_id"]
        .duplicated()
        .sum()
        == 0
    )

    assert pd.api.types.is_datetime64_any_dtype(
        df_silver["event_timestamp"]
    )

    assert pd.api.types.is_numeric_dtype(
        df_silver["passenger_count"]
    )


# ============================================================
# TEST 2 — EVENT_ID DUPLIQUÉ
# ============================================================

def test_silver_blocks_duplicate_event_id():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_pass = build_valid_pass_data()

    df_pass.loc[
        1,
        "event_id",
    ] = 1001

    # --------------------------------------------------------
    # ACT + ASSERT
    # --------------------------------------------------------

    with pytest.raises(
        ValueError,
        match="event_id dupliqué",
    ):

        build_silver(
            df_pass
        )


# ============================================================
# TEST 3 — EVENT_ID NULL
# ============================================================

def test_silver_blocks_null_event_id():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_pass = build_valid_pass_data()

    df_pass.loc[
        0,
        "event_id",
    ] = None

    # --------------------------------------------------------
    # ACT + ASSERT
    # --------------------------------------------------------

    with pytest.raises(
        ValueError,
        match="event_id",
    ):

        build_silver(
            df_pass
        )


# ============================================================
# TEST 4 — TIMESTAMP INVALIDE
# ============================================================

def test_silver_blocks_invalid_timestamp():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_pass = build_valid_pass_data()

    df_pass.loc[
        0,
        "event_timestamp",
    ] = "NOT_A_DATE"

    # --------------------------------------------------------
    # ACT + ASSERT
    # --------------------------------------------------------

    with pytest.raises(
        ValueError,
        match="timestamp",
    ):

        build_silver(
            df_pass
        )


# ============================================================
# TEST 5 — PASSENGER_COUNT NON NUMÉRIQUE
# ============================================================

def test_silver_blocks_invalid_passenger_count():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_pass = build_valid_pass_data()

    # La colonne est volontairement convertie en object
    # afin de pouvoir simuler une donnée texte corrompue
    # sans générer de FutureWarning Pandas.

    df_pass["passenger_count"] = (
        df_pass["passenger_count"]
        .astype("object")
    )

    df_pass.loc[
        0,
        "passenger_count",
    ] = "ABC"

    # --------------------------------------------------------
    # ACT + ASSERT
    # --------------------------------------------------------

    with pytest.raises(
        ValueError,
        match="passenger_count",
    ):

        build_silver(
            df_pass
        )


# ============================================================
# TEST 6 — COLONNE OBLIGATOIRE MANQUANTE
# ============================================================

def test_silver_blocks_missing_required_column():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_pass = build_valid_pass_data()

    df_pass = df_pass.drop(
        columns=[
            "source_file",
        ]
    )

    # --------------------------------------------------------
    # ACT + ASSERT
    # --------------------------------------------------------

    with pytest.raises(
        ValueError,
        match="Colonnes manquantes",
    ):

        build_silver(
            df_pass
        )