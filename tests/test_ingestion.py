# tests/test_ingestion.py
# Tests automatisés de l'ingestion RAW → BRONZE

from pathlib import Path

import pandas as pd

from src.ingestion import ingest_bronze_source


# ============================================================
# TEST 1 — INGESTION D'UN NOUVEAU FICHIER
# ============================================================

def test_ingest_new_file(tmp_path: Path):
    """
    Vérifie qu'un nouveau fichier RAW est bien chargé
    dans Bronze avec les métadonnées techniques.
    """

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_raw = pd.DataFrame(
        {
            "event_id": [1, 2],
            "event_timestamp": [
                "2026-10-01 08:00:00",
                "2026-10-01 08:05:00",
            ],
            "line": ["A", "B"],
            "station": ["Nation", "Châtelet"],
            "event_type": [
                "validation",
                "validation",
            ],
            "passenger_count": [100, 150],
        }
    )

    raw_file = (
        tmp_path
        / "ratp_test_20261001.csv"
    )

    bronze_file = (
        tmp_path
        / "bronze_test.csv"
    )

    df_raw.to_csv(
        raw_file,
        index=False,
    )

    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    ingest_bronze_source(
        df_raw=df_raw,
        raw_file=raw_file,
        batch_id="2026-10-01",
        bronze_file=bronze_file,
    )

    df_bronze = pd.read_csv(
        bronze_file
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert len(df_bronze) == 2

    assert "batch_id" in df_bronze.columns
    assert "source_file" in df_bronze.columns
    assert "ingestion_timestamp" in df_bronze.columns

    assert (
        df_bronze["source_file"]
        == "ratp_test_20261001.csv"
    ).all()

    assert (
        df_bronze["batch_id"]
        == "2026-10-01"
    ).all()


# ============================================================
# TEST 2 — IDEMPOTENCE
# ============================================================

def test_ingestion_is_idempotent(tmp_path: Path):
    """
    Vérifie que rejouer le même fichier RAW
    ne crée aucune ligne supplémentaire dans Bronze.
    """

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    df_raw = pd.DataFrame(
        {
            "event_id": [1, 2],
            "event_timestamp": [
                "2026-10-01 08:00:00",
                "2026-10-01 08:05:00",
            ],
            "line": ["A", "B"],
            "station": ["Nation", "Châtelet"],
            "event_type": [
                "validation",
                "validation",
            ],
            "passenger_count": [100, 150],
        }
    )

    raw_file = (
        tmp_path
        / "ratp_test_20261001.csv"
    )

    bronze_file = (
        tmp_path
        / "bronze_test.csv"
    )

    df_raw.to_csv(
        raw_file,
        index=False,
    )

    # --------------------------------------------------------
    # ACT — PREMIÈRE INGESTION
    # --------------------------------------------------------

    ingest_bronze_source(
        df_raw=df_raw,
        raw_file=raw_file,
        batch_id="2026-10-01",
        bronze_file=bronze_file,
    )

    # --------------------------------------------------------
    # ACT — REPLAY DU MÊME FICHIER
    # --------------------------------------------------------

    ingest_bronze_source(
        df_raw=df_raw,
        raw_file=raw_file,
        batch_id="2026-10-01",
        bronze_file=bronze_file,
    )

    df_bronze = pd.read_csv(
        bronze_file
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert len(df_bronze) == 2

    assert (
        df_bronze["event_id"]
        .nunique()
        == 2
    )

    assert (
        df_bronze["source_file"]
        .nunique()
        == 1
    )