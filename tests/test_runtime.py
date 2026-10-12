# tests/test_runtime.py
# Tests automatisés des paramètres d'exécution du pipeline RATP

from pathlib import Path

from config.runtime import parse_runtime_args
from config.settings import (
    RAW_FILE,
    BATCH_ID,
)


# ============================================================
# TEST 1 — VALEURS PAR DÉFAUT
# ============================================================

def test_runtime_uses_default_values():

    # --------------------------------------------------------
    # ARRANGE / ACT
    # --------------------------------------------------------

    args = parse_runtime_args(
        []
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert args.raw_file == RAW_FILE

    assert args.batch_id == BATCH_ID


# ============================================================
# TEST 2 — SURCHARGE DES PARAMÈTRES
# ============================================================

def test_runtime_accepts_custom_values():

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    custom_raw_file = (
        Path("data")
        / "raw"
        / "ratp_events_20261013.csv"
    )

    custom_batch_id = "2026-10-13"

    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    args = parse_runtime_args(
        [
            "--raw-file",
            str(custom_raw_file),

            "--batch-id",
            custom_batch_id,
        ]
    )

    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    assert (
        args.raw_file
        == custom_raw_file
    )

    assert (
        args.batch_id
        == custom_batch_id
    )