# tests/test_remediation.py
# Tests automatisés de la RCA et de la remédiation

import pandas as pd

from src.remediation import (
    resolve_conflicts,
    build_remediation_audit,
)


# ============================================================
# FONCTION UTILITAIRE
# ============================================================

def build_test_conflicts() -> pd.DataFrame:
    """
    Construit un conflit synthétique.

    Deux versions existent pour event_id = 4003 :
    - passenger_count = 0
    - passenger_count = 999
    """

    return pd.DataFrame(
        {
            "event_id": [
                4003,
                4003,
            ],

            "event_timestamp": [
                "2026-10-11 06:25:00",
                "2026-10-11 06:25:00",
            ],

            "line": [
                "C",
                "C",
            ],

            "station": [
                "La Défense",
                "La Défense",
            ],

            "event_type": [
                "incident",
                "incident",
            ],

            "passenger_count": [
                0,
                999,
            ],

            "batch_id": [
                "2026-10-11",
                "2026-10-12",
            ],

            "source_file": [
                "ratp_events_20261011.csv",
                "ratp_events_20261012.csv",
            ],

            "ingestion_timestamp": [
                "2026-10-11 07:00:00",
                "2026-10-12 07:00:00",
            ],
        }
    )


# ============================================================
# TEST 1 — RÉSOLUTION D'UN CONFLIT
# ============================================================

def test_resolve_conflict():

    # ARRANGE
    df_conflicts = build_test_conflicts()

    resolutions = {
        4003: {
            "passenger_count": 0,
        }
    }

    # ACT
    (
        df_canonical,
        df_rejected,
        df_unresolved,
    ) = resolve_conflicts(
        df_conflicts=df_conflicts,
        resolutions=resolutions,
    )

    # ASSERT
    assert len(df_canonical) == 1
    assert len(df_rejected) == 1
    assert len(df_unresolved) == 0

    assert (
        df_canonical["passenger_count"].iloc[0]
        == 0
    )

    assert (
        df_rejected["passenger_count"].iloc[0]
        == 999
    )


# ============================================================
# TEST 2 — CONFLIT SANS DÉCISION MÉTIER
# ============================================================

def test_unresolved_conflict():

    # ARRANGE
    df_conflicts = build_test_conflicts()

    resolutions = {}

    # ACT
    (
        df_canonical,
        df_rejected,
        df_unresolved,
    ) = resolve_conflicts(
        df_conflicts=df_conflicts,
        resolutions=resolutions,
    )

    # ASSERT
    assert len(df_canonical) == 0
    assert len(df_rejected) == 0
    assert len(df_unresolved) == 2

    assert set(
        df_unresolved["event_id"]
    ) == {
        4003,
    }


# ============================================================
# TEST 3 — AUDIT D'UNE REMÉDIATION RÉSOLUE
# ============================================================

def test_remediation_audit():

    # ARRANGE
    df_conflicts = build_test_conflicts()

    resolutions = {
        4003: {
            "passenger_count": 0,
        }
    }

    # ACT
    (
        df_canonical,
        df_rejected,
        df_unresolved,
    ) = resolve_conflicts(
        df_conflicts=df_conflicts,
        resolutions=resolutions,
    )

    df_audit = build_remediation_audit(
        df_canonical=df_canonical,
        df_rejected=df_rejected,
        df_unresolved=df_unresolved,
    )

    # ASSERT
    assert len(df_audit) == 2

    assert set(
        df_audit["remediation_role"]
    ) == {
        "CANONICAL",
        "REJECTED",
    }

    assert (
        df_audit["remediation_status"]
        == "RESOLVED"
    ).all()

    canonical_audit = df_audit[
        df_audit["remediation_role"]
        == "CANONICAL"
    ]

    rejected_audit = df_audit[
        df_audit["remediation_role"]
        == "REJECTED"
    ]

    assert (
        canonical_audit["remediation_action"].iloc[0]
        == "PROMOTE_TO_SILVER"
    )

    assert (
        rejected_audit["remediation_action"].iloc[0]
        == "EXCLUDED_FROM_SILVER"
    )


# ============================================================
# TEST 4 — AUDIT D'UN CONFLIT NON RÉSOLU
# ============================================================

def test_unresolved_remediation_audit():

    # ARRANGE
    df_conflicts = build_test_conflicts()

    # ACT
    (
        df_canonical,
        df_rejected,
        df_unresolved,
    ) = resolve_conflicts(
        df_conflicts=df_conflicts,
        resolutions={},
    )

    df_audit = build_remediation_audit(
        df_canonical=df_canonical,
        df_rejected=df_rejected,
        df_unresolved=df_unresolved,
    )

    # ASSERT
    assert len(df_audit) == 2

    assert (
        df_audit["remediation_status"]
        == "UNRESOLVED"
    ).all()

    assert (
        df_audit["remediation_role"]
        == "UNRESOLVED"
    ).all()

    assert (
        df_audit["remediation_action"]
        == "KEEP_IN_QUARANTINE"
    ).all()