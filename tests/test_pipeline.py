# tests/test_pipeline.py
# Test d'intégration complet du pipeline RATP
#
# Ce test vérifie :
# RAW
# → BRONZE
# → DATA QUALITY
# → AUDIT / QUARANTAINE
# → REMÉDIATION
# → SILVER
# → GOLD
#
# Toutes les données sont synthétiques et stockées
# dans tmp_path afin de ne jamais toucher
# aux fichiers réels du projet.

from pathlib import Path

import pandas as pd

from src.pipeline import run_pipeline


# ============================================================
# TEST D'INTÉGRATION COMPLET
# ============================================================

def test_full_ratp_pipeline(tmp_path: Path):

    # ========================================================
    # ARRANGE
    # ========================================================

    # --------------------------------------------------------
    # 1. CRÉATION DES DOSSIERS TEMPORAIRES
    # --------------------------------------------------------

    raw_dir = (
        tmp_path
        / "data"
        / "raw"
    )

    bronze_dir = (
        tmp_path
        / "data"
        / "bronze"
    )

    dq_audit_dir = (
        tmp_path
        / "data"
        / "dq_audit"
    )

    quarantine_dir = (
        tmp_path
        / "data"
        / "quarantine"
    )

    silver_dir = (
        tmp_path
        / "data"
        / "silver"
    )

    gold_dir = (
        tmp_path
        / "data"
        / "gold"
    )

    raw_dir.mkdir(
        parents=True,
        exist_ok=True,
    )


    # --------------------------------------------------------
    # 2. FICHIERS TEMPORAIRES
    # --------------------------------------------------------

    raw_file = (
        raw_dir
        / "ratp_integration_test.csv"
    )

    bronze_file = (
        bronze_dir
        / "ratp_bronze_test.csv"
    )


    # --------------------------------------------------------
    # 3. DATASET RAW SYNTHÉTIQUE
    # --------------------------------------------------------

    # Nous créons volontairement :
    #
    # event_id 1001
    # → ligne normale
    #
    # event_id 1002
    # → deux versions strictement identiques
    # → EXACT_DUPLICATE
    #
    # event_id 4003
    # → passenger_count = 0
    # → passenger_count = 999
    # → CONFLICTING_DUPLICATE

    df_raw = pd.DataFrame(
        {
            "event_id": [
                1001,
                1002,
                1002,
                4003,
                4003,
            ],

            "event_timestamp": [
                "2026-10-01 08:00:00",
                "2026-10-01 09:00:00",
                "2026-10-01 09:00:00",
                "2026-10-02 06:25:00",
                "2026-10-02 06:25:00",
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
        }
    )

    df_raw.to_csv(
        raw_file,
        index=False,
    )


    # --------------------------------------------------------
    # 4. DÉCISION MÉTIER SYNTHÉTIQUE
    # --------------------------------------------------------

    # Pour event_id 4003 :
    #
    # passenger_count = 0
    #
    # est considéré comme la version canonique.
    #
    # C'est une décision métier simulée uniquement
    # pour notre test.

    resolutions = {
        4003: {
            "passenger_count": 0,
        }
    }


    # ========================================================
    # ACT — PREMIÈRE EXÉCUTION
    # ========================================================

    results = run_pipeline(
        raw_file=raw_file,
        bronze_file=bronze_file,
        batch_id="2026-10-02",
        dq_audit_dir=dq_audit_dir,
        quarantine_dir=quarantine_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        resolutions=resolutions,
    )


    # ========================================================
    # ASSERT — VOLUMÉTRIE BRONZE / DQ
    # ========================================================

    assert results["bronze_rows"] == 5

    assert results["pass_rows"] == 2

    assert (
        results["exact_duplicate_rows"]
        == 1
    )

    assert (
        results["conflict_rows"]
        == 2
    )

    assert (
        results["dq_audit_rows"]
        == 3
    )

    assert (
        results["quarantine_rows"]
        == 2
    )


    # ========================================================
    # ASSERT — REMÉDIATION
    # ========================================================

    assert (
        results["canonical_rows"]
        == 1
    )

    assert (
        results["rejected_conflict_rows"]
        == 1
    )

    assert (
        results["unresolved_conflict_rows"]
        == 0
    )

    assert (
        results["remediation_audit_rows"]
        == 2
    )


    # ========================================================
    # ASSERT — SILVER
    # ========================================================

    # PASS initial :
    #
    # 1001
    # 1002
    #
    # = 2 lignes
    #
    # Après remédiation :
    #
    # + 4003 canonique
    #
    # = 3 lignes Silver finales

    assert (
        results["silver_initial_rows"]
        == 2
    )

    assert (
        results["silver_final_rows"]
        == 3
    )


    # ========================================================
    # ASSERT — GOLD
    # ========================================================

    # Grain Gold :
    #
    # 2026-10-01 + A
    # 2026-10-01 + B
    # 2026-10-02 + C
    #
    # = 3 lignes Gold

    assert (
        results["gold_rows"]
        == 3
    )

    assert (
        results["gold_event_count"]
        == 3
    )


    # ========================================================
    # ASSERT — QUALITY GATES
    # ========================================================

    assert (
        results["bronze_reconciliation_ok"]
        is True
    )

    assert (
        results["final_reconciliation_ok"]
        is True
    )

    assert (
        results["silver_gate_passed"]
        is True
    )

    assert (
        results["gold_promotion_gate_passed"]
        is True
    )

    assert (
        results["silver_gold_reconciliation_ok"]
        is True
    )

    assert (
        results["gold_build_gate_passed"]
        is True
    )


    # ========================================================
    # ASSERT — FICHIERS PRODUITS
    # ========================================================

    assert (
        results["dq_audit_file"]
        .exists()
    )

    assert (
        results["quarantine_file"]
        .exists()
    )

    assert (
        results["remediation_audit_file"]
        .exists()
    )

    assert (
        results["silver_file"]
        .exists()
    )

    assert (
        results["gold_file"]
        .exists()
    )


    # ========================================================
    # ASSERT — AUCUN FICHIER HORS DE TMP_PATH
    # ========================================================

    assert (
        results["dq_audit_file"]
        .is_relative_to(tmp_path)
    )

    assert (
        results["quarantine_file"]
        .is_relative_to(tmp_path)
    )

    assert (
        results["remediation_audit_file"]
        .is_relative_to(tmp_path)
    )

    assert (
        results["silver_file"]
        .is_relative_to(tmp_path)
    )

    assert (
        results["gold_file"]
        .is_relative_to(tmp_path)
    )


    # ========================================================
    # ASSERT — CONTENU SILVER
    # ========================================================

    df_silver = pd.read_csv(
        results["silver_file"]
    )

    assert len(df_silver) == 3

    assert set(
        df_silver["event_id"]
    ) == {
        1001,
        1002,
        4003,
    }

    canonical_4003 = df_silver[
        df_silver["event_id"]
        == 4003
    ]

    assert (
        canonical_4003["passenger_count"]
        .iloc[0]
        == 0
    )


    # ========================================================
    # ASSERT — CONTENU GOLD
    # ========================================================

    df_gold = pd.read_csv(
        results["gold_file"]
    )

    assert len(df_gold) == 3

    assert (
        df_gold["total_events"]
        .sum()
        == 3
    )


    # ========================================================
    # ACT — DEUXIÈME EXÉCUTION
    # TEST D'IDEMPOTENCE DU PIPELINE COMPLET
    # ========================================================

    results_replay = run_pipeline(
        raw_file=raw_file,
        bronze_file=bronze_file,
        batch_id="2026-10-02",
        dq_audit_dir=dq_audit_dir,
        quarantine_dir=quarantine_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        resolutions=resolutions,
    )


    # ========================================================
    # ASSERT — IDEMPOTENCE
    # ========================================================

    # Le même fichier RAW est rejoué.
    #
    # Bronze doit rester à 5 lignes,
    # et non passer à 10.

    assert (
        results_replay["bronze_rows"]
        == 5
    )

    assert (
        results_replay["silver_final_rows"]
        == 3
    )

    assert (
        results_replay["gold_event_count"]
        == 3
    )

    assert (
        results_replay["gold_build_gate_passed"]
        is True
    )