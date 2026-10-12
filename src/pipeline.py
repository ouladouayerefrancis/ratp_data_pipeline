# src/pipeline.py
# Pipeline RATP complet, configurable, testable et journalisé
#
# RAW
# → BRONZE
# → DATA QUALITY
# → AUDIT / QUARANTAINE
# → SILVER INITIAL
# → RCA / REMÉDIATION
# → REPLAY
# → SILVER FINAL
# → GOLD
# → QUALITY GATES
# → LOGGING

from pathlib import Path

import logging

import pandas as pd

from config.settings import (
    BRONZE_FILE,
    DQ_AUDIT_DIR,
    QUARANTINE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    LOG_DIR,
)

from config.runtime import parse_runtime_args

from config.logging_config import configure_logging

from src.ingestion import ingest_bronze_source

from src.data_quality import (
    classify_duplicates,
    build_dq_outputs,
)

from src.silver import build_silver

from src.remediation import (
    resolve_conflicts,
    build_remediation_audit,
)

from src.gold import (
    build_gold_daily_line,
    validate_gold,
)

# ============================================================
# FONCTION UTILITAIRE DE LOGGING
# ============================================================

def log_message(
    logger: logging.Logger | None,
    level: str,
    message: str,
) -> None:
    """
    Écrit un message dans le logger uniquement
    lorsqu'un logger a été fourni.

    Les tests pytest peuvent donc appeler run_pipeline()
    sans créer de fichier de logs.
    """

    if logger is None:
        return

    log_function = getattr(
        logger,
        level.lower(),
        logger.info,
    )

    log_function(
        message
    )


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def run_pipeline(
    raw_file: Path,
    bronze_file: Path,
    batch_id: str,
    dq_audit_dir: Path,
    quarantine_dir: Path,
    silver_dir: Path,
    gold_dir: Path,
    resolutions: dict,
    logger: logging.Logger | None = None,
) -> dict:
    """
    Exécute le pipeline RATP complet.

    Les chemins sont fournis en paramètres afin de permettre :
    - l'exécution normale du projet ;
    - l'exécution isolée avec tmp_path dans pytest.

    Le logger est optionnel afin que les tests puissent
    fonctionner sans écrire dans les vrais fichiers de logs.

    Retourne les métriques principales et les chemins
    de sortie pour les tests et la CI/CD.
    """

    # ========================================================
    # 1. NORMALISATION DES CHEMINS
    # ========================================================

    raw_file = Path(raw_file)
    bronze_file = Path(bronze_file)
    dq_audit_dir = Path(dq_audit_dir)
    quarantine_dir = Path(quarantine_dir)
    silver_dir = Path(silver_dir)
    gold_dir = Path(gold_dir)

    log_message(
        logger,
        "info",
        (
            "Pipeline démarré | "
            f"batch_id={batch_id} | "
            f"raw_file={raw_file.name}"
        ),
    )


    # ========================================================
    # 2. CRÉATION DES DOSSIERS
    # ========================================================

    bronze_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    dq_audit_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    quarantine_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    silver_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    gold_dir.mkdir(
        parents=True,
        exist_ok=True,
    )


    # ========================================================
    # 3. CONTRÔLE DU FICHIER RAW
    # ========================================================

    if not raw_file.exists():

        log_message(
            logger,
            "error",
            (
                "Fichier RAW introuvable | "
                f"path={raw_file}"
            ),
        )

        raise FileNotFoundError(
            f"Fichier RAW introuvable : {raw_file}"
        )


    # ========================================================
    # 4. RAW → BRONZE
    # ========================================================

    print("=" * 60)
    print("RATP PIPELINE — RAW → BRONZE")
    print("=" * 60)

    print(
        "Fichier RAW    :",
        raw_file,
    )

    print(
        "Fichier Bronze :",
        bronze_file,
    )

    print(
        "Batch          :",
        batch_id,
    )

    df_raw = pd.read_csv(
        raw_file
    )

    print(
        "\nNombre de lignes RAW :",
        len(df_raw),
    )

    log_message(
        logger,
        "info",
        (
            "RAW chargé | "
            f"rows={len(df_raw)}"
        ),
    )

    ingest_bronze_source(
        df_raw=df_raw,
        raw_file=raw_file,
        batch_id=batch_id,
        bronze_file=bronze_file,
    )


    # ========================================================
    # 5. RELECTURE DU BRONZE
    # ========================================================

    df_bronze = pd.read_csv(
        bronze_file
    )

    print(
        "\nNombre total de lignes Bronze :",
        len(df_bronze),
    )

    print(
        "\nNombre de lignes par source_file :"
    )

    print(
        df_bronze["source_file"]
        .value_counts()
        .sort_index()
    )

    log_message(
        logger,
        "info",
        (
            "Bronze disponible | "
            f"rows={len(df_bronze)}"
        ),
    )


    # ========================================================
    # 6. DATA QUALITY
    # ========================================================

    print("\n" + "=" * 60)
    print("DATA QUALITY — CLASSIFICATION BRONZE")
    print("=" * 60)

    (
        df_pass,
        df_exact_duplicates,
        df_conflicts,
    ) = classify_duplicates(
        df_bronze
    )

    print(
        "PASS                  :",
        len(df_pass),
    )

    print(
        "EXACT_DUPLICATE       :",
        len(df_exact_duplicates),
    )

    print(
        "CONFLICTING_DUPLICATE :",
        len(df_conflicts),
    )

    log_message(
        logger,
        "info",
        (
            "Classification DQ terminée | "
            f"pass={len(df_pass)} | "
            f"exact_duplicates={len(df_exact_duplicates)} | "
            f"conflicts={len(df_conflicts)}"
        ),
    )

    if len(df_exact_duplicates) > 0:

        log_message(
            logger,
            "warning",
            (
                "Doublons exacts détectés | "
                f"rows={len(df_exact_duplicates)}"
            ),
        )

    if len(df_conflicts) > 0:

        log_message(
            logger,
            "warning",
            (
                "Doublons conflictuels détectés | "
                f"rows={len(df_conflicts)}"
            ),
        )


    # ========================================================
    # 7. RÉCONCILIATION BRONZE / DQ
    # ========================================================

    total_explained = (
        len(df_pass)
        + len(df_exact_duplicates)
        + len(df_conflicts)
    )

    bronze_reconciliation_ok = (
        total_explained
        == len(df_bronze)
    )

    print(
        "TOTAL EXPLIQUÉ        :",
        total_explained,
    )

    print(
        "RÉCONCILIATION         :",
        "PASS"
        if bronze_reconciliation_ok
        else "FAIL",
    )

    if not bronze_reconciliation_ok:

        log_message(
            logger,
            "error",
            (
                "Réconciliation Bronze/DQ échouée | "
                f"bronze={len(df_bronze)} | "
                f"explained={total_explained}"
            ),
        )

        raise RuntimeError(
            "PIPELINE BLOCKED : "
            "réconciliation Bronze / DQ incorrecte."
        )

    log_message(
        logger,
        "info",
        "Réconciliation Bronze/DQ=PASS",
    )


    # ========================================================
    # 8. AUDIT DQ / QUARANTAINE
    # ========================================================

    (
        df_dq_audit,
        df_quarantine,
    ) = build_dq_outputs(
        df_exact_duplicates=df_exact_duplicates,
        df_conflicts=df_conflicts,
    )

    dq_audit_file = (
        dq_audit_dir
        / "ratp_dq_audit.csv"
    )

    quarantine_file = (
        quarantine_dir
        / "ratp_quarantine.csv"
    )

    df_dq_audit.to_csv(
        dq_audit_file,
        index=False,
    )

    df_quarantine.to_csv(
        quarantine_file,
        index=False,
    )

    print("\n" + "=" * 60)
    print("DATA QUALITY — AUDIT / QUARANTAINE")
    print("=" * 60)

    print(
        "Anomalies auditées    :",
        len(df_dq_audit),
    )

    print(
        "Lignes en quarantaine :",
        len(df_quarantine),
    )

    print(
        "Fichier audit         :",
        dq_audit_file,
    )

    print(
        "Fichier quarantaine   :",
        quarantine_file,
    )

    log_message(
        logger,
        "info",
        (
            "Sorties DQ écrites | "
            f"audit_rows={len(df_dq_audit)} | "
            f"quarantine_rows={len(df_quarantine)}"
        ),
    )


    # ========================================================
    # 9. SILVER INITIAL
    # ========================================================

    print("\n" + "=" * 60)
    print("CONSTRUCTION DU SILVER INITIAL")
    print("=" * 60)

    df_silver_initial = build_silver(
        df_pass
    )

    silver_reconciliation_ok = (
        len(df_silver_initial)
        == len(df_pass)
    )

    print(
        "Nombre de lignes Silver :",
        len(df_silver_initial),
    )

    print(
        "event_id uniques        :",
        df_silver_initial["event_id"]
        .nunique(),
    )

    print(
        "Doublons event_id       :",
        df_silver_initial["event_id"]
        .duplicated()
        .sum(),
    )

    print(
        "Réconciliation PASS/Silver :",
        "PASS"
        if silver_reconciliation_ok
        else "FAIL",
    )

    if not silver_reconciliation_ok:

        log_message(
            logger,
            "error",
            "Réconciliation PASS/Silver=FAIL",
        )

        raise RuntimeError(
            "PIPELINE BLOCKED : "
            "réconciliation PASS / Silver incorrecte."
        )

    log_message(
        logger,
        "info",
        (
            "Silver initial construit | "
            f"rows={len(df_silver_initial)}"
        ),
    )


    # ========================================================
    # 10. RCA / REMÉDIATION
    # ========================================================

    print("\n" + "=" * 60)
    print("RCA / REMÉDIATION")
    print("=" * 60)

    (
        df_canonical,
        df_rejected_conflicts,
        df_unresolved_conflicts,
    ) = resolve_conflicts(
        df_conflicts=df_conflicts,
        resolutions=resolutions,
    )

    print(
        "Versions canoniques  :",
        len(df_canonical),
    )

    print(
        "Versions rejetées    :",
        len(df_rejected_conflicts),
    )

    print(
        "Conflits non résolus :",
        len(df_unresolved_conflicts),
    )

    log_message(
        logger,
        "info",
        (
            "Remédiation terminée | "
            f"canonical={len(df_canonical)} | "
            f"rejected={len(df_rejected_conflicts)} | "
            f"unresolved={len(df_unresolved_conflicts)}"
        ),
    )

    if len(df_unresolved_conflicts) > 0:

        log_message(
            logger,
            "warning",
            (
                "Conflits non résolus | "
                f"rows={len(df_unresolved_conflicts)}"
            ),
        )


    # ========================================================
    # 11. AUDIT DE REMÉDIATION
    # ========================================================

    df_remediation_audit = build_remediation_audit(
        df_canonical=df_canonical,
        df_rejected=df_rejected_conflicts,
        df_unresolved=df_unresolved_conflicts,
    )

    remediation_audit_file = (
        dq_audit_dir
        / "ratp_remediation_audit.csv"
    )

    df_remediation_audit.to_csv(
        remediation_audit_file,
        index=False,
    )

    print(
        "Lignes audit remédiation :",
        len(df_remediation_audit),
    )

    print(
        "Fichier remédiation       :",
        remediation_audit_file,
    )

    log_message(
        logger,
        "info",
        (
            "Audit remédiation écrit | "
            f"rows={len(df_remediation_audit)}"
        ),
    )


    # ========================================================
    # 12. REPLAY CONTRÔLÉ VERS SILVER
    # ========================================================

    print("\n" + "=" * 60)
    print("REPLAY CONTRÔLÉ VERS SILVER")
    print("=" * 60)

    df_silver_before_validation = pd.concat(
        [
            df_silver_initial,
            df_canonical,
        ],
        ignore_index=True,
    )

    df_silver_final = build_silver(
        df_silver_before_validation
    )

    silver_file = (
        silver_dir
        / "ratp_events_silver.csv"
    )

    df_silver_final.to_csv(
        silver_file,
        index=False,
    )

    print(
        "Silver initial         :",
        len(df_silver_initial),
    )

    print(
        "Silver après replay    :",
        len(df_silver_final),
    )

    print(
        "event_id uniques       :",
        df_silver_final["event_id"]
        .nunique(),
    )

    print(
        "Doublons event_id      :",
        df_silver_final["event_id"]
        .duplicated()
        .sum(),
    )

    log_message(
        logger,
        "info",
        (
            "Replay Silver terminé | "
            f"initial_rows={len(df_silver_initial)} | "
            f"final_rows={len(df_silver_final)}"
        ),
    )


    # ========================================================
    # 13. RÉCONCILIATION FINALE
    # ========================================================

    final_explained = (
        len(df_silver_final)
        + len(df_exact_duplicates)
        + len(df_rejected_conflicts)
        + len(df_unresolved_conflicts)
    )

    final_reconciliation_ok = (
        final_explained
        == len(df_bronze)
    )

    print("\n" + "=" * 60)
    print("RÉCONCILIATION APRÈS REMÉDIATION")
    print("=" * 60)

    print(
        "Bronze                     :",
        len(df_bronze),
    )

    print(
        "Silver final               :",
        len(df_silver_final),
    )

    print(
        "Exact duplicates rejetés   :",
        len(df_exact_duplicates),
    )

    print(
        "Conflits rejetés           :",
        len(df_rejected_conflicts),
    )

    print(
        "Conflits non résolus       :",
        len(df_unresolved_conflicts),
    )

    print(
        "TOTAL EXPLIQUÉ             :",
        final_explained,
    )

    print(
        "Réconciliation finale      :",
        "PASS"
        if final_reconciliation_ok
        else "FAIL",
    )

    if not final_reconciliation_ok:

        log_message(
            logger,
            "error",
            (
                "Réconciliation finale=FAIL | "
                f"bronze={len(df_bronze)} | "
                f"explained={final_explained}"
            ),
        )

    else:

        log_message(
            logger,
            "info",
            "Réconciliation finale=PASS",
        )


    # ========================================================
    # 14. QUALITY GATES AVANT GOLD
    # ========================================================

    silver_gate_passed = (
        bronze_reconciliation_ok
        and silver_reconciliation_ok
        and final_reconciliation_ok
    )

    unresolved_conflicts = len(
        df_unresolved_conflicts
    )

    gold_promotion_gate_passed = (
        silver_gate_passed
        and unresolved_conflicts == 0
    )

    log_message(
        logger,
        "info",
        (
            "Quality Gates | "
            f"silver_gate={silver_gate_passed} | "
            f"gold_promotion_gate={gold_promotion_gate_passed}"
        ),
    )


    # ========================================================
    # 15. CONSTRUCTION GOLD
    # ========================================================

    print("\n" + "=" * 60)
    print("CONSTRUCTION DE LA COUCHE GOLD")
    print("=" * 60)

    df_gold = pd.DataFrame()

    gold_checks = {}

    gold_build_gate_passed = False

    gold_file = (
        gold_dir
        / "ratp_daily_line_metrics.csv"
    )

    if gold_promotion_gate_passed:

        df_gold = build_gold_daily_line(
            df_silver_final
        )

        print(
            "Nombre de lignes Silver :",
            len(df_silver_final),
        )

        print(
            "Nombre de lignes Gold   :",
            len(df_gold),
        )

        print("\nMart Gold :")

        print(
            df_gold.to_string(
                index=False
            )
        )

        log_message(
            logger,
            "info",
            (
                "Gold construit | "
                f"rows={len(df_gold)}"
            ),
        )

    else:

        print(
            "GOLD : BLOCKED"
        )

        print(
            "Le Gold Promotion Gate "
            "n'est pas validé."
        )

        log_message(
            logger,
            "warning",
            "Gold Promotion Gate=BLOCKED",
        )


    # ========================================================
    # 16. CONTRÔLES GOLD
    # ========================================================

    if gold_promotion_gate_passed:

        gold_checks = validate_gold(
            df_gold=df_gold,
            silver_row_count=len(
                df_silver_final
            ),
        )

        print("\nContrôles Gold :")

        for (
            check_name,
            check_result,
        ) in gold_checks.items():

            print(
                f"{check_name:<30} :",
                "PASS"
                if check_result
                else "FAIL",
            )

            log_message(
                logger,
                (
                    "info"
                    if check_result
                    else "error"
                ),
                (
                    "Gold check | "
                    f"{check_name}={check_result}"
                ),
            )

        gold_build_gate_passed = all(
            gold_checks.values()
        )


    # ========================================================
    # 17. ÉCRITURE GOLD
    # ========================================================

    if gold_build_gate_passed:

        df_gold.to_csv(
            gold_file,
            index=False,
        )

        print(
            "\nGOLD : SUCCESS"
        )

        print(
            "Fichier Gold :",
            gold_file,
        )

        log_message(
            logger,
            "info",
            (
                "Gold écrit avec succès | "
                f"path={gold_file}"
            ),
        )

    elif gold_promotion_gate_passed:

        log_message(
            logger,
            "error",
            "Gold Build Gate=BLOCKED",
        )

        raise RuntimeError(
            "GOLD BLOCKED : "
            "au moins un contrôle Gold a échoué."
        )


    # ========================================================
    # 18. RÉCONCILIATION SILVER / GOLD
    # ========================================================

    silver_event_count = len(
        df_silver_final
    )

    if gold_build_gate_passed:

        gold_event_count = int(
            df_gold["total_events"]
            .sum()
        )

        silver_gold_reconciliation_ok = (
            silver_event_count
            == gold_event_count
        )

    else:

        gold_event_count = 0

        silver_gold_reconciliation_ok = False

    log_message(
        logger,
        (
            "info"
            if silver_gold_reconciliation_ok
            else "error"
        ),
        (
            "Réconciliation Silver/Gold | "
            f"silver_events={silver_event_count} | "
            f"gold_events={gold_event_count} | "
            f"status={silver_gold_reconciliation_ok}"
        ),
    )


    # ========================================================
    # 19. VERDICT FINAL
    # ========================================================

    print("\n" + "=" * 60)
    print("RATP DATA PIPELINE — VERDICT FINAL")
    print("=" * 60)

    print(
        "Bronze                 :",
        len(df_bronze),
    )

    print(
        "PASS initial           :",
        len(df_pass),
    )

    print(
        "Exact duplicates       :",
        len(df_exact_duplicates),
    )

    print(
        "Conflicting duplicates :",
        len(df_conflicts),
    )

    print(
        "Audit DQ               :",
        len(df_dq_audit),
    )

    print(
        "Quarantaine            :",
        len(df_quarantine),
    )

    print(
        "Versions canoniques    :",
        len(df_canonical),
    )

    print(
        "Versions rejetées      :",
        len(df_rejected_conflicts),
    )

    print(
        "Audit remédiation      :",
        len(df_remediation_audit),
    )

    print(
        "Silver initial         :",
        len(df_silver_initial),
    )

    print(
        "Silver final           :",
        len(df_silver_final),
    )

    print(
        "Réconciliation Bronze  :",
        "PASS"
        if bronze_reconciliation_ok
        else "FAIL",
    )

    print(
        "Réconciliation finale  :",
        "PASS"
        if final_reconciliation_ok
        else "FAIL",
    )

    print(
        "Silver Build Gate      :",
        "PASS"
        if silver_gate_passed
        else "BLOCKED",
    )

    print(
        "Conflits non résolus   :",
        unresolved_conflicts,
    )

    print(
        "Gold Promotion Gate    :",
        "PASS"
        if gold_promotion_gate_passed
        else "BLOCKED",
    )

    print(
        "Gold rows              :",
        len(df_gold),
    )

    print(
        "Événements Silver      :",
        silver_event_count,
    )

    print(
        "Événements dans Gold   :",
        gold_event_count,
    )

    print(
        "Silver/Gold reconcile  :",
        "PASS"
        if silver_gold_reconciliation_ok
        else "FAIL",
    )

    print(
        "Gold Build Gate        :",
        "PASS"
        if gold_build_gate_passed
        else "BLOCKED",
    )

    print("=" * 60)

    log_message(
        logger,
        "info",
        (
            "Verdict final | "
            f"bronze={len(df_bronze)} | "
            f"silver={len(df_silver_final)} | "
            f"gold_rows={len(df_gold)} | "
            f"silver_gate={silver_gate_passed} | "
            f"gold_gate={gold_build_gate_passed}"
        ),
    )


    # ========================================================
    # 20. RETOUR STRUCTURÉ POUR TESTS / CI
    # ========================================================

    return {
        "bronze_rows": len(df_bronze),

        "pass_rows": len(df_pass),

        "exact_duplicate_rows": len(
            df_exact_duplicates
        ),

        "conflict_rows": len(
            df_conflicts
        ),

        "dq_audit_rows": len(
            df_dq_audit
        ),

        "quarantine_rows": len(
            df_quarantine
        ),

        "canonical_rows": len(
            df_canonical
        ),

        "rejected_conflict_rows": len(
            df_rejected_conflicts
        ),

        "unresolved_conflict_rows": len(
            df_unresolved_conflicts
        ),

        "remediation_audit_rows": len(
            df_remediation_audit
        ),

        "silver_initial_rows": len(
            df_silver_initial
        ),

        "silver_final_rows": len(
            df_silver_final
        ),

        "gold_rows": len(
            df_gold
        ),

        "gold_event_count": gold_event_count,

        "bronze_reconciliation_ok": bool(
            bronze_reconciliation_ok
        ),

        "final_reconciliation_ok": bool(
            final_reconciliation_ok
        ),

        "silver_gate_passed": bool(
            silver_gate_passed
        ),

        "gold_promotion_gate_passed": bool(
            gold_promotion_gate_passed
        ),

        "silver_gold_reconciliation_ok": bool(
            silver_gold_reconciliation_ok
        ),

        "gold_build_gate_passed": bool(
            gold_build_gate_passed
        ),

        "dq_audit_file": dq_audit_file,

        "quarantine_file": quarantine_file,

        "remediation_audit_file": remediation_audit_file,

        "silver_file": silver_file,

        "gold_file": gold_file,
    }


# ============================================================
# EXÉCUTION NORMALE / CLI
# ============================================================

def main():
    """
    Point d'entrée du pipeline RATP.

    Sans argument :
        utilise les valeurs par défaut définies
        dans config/runtime.py.

    Avec arguments :
        --raw-file
        --batch-id

    les valeurs fournies depuis CMD remplacent
    les valeurs par défaut.
    """

    # ========================================================
    # 1. PARAMÈTRES D'EXÉCUTION
    # ========================================================

    runtime_args = parse_runtime_args()

    runtime_raw_file = runtime_args.raw_file
    runtime_batch_id = runtime_args.batch_id


    # ========================================================
    # 2. CONFIGURATION DU LOGGER
    # ========================================================

    logger = configure_logging(
        LOG_DIR
    )

    logger.info(
        "============================================================"
    )

    logger.info(
        (
            "DÉMARRAGE RATP DATA PIPELINE | "
            "batch_id=%s | "
            "raw_file=%s"
        ),
        runtime_batch_id,
        runtime_raw_file,
    )


    # ========================================================
    # 3. RÉSOLUTION MÉTIER SIMULÉE
    # ========================================================

    resolutions = {
        4003: {
            "passenger_count": 0,
        }
    }


    # ========================================================
    # 4. EXÉCUTION DU PIPELINE
    # ========================================================

    try:

        results = run_pipeline(
            raw_file=runtime_raw_file,
            bronze_file=BRONZE_FILE,
            batch_id=runtime_batch_id,
            dq_audit_dir=DQ_AUDIT_DIR,
            quarantine_dir=QUARANTINE_DIR,
            silver_dir=SILVER_DIR,
            gold_dir=GOLD_DIR,
            resolutions=resolutions,
            logger=logger,
        )

        logger.info(
            (
                "PIPELINE TERMINÉ AVEC SUCCÈS | "
                "batch_id=%s | "
                "bronze_rows=%s | "
                "silver_rows=%s | "
                "gold_rows=%s"
            ),
            runtime_batch_id,
            results["bronze_rows"],
            results["silver_final_rows"],
            results["gold_rows"],
        )

    except Exception:

        logger.exception(
            (
                "ÉCHEC DU PIPELINE RATP | "
                "batch_id=%s | "
                "raw_file=%s"
            ),
            runtime_batch_id,
            runtime_raw_file,
        )

        raise


# ============================================================
# POINT D'ENTRÉE
# ============================================================

if __name__ == "__main__":
    main()