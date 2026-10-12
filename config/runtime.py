# config/runtime.py
# Paramètres d'exécution du pipeline RATP

import argparse
from pathlib import Path

from config.settings import (
    RAW_FILE,
    BATCH_ID,
)


# ============================================================
# CONSTRUCTION DU PARSER CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    """
    Construit le parser des arguments de ligne de commande.

    Les valeurs de config/settings.py servent de valeurs
    par défaut lorsque l'utilisateur ne fournit aucun argument.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Exécution du pipeline Data RATP "
            "RAW → Bronze → Silver → Gold"
        )
    )

    parser.add_argument(
        "--raw-file",
        type=Path,
        default=RAW_FILE,
        help=(
            "Chemin du fichier RAW à traiter."
        ),
    )

    parser.add_argument(
        "--batch-id",
        type=str,
        default=BATCH_ID,
        help=(
            "Identifiant du batch, par exemple 2026-10-12."
        ),
    )

    return parser


# ============================================================
# LECTURE DES ARGUMENTS
# ============================================================

def parse_runtime_args(
    args=None,
) -> argparse.Namespace:
    """
    Lit les paramètres d'exécution.

    args=None :
        utilise réellement les arguments CMD.

    args=[...] :
        permet de tester la fonction avec pytest.
    """

    parser = build_parser()

    return parser.parse_args(
        args
    )