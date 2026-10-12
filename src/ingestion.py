# src/ingestion.py
# Gestion de l'ingestion RAW → BRONZE

from pathlib import Path

import pandas as pd


def ingest_bronze_source(
    df_raw: pd.DataFrame,
    raw_file: Path,
    batch_id: str,
    bronze_file: Path,
) -> pd.DataFrame:
    """
    Ingère un fichier source dans la couche Bronze.

    Principes :
    - conserver fidèlement toutes les lignes reçues ;
    - ajouter les métadonnées techniques ;
    - empêcher la réingestion du même fichier source ;
    - ne pas appliquer de règles Data Quality métier ici.
    """

    # Copie de sécurité :
    # on ne modifie jamais directement le DataFrame RAW reçu.
    df_batch = df_raw.copy()

    # Métadonnées techniques
    df_batch["batch_id"] = batch_id
    df_batch["source_file"] = raw_file.name
    df_batch["ingestion_timestamp"] = pd.Timestamp.now()

    # S'assurer que le dossier Bronze existe.
    bronze_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # ========================================================
    # CONTRÔLE D'IDEMPOTENCE
    # ========================================================

    if bronze_file.exists():

        df_existing = pd.read_csv(
            bronze_file
        )

        if "source_file" not in df_existing.columns:
            raise ValueError(
                "Le fichier Bronze existe mais ne contient "
                "pas la colonne technique 'source_file'."
            )

        already_processed = (
            raw_file.name
            in df_existing["source_file"].values
        )

    else:

        already_processed = False

    # Le même fichier ne doit pas être chargé deux fois.
    if already_processed:

        print(
            f"SKIP : {raw_file.name} "
            "a déjà été ingéré en Bronze."
        )

        return df_batch

    # ========================================================
    # ÉCRITURE BRONZE
    # ========================================================

    # Bronze conserve toutes les lignes reçues.
    # Aucun drop_duplicates() ici.
    df_batch.to_csv(
        bronze_file,
        mode="a",
        header=not bronze_file.exists(),
        index=False,
    )

    print(
        f"SUCCESS : {len(df_batch)} lignes "
        "conservées en Bronze."
    )

    return df_batch