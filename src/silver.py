# src/silver.py
# Construction et validation de la couche SILVER

import pandas as pd


# ============================================================
# COLONNES OBLIGATOIRES POUR SILVER
# ============================================================

REQUIRED_COLUMNS = [
    "event_id",
    "event_timestamp",
    "line",
    "station",
    "event_type",
    "passenger_count",
    "batch_id",
    "source_file",
    "ingestion_timestamp",
]


# ============================================================
# CONSTRUCTION DE LA COUCHE SILVER
# ============================================================

def build_silver(
    df_pass: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construit la couche Silver à partir des données
    ayant passé les contrôles Data Quality.

    Contrôles réalisés :
    - colonnes obligatoires ;
    - absence de NULL sur les clés critiques ;
    - unicité de event_id ;
    - validité de event_timestamp ;
    - validité numérique de passenger_count.
    """

    # --------------------------------------------------------
    # COPIE DE TRAVAIL
    # --------------------------------------------------------

    df_silver = df_pass.copy()


    # --------------------------------------------------------
    # 1. CONTRÔLE DES COLONNES OBLIGATOIRES
    # --------------------------------------------------------

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df_silver.columns
    ]

    if missing_columns:

        raise ValueError(
            "Colonnes manquantes pour Silver : "
            f"{missing_columns}"
        )


    # --------------------------------------------------------
    # 2. CONTRÔLE DES VALEURS NULL CRITIQUES
    # --------------------------------------------------------

    critical_columns = [
        "event_id",
        "batch_id",
        "source_file",
    ]

    for column in critical_columns:

        null_count = (
            df_silver[column]
            .isna()
            .sum()
        )

        if null_count > 0:

            raise ValueError(
                f"Silver bloqué : "
                f"{null_count} valeur(s) NULL "
                f"dans {column}."
            )


    # --------------------------------------------------------
    # 3. CONTRÔLE DE L'UNICITÉ DE EVENT_ID
    # --------------------------------------------------------

    duplicate_event_ids = (
        df_silver["event_id"]
        .duplicated()
        .sum()
    )

    if duplicate_event_ids > 0:

        raise ValueError(
            "Silver bloqué : "
            f"{duplicate_event_ids} "
            "event_id dupliqué(s)."
        )


    # --------------------------------------------------------
    # 4. CONVERSION ET VALIDATION DU TIMESTAMP
    # --------------------------------------------------------

    df_silver["event_timestamp"] = pd.to_datetime(
        df_silver["event_timestamp"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )

    invalid_timestamps = (
        df_silver["event_timestamp"]
        .isna()
        .sum()
    )

    if invalid_timestamps > 0:

        raise ValueError(
            "Silver bloqué : "
            f"{invalid_timestamps} "
            "timestamp(s) invalide(s)."
        )


    # --------------------------------------------------------
    # 5. CONVERSION ET VALIDATION DE PASSENGER_COUNT
    # --------------------------------------------------------

    df_silver["passenger_count"] = pd.to_numeric(
        df_silver["passenger_count"],
        errors="coerce",
    )

    invalid_passenger_counts = (
        df_silver["passenger_count"]
        .isna()
        .sum()
    )

    if invalid_passenger_counts > 0:

        raise ValueError(
            "Silver bloqué : "
            f"{invalid_passenger_counts} "
            "passenger_count non numérique(s)."
        )


    # --------------------------------------------------------
    # 6. SÉLECTION DU SCHÉMA SILVER FINAL
    # --------------------------------------------------------

    df_silver = (
        df_silver[
            REQUIRED_COLUMNS
        ]
        .copy()
    )


    # --------------------------------------------------------
    # 7. RETOUR DU DATAFRAME SILVER
    # --------------------------------------------------------

    return df_silver