# src/gold.py
# Construction et validation de la couche GOLD

import pandas as pd


# ============================================================
# COLONNES SILVER NÉCESSAIRES À GOLD
# ============================================================

REQUIRED_SILVER_COLUMNS = [
    "event_id",
    "event_timestamp",
    "line",
    "station",
    "event_type",
    "passenger_count",
]


# ============================================================
# 1. CONSTRUCTION DU MART GOLD QUOTIDIEN PAR LIGNE
# ============================================================

def build_gold_daily_line(
    df_silver: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construit un mart Gold quotidien par ligne RATP.

    Grain :
        1 ligne Gold
        =
        1 date
        +
        1 ligne de transport

    KPI :
    - total_events
    - total_passengers
    - validation_count
    - incident_count
    - station_count
    - incident_rate
    """

    # --------------------------------------------------------
    # COPIE DE TRAVAIL
    # --------------------------------------------------------

    df = df_silver.copy()


    # --------------------------------------------------------
    # 1. CONTRÔLE DES COLONNES OBLIGATOIRES
    # --------------------------------------------------------

    missing_columns = [
        column
        for column in REQUIRED_SILVER_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:

        raise ValueError(
            "Impossible de construire Gold. "
            "Colonnes Silver manquantes : "
            f"{missing_columns}"
        )


    # --------------------------------------------------------
    # 2. CONVERSION / VALIDATION DU TIMESTAMP
    # --------------------------------------------------------

    df["event_timestamp"] = pd.to_datetime(
        df["event_timestamp"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )

    invalid_timestamps = (
        df["event_timestamp"]
        .isna()
        .sum()
    )

    if invalid_timestamps > 0:

        raise ValueError(
            "Gold bloqué : "
            f"{invalid_timestamps} "
            "timestamp(s) invalide(s)."
        )


    # --------------------------------------------------------
    # 3. DATE MÉTIER
    # --------------------------------------------------------

    df["event_date"] = (
        df["event_timestamp"]
        .dt.date
    )


    # --------------------------------------------------------
    # 4. INDICATEUR VALIDATION
    # --------------------------------------------------------

    df["is_validation"] = (
        df["event_type"]
        .eq("validation")
        .astype(int)
    )


    # --------------------------------------------------------
    # 5. INDICATEUR INCIDENT
    # --------------------------------------------------------

    df["is_incident"] = (
        df["event_type"]
        .eq("incident")
        .astype(int)
    )


    # --------------------------------------------------------
    # 6. AGRÉGATION GOLD
    # --------------------------------------------------------

    df_gold = (
        df
        .groupby(
            [
                "event_date",
                "line",
            ],
            as_index=False,
        )
        .agg(
            total_events=(
                "event_id",
                "count",
            ),

            total_passengers=(
                "passenger_count",
                "sum",
            ),

            validation_count=(
                "is_validation",
                "sum",
            ),

            incident_count=(
                "is_incident",
                "sum",
            ),

            station_count=(
                "station",
                "nunique",
            ),
        )
    )


    # --------------------------------------------------------
    # 7. TAUX D'INCIDENT
    # --------------------------------------------------------

    df_gold["incident_rate"] = (
        df_gold["incident_count"]
        / df_gold["total_events"]
    )


    # --------------------------------------------------------
    # 8. TRI DU MART
    # --------------------------------------------------------

    df_gold = (
        df_gold
        .sort_values(
            [
                "event_date",
                "line",
            ]
        )
        .reset_index(
            drop=True
        )
    )


    # --------------------------------------------------------
    # 9. RETOUR
    # --------------------------------------------------------

    return df_gold


# ============================================================
# 2. VALIDATION DATA QUALITY GOLD
# ============================================================

def validate_gold(
    df_gold: pd.DataFrame,
    silver_row_count: int,
) -> dict:
    """
    Vérifie les règles Data Quality de Gold.

    Le dictionnaire retourné contient uniquement
    des booléens Python True / False.
    """


    # --------------------------------------------------------
    # 1. GRAIN GOLD UNIQUE
    # --------------------------------------------------------

    unique_gold_grain = (
        df_gold
        .duplicated(
            subset=[
                "event_date",
                "line",
            ]
        )
        .sum()
        == 0
    )


    # --------------------------------------------------------
    # 2. CLÉS GOLD NON NULLES
    # --------------------------------------------------------

    no_null_gold_keys = (
        df_gold[
            [
                "event_date",
                "line",
            ]
        ]
        .isna()
        .sum()
        .sum()
        == 0
    )


    # --------------------------------------------------------
    # 3. TOTAL_EVENTS >= 0
    # --------------------------------------------------------

    non_negative_events = (
        (
            df_gold["total_events"]
            >= 0
        )
        .all()
    )


    # --------------------------------------------------------
    # 4. TOTAL_PASSENGERS >= 0
    # --------------------------------------------------------

    non_negative_passengers = (
        (
            df_gold["total_passengers"]
            >= 0
        )
        .all()
    )


    # --------------------------------------------------------
    # 5. INCIDENT_COUNT >= 0
    # --------------------------------------------------------

    non_negative_incidents = (
        (
            df_gold["incident_count"]
            >= 0
        )
        .all()
    )


    # --------------------------------------------------------
    # 6. INCIDENT_RATE ENTRE 0 ET 1
    # --------------------------------------------------------

    valid_incident_rate = (
        df_gold["incident_rate"]
        .between(
            0,
            1,
            inclusive="both",
        )
        .all()
    )


    # --------------------------------------------------------
    # 7. RÉCONCILIATION SILVER → GOLD
    # --------------------------------------------------------

    gold_event_count = (
        df_gold["total_events"]
        .sum()
    )

    silver_gold_reconciliation = (
        gold_event_count
        == silver_row_count
    )


    # --------------------------------------------------------
    # 8. NORMALISATION EN BOOLÉENS PYTHON
    # --------------------------------------------------------

    checks = {
        "unique_gold_grain": bool(
            unique_gold_grain
        ),

        "no_null_gold_keys": bool(
            no_null_gold_keys
        ),

        "non_negative_events": bool(
            non_negative_events
        ),

        "non_negative_passengers": bool(
            non_negative_passengers
        ),

        "non_negative_incidents": bool(
            non_negative_incidents
        ),

        "valid_incident_rate": bool(
            valid_incident_rate
        ),

        "silver_gold_reconciliation": bool(
            silver_gold_reconciliation
        ),
    }


    # --------------------------------------------------------
    # 9. RETOUR DES CONTRÔLES
    # --------------------------------------------------------

    return checks