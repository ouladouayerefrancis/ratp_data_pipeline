# src/remediation.py
# Gestion des RCA, remédiations et traces de résolution

import pandas as pd


# ============================================================
# 1. RÉSOLUTION DES CONFLITS
# ============================================================

def resolve_conflicts(
    df_conflicts: pd.DataFrame,
    resolutions: dict,
):
    """
    Applique des décisions métier explicites
    aux conflits Data Quality.

    Exemple :

    resolutions = {
        4003: {
            "passenger_count": 0
        }
    }

    Retourne :
    - les versions canoniques ;
    - les versions rejetées ;
    - les conflits encore non résolus.
    """

    canonical_rows = []
    rejected_rows = []
    unresolved_rows = []

    for event_id, group in df_conflicts.groupby("event_id"):

        # Aucun arbitrage métier disponible
        if event_id not in resolutions:

            unresolved_rows.append(
                group.copy()
            )

            continue

        resolution = resolutions[event_id]

        candidate_mask = pd.Series(
            True,
            index=group.index,
        )

        # Application de la décision métier
        for column, expected_value in resolution.items():

            if column not in group.columns:

                raise ValueError(
                    f"Colonne de résolution inconnue : "
                    f"{column}"
                )

            candidate_mask &= (
                group[column]
                == expected_value
            )

        candidates = (
            group[
                candidate_mask
            ]
            .copy()
        )

        # La résolution doit identifier
        # exactement une version canonique.
        if len(candidates) != 1:

            unresolved_rows.append(
                group.copy()
            )

            continue

        canonical = candidates.copy()

        rejected = (
            group[
                ~group.index.isin(
                    canonical.index
                )
            ]
            .copy()
        )

        canonical_rows.append(
            canonical
        )

        rejected_rows.append(
            rejected
        )

    # ========================================================
    # CONSTRUCTION DES SORTIES
    # ========================================================

    if canonical_rows:

        df_canonical = pd.concat(
            canonical_rows,
            ignore_index=True,
        )

    else:

        df_canonical = pd.DataFrame(
            columns=df_conflicts.columns
        )

    if rejected_rows:

        df_rejected = pd.concat(
            rejected_rows,
            ignore_index=True,
        )

    else:

        df_rejected = pd.DataFrame(
            columns=df_conflicts.columns
        )

    if unresolved_rows:

        df_unresolved = pd.concat(
            unresolved_rows,
            ignore_index=True,
        )

    else:

        df_unresolved = pd.DataFrame(
            columns=df_conflicts.columns
        )

    return (
        df_canonical,
        df_rejected,
        df_unresolved,
    )


# ============================================================
# 2. AUDIT DE REMÉDIATION
# ============================================================

def build_remediation_audit(
    df_canonical: pd.DataFrame,
    df_rejected: pd.DataFrame,
    df_unresolved: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construit la trace du cycle de remédiation DQ002.

    Une anomalie détectée ne disparaît pas :
    sa résolution est également auditée.
    """

    remediation_frames = []

    remediation_timestamp = pd.Timestamp.now()

    # ========================================================
    # VERSION CANONIQUE
    # ========================================================

    if not df_canonical.empty:

        canonical_audit = (
            df_canonical.copy()
        )

        canonical_audit["dq_rule_id"] = "DQ002"

        canonical_audit[
            "remediation_status"
        ] = "RESOLVED"

        canonical_audit[
            "remediation_role"
        ] = "CANONICAL"

        canonical_audit[
            "remediation_action"
        ] = "PROMOTE_TO_SILVER"

        canonical_audit[
            "resolution_basis"
        ] = "SIMULATED_BUSINESS_CONFIRMATION"

        canonical_audit[
            "remediated_at"
        ] = remediation_timestamp

        remediation_frames.append(
            canonical_audit
        )

    # ========================================================
    # VERSION REJETÉE
    # ========================================================

    if not df_rejected.empty:

        rejected_audit = (
            df_rejected.copy()
        )

        rejected_audit["dq_rule_id"] = "DQ002"

        rejected_audit[
            "remediation_status"
        ] = "RESOLVED"

        rejected_audit[
            "remediation_role"
        ] = "REJECTED"

        rejected_audit[
            "remediation_action"
        ] = "EXCLUDED_FROM_SILVER"

        rejected_audit[
            "resolution_basis"
        ] = "SIMULATED_BUSINESS_CONFIRMATION"

        rejected_audit[
            "remediated_at"
        ] = remediation_timestamp

        remediation_frames.append(
            rejected_audit
        )

    # ========================================================
    # CONFLITS NON RÉSOLUS
    # ========================================================

    if not df_unresolved.empty:

        unresolved_audit = (
            df_unresolved.copy()
        )

        unresolved_audit["dq_rule_id"] = "DQ002"

        unresolved_audit[
            "remediation_status"
        ] = "UNRESOLVED"

        unresolved_audit[
            "remediation_role"
        ] = "UNRESOLVED"

        unresolved_audit[
            "remediation_action"
        ] = "KEEP_IN_QUARANTINE"

        unresolved_audit[
            "resolution_basis"
        ] = None

        unresolved_audit[
            "remediated_at"
        ] = remediation_timestamp

        remediation_frames.append(
            unresolved_audit
        )

    # ========================================================
    # AUDIT FINAL
    # ========================================================

    if remediation_frames:

        return pd.concat(
            remediation_frames,
            ignore_index=True,
        )

    return pd.DataFrame()