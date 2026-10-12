# config/logging_config.py
# Configuration centralisée du logging du pipeline RATP

import logging
from pathlib import Path


# ============================================================
# CONFIGURATION DU LOGGER
# ============================================================

def configure_logging(
    log_dir: Path,
    log_level: str = "INFO",
) -> logging.Logger:
    """
    Configure le logger principal du pipeline RATP.

    Les logs sont envoyés :
    - dans le terminal ;
    - dans un fichier ratp_pipeline.log.

    Retourne le logger configuré.
    """

    # --------------------------------------------------------
    # 1. NORMALISATION DU CHEMIN
    # --------------------------------------------------------

    log_dir = Path(log_dir)

    log_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    log_file = (
        log_dir
        / "ratp_pipeline.log"
    )


    # --------------------------------------------------------
    # 2. CRÉATION DU LOGGER
    # --------------------------------------------------------

    logger = logging.getLogger(
        "ratp_pipeline"
    )

    logger.setLevel(
        getattr(
            logging,
            log_level.upper(),
            logging.INFO,
        )
    )


    # --------------------------------------------------------
    # 3. ÉVITER LES HANDLERS EN DOUBLE
    # --------------------------------------------------------

    if logger.handlers:

        return logger


    # --------------------------------------------------------
    # 4. FORMAT DES LOGS
    # --------------------------------------------------------

    formatter = logging.Formatter(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    )


    # --------------------------------------------------------
    # 5. LOGS TERMINAL
    # --------------------------------------------------------

    console_handler = logging.StreamHandler()

    console_handler.setFormatter(
        formatter
    )

    console_handler.setLevel(
        logger.level
    )


    # --------------------------------------------------------
    # 6. LOGS FICHIER
    # --------------------------------------------------------

    file_handler = logging.FileHandler(
        log_file,
        encoding="utf-8",
    )

    file_handler.setFormatter(
        formatter
    )

    file_handler.setLevel(
        logger.level
    )


    # --------------------------------------------------------
    # 7. AJOUT DES HANDLERS
    # --------------------------------------------------------

    logger.addHandler(
        console_handler
    )

    logger.addHandler(
        file_handler
    )


    # --------------------------------------------------------
    # 8. ÉVITER LA PROPAGATION VERS LE ROOT LOGGER
    # --------------------------------------------------------

    logger.propagate = False


    # --------------------------------------------------------
    # 9. RETOUR
    # --------------------------------------------------------

    return logger