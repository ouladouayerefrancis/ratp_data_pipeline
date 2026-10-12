# tests/test_logging.py
# Test automatisé du système de logging RATP

from pathlib import Path

from config.logging_config import configure_logging


# ============================================================
# TEST — ÉCRITURE D'UN LOG DANS UN FICHIER TEMPORAIRE
# ============================================================

def test_logging_writes_to_file(tmp_path: Path):

    # --------------------------------------------------------
    # ARRANGE
    # --------------------------------------------------------

    logger = configure_logging(
        tmp_path
    )

    test_message = (
        "TEST AUTOMATIQUE LOGGING RATP"
    )


    # --------------------------------------------------------
    # ACT
    # --------------------------------------------------------

    logger.info(
        test_message
    )

    # Force l'écriture immédiate sur disque.
    for handler in logger.handlers:
        handler.flush()


    # --------------------------------------------------------
    # ASSERT
    # --------------------------------------------------------

    log_file = (
        tmp_path
        / "ratp_pipeline.log"
    )

    assert log_file.exists()

    log_content = log_file.read_text(
        encoding="utf-8"
    )

    assert test_message in log_content

    assert "INFO" in log_content

    assert "ratp_pipeline" in log_content


    # --------------------------------------------------------
    # NETTOYAGE DU LOGGER
    # --------------------------------------------------------

    # Important pour éviter que les handlers de ce test
    # restent actifs dans les tests suivants.

    for handler in logger.handlers[:]:

        handler.close()

        logger.removeHandler(
            handler
        )