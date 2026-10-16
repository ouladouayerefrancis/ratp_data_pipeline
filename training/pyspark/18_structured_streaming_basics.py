# Bloc 18A — Spark Structured Streaming : fondamentaux
#
# Objectif :
# comprendre :
# - readStream
# - writeStream
# - micro-batches
# - trigger
# - checkpoint
# - StreamingQuery

import os
import sys
from pathlib import Path


# -------------------------------------------------------
# 0. Configuration Python locale Windows
# -------------------------------------------------------

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when


# -------------------------------------------------------
# 1. Création de la SparkSession
# -------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RATP-Structured-Streaming-Basics")
    .master("local[*]")
    .config(
        "spark.pyspark.python",
        sys.executable
    )
    .config(
        "spark.pyspark.driver.python",
        sys.executable
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# -------------------------------------------------------
# 2. Chemins
# -------------------------------------------------------

base_dir = Path(__file__).resolve().parent

checkpoint_dir = (
    base_dir
    / "output"
    / "checkpoints"
    / "18_structured_streaming"
)


query = None


try:

    # ===================================================
    # 3. SOURCE STREAMING
    # ===================================================

    print("\n=== 1. SOURCE STREAMING ===")

    # "rate" est une source intégrée à Spark.
    #
    # Elle produit continuellement :
    #
    # timestamp | value
    #
    # rowsPerSecond = 3
    # signifie environ 3 nouvelles lignes par seconde.

    raw_stream_df = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 3)
        .load()
    )

    print(
        "raw_stream_df.isStreaming :",
        raw_stream_df.isStreaming
    )

    raw_stream_df.printSchema()


    # ===================================================
    # 4. TRANSFORMATIONS
    # ===================================================

    print("\n=== 2. TRANSFORMATIONS STREAMING ===")

    events_stream_df = (
        raw_stream_df
        .select(
            col("timestamp").alias(
                "event_timestamp"
            ),
            col("value").alias(
                "event_id"
            ),
        )
        .withColumn(
            "line",
            when(
                col("event_id") % 2 == 0,
                lit("A")
            ).otherwise(
                lit("B")
            )
        )
        .withColumn(
            "passenger_count",
            (
                col("event_id") * 10 + 100
            ).cast("integer")
        )
    )

    print(
        "events_stream_df.isStreaming :",
        events_stream_df.isStreaming
    )

    events_stream_df.printSchema()


    # ===================================================
    # 5. DÉMARRAGE DE LA QUERY
    # ===================================================

    print("\n=== 3. DÉMARRAGE DE LA STREAMING QUERY ===")

    query = (
        events_stream_df
        .writeStream

        # Sink pédagogique :
        # afficher les données dans le terminal.
        .format("console")

        # Écrire uniquement les nouvelles lignes.
        .outputMode("append")

        .option(
            "truncate",
            "false"
        )

        # Spark mémorise ici l'état de progression.
        .option(
            "checkpointLocation",
            str(checkpoint_dir)
        )

        # Un micro-batch environ toutes les 2 secondes.
        .trigger(
            processingTime="2 seconds"
        )

        .queryName(
            "ratp_training_stream"
        )

        .start()
    )

    print(
        "Query active :",
        query.isActive
    )


    # ===================================================
    # 6. LAISSER LE STREAM TOURNER
    # ===================================================

    print(
        "\n=== 4. STREAM ACTIF PENDANT 10 SECONDES ==="
    )

    query.awaitTermination(10)


    # ===================================================
    # 7. ÉTAT DE LA QUERY
    # ===================================================

    print("\n=== 5. ÉTAT DE LA QUERY ===")

    print(
        "Nom :",
        query.name
    )

    print(
        "Active :",
        query.isActive
    )

    print(
        "Status :",
        query.status
    )


finally:

    # ===================================================
    # 8. ARRÊT PROPRE
    # ===================================================

    if query is not None and query.isActive:

        query.stop()

        print(
            "\nStreaming Query arrêtée proprement."
        )

    spark.stop()

    print(
        "SparkSession arrêtée proprement."
    )