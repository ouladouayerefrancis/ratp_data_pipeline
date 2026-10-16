# Bloc 18E — Structured Streaming
# Déduplication avec Watermark
#
# Objectifs :
# - simuler des doublons métier
# - dédupliquer un flux en temps réel
# - utiliser dropDuplicatesWithinWatermark()
# - observer l'état de déduplication
# - relier Streaming et Data Quality

import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    floor,
    lit,
    when,
)


# -------------------------------------------------------
# 1. SparkSession
# -------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RATP-Streaming-Dedup-Watermark")
    .master("local[*]")
    .config(
        "spark.pyspark.python",
        sys.executable,
    )
    .config(
        "spark.pyspark.driver.python",
        sys.executable,
    )
    .config(
        "spark.sql.shuffle.partitions",
        "4",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# -------------------------------------------------------
# 2. Checkpoint
# -------------------------------------------------------

checkpoint_dir = (
    Path.home()
    / "ratp_spark_checkpoints"
    / "18e_streaming_dedup_watermark"
)

checkpoint_dir.mkdir(
    parents=True,
    exist_ok=True,
)


query = None


try:

    # ===================================================
    # 3. SOURCE STREAMING
    # ===================================================

    print(
        "\n=== 1. SOURCE STREAMING ==="
    )

    raw_df = (
        spark.readStream
        .format("rate")
        .option(
            "rowsPerSecond",
            4,
        )
        .load()
    )


    # ===================================================
    # 4. SIMULATION DES DOUBLONS
    # ===================================================

    print(
        "\n=== 2. SIMULATION DES DOUBLONS ==="
    )

    events_df = (
        raw_df

        .select(
            col("timestamp").alias(
                "event_timestamp"
            ),

            col("value").alias(
                "technical_id"
            ),
        )

        # Chaque paire de lignes reçoit
        # le même identifiant métier.
        #
        # technical_id :
        # 0 1 2 3 4 5 ...
        #
        # business_event_id :
        # 0 0 1 1 2 2 ...
        #
        # Nous fabriquons donc volontairement
        # un doublon par événement métier.
        .withColumn(
            "business_event_id",

            floor(
                col("technical_id") / 2
            ).cast("long")
        )

        .withColumn(
            "line",

            when(
                col("business_event_id") % 2 == 0,
                lit("A")
            )

            .otherwise(
                lit("B")
            )
        )

        .withColumn(
            "passenger_count",

            (
                col("business_event_id") * 10
                + 100
            ).cast("integer")
        )
    )


    print(
        "Streaming :",
        events_df.isStreaming
    )

    events_df.printSchema()


    # ===================================================
    # 5. WATERMARK
    # ===================================================

    print(
        "\n=== 3. WATERMARK ==="
    )

    watermarked_df = (
        events_df

        .withWatermark(
            "event_timestamp",
            "10 seconds"
        )
    )


    # ===================================================
    # 6. DÉDUPLICATION STREAMING
    # ===================================================

    print(
        "\n=== 4. DÉDUPLICATION STREAMING ==="
    )

    deduplicated_df = (
        watermarked_df

        .dropDuplicatesWithinWatermark(
            [
                "business_event_id"
            ]
        )
    )


    # ===================================================
    # 7. SORTIE
    # ===================================================

    query = (
        deduplicated_df

        .select(
            "event_timestamp",
            "technical_id",
            "business_event_id",
            "line",
            "passenger_count",
        )

        .writeStream

        .format("console")

        .outputMode("append")

        .option(
            "truncate",
            "false"
        )

        .option(
            "checkpointLocation",
            str(checkpoint_dir)
        )

        .trigger(
            processingTime="2 seconds"
        )

        .queryName(
            "ratp_streaming_dedup"
        )

        .start()
    )


    print(
        "\n=== 5. STREAM ACTIF PENDANT 30 SECONDES ==="
    )

    query.awaitTermination(
        30
    )


    # ===================================================
    # 8. OBSERVABILITÉ
    # ===================================================

    print(
        "\n=== 6. MÉTRIQUES DE DÉDUPLICATION ==="
    )

    total_input = 0
    total_output = 0


    for progress in query.recentProgress:

        batch_id = progress.get(
            "batchId"
        )

        input_rows = (
            progress.get(
                "numInputRows",
                0
            )
            or 0
        )

        sink = progress.get(
            "sink",
            {}
        )

        output_rows = (
            sink.get(
                "numOutputRows",
                0
            )
            or 0
        )

        total_input += input_rows
        total_output += output_rows


        event_time = progress.get(
            "eventTime",
            {}
        )

        watermark_value = (
            event_time.get(
                "watermark"
            )
        )


        state_operators = progress.get(
            "stateOperators",
            []
        )


        print(
            f"\nBatch {batch_id}"
        )

        print(
            "  Input rows           :",
            input_rows
        )

        print(
            "  Output rows          :",
            output_rows
        )

        print(
            "  Watermark            :",
            watermark_value
        )


        if state_operators:

            state = state_operators[0]

            print(
                "  State rows total     :",
                state.get(
                    "numRowsTotal"
                )
            )

            print(
                "  State rows updated   :",
                state.get(
                    "numRowsUpdated"
                )
            )

            print(
                "  State rows removed   :",
                state.get(
                    "numRowsRemoved"
                )
            )

            print(
                "  Dropped watermark    :",
                state.get(
                    "numRowsDroppedByWatermark"
                )
            )

            print(
                "  Memory used bytes    :",
                state.get(
                    "memoryUsedBytes"
                )
            )


            custom_metrics = (
                state.get(
                    "customMetrics",
                    {}
                )
            )

            print(
                "  Custom metrics       :",
                custom_metrics
            )


    # ===================================================
    # 9. BILAN GLOBAL
    # ===================================================

    print(
        "\n=== 7. BILAN GLOBAL ==="
    )

    print(
        "Total input :",
        total_input
    )

    print(
        "Total output après déduplication :",
        total_output
    )

    print(
        "Différence input/output :",
        total_input - total_output
    )


finally:

    if query is not None and query.isActive:

        query.stop()

        print(
            "\nStreaming Query arrêtée proprement."
        )


    spark.stop()

    print(
        "SparkSession arrêtée proprement."
    )