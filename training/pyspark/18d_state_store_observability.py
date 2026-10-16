# Bloc 18D — Structured Streaming
# Observabilité du State Store
#
# Objectifs :
# - comprendre ce que Spark garde en état
# - observer la taille du State Store
# - suivre les lignes ajoutées / supprimées
# - suivre la mémoire utilisée

import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    window,
    count,
)


spark = (
    SparkSession.builder
    .appName("RATP-State-Store-Observability")
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


checkpoint_dir = (
    Path.home()
    / "ratp_spark_checkpoints"
    / "18d_state_store_observability"
)

checkpoint_dir.mkdir(
    parents=True,
    exist_ok=True,
)


query = None


try:

    # ===================================================
    # 1. SOURCE
    # ===================================================

    raw_df = (
        spark.readStream
        .format("rate")
        .option(
            "rowsPerSecond",
            5,
        )
        .load()
    )


    # ===================================================
    # 2. ÉVÉNEMENTS
    # ===================================================

    events_df = (
        raw_df

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
            )
            .otherwise(
                lit("B")
            )
        )
    )


    # ===================================================
    # 3. WATERMARK
    # ===================================================

    watermarked_df = (
        events_df
        .withWatermark(
            "event_timestamp",
            "10 seconds"
        )
    )


    # ===================================================
    # 4. AGRÉGATION STATEFUL
    # ===================================================

    aggregated_df = (
        watermarked_df

        .groupBy(
            window(
                col("event_timestamp"),
                "5 seconds"
            ),

            col("line")
        )

        .agg(
            count("*").alias(
                "total_events"
            )
        )
    )


    # ===================================================
    # 5. QUERY
    # ===================================================

    query = (
        aggregated_df

        .writeStream

        .format("console")

        .outputMode("update")

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
            "ratp_state_store_observability"
        )

        .start()
    )


    print(
        "\n=== STREAM ACTIF PENDANT 30 SECONDES ==="
    )

    query.awaitTermination(
        30
    )


    # ===================================================
    # 6. MÉTRIQUES STATE STORE
    # ===================================================

    print(
        "\n=== STATE STORE PAR MICRO-BATCH ==="
    )

    for progress in query.recentProgress:

        batch_id = progress.get(
            "batchId"
        )

        input_rows = progress.get(
            "numInputRows",
            0
        )

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

        if not state_operators:

            print(
                f"Batch {batch_id} : "
                "aucun opérateur stateful"
            )

            continue


        state = state_operators[0]


        print(
            f"\nBatch {batch_id}"
        )

        print(
            "  Input rows              :",
            input_rows
        )

        print(
            "  State rows total        :",
            state.get(
                "numRowsTotal"
            )
        )

        print(
            "  State rows updated      :",
            state.get(
                "numRowsUpdated"
            )
        )

        print(
            "  State rows removed      :",
            state.get(
                "numRowsRemoved"
            )
        )

        print(
            "  Dropped by watermark    :",
            state.get(
                "numRowsDroppedByWatermark"
            )
        )

        print(
            "  Memory used bytes       :",
            state.get(
                "memoryUsedBytes"
            )
        )

        print(
            "  Watermark               :",
            watermark_value
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