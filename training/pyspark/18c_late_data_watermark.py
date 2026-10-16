# Bloc 18C — Structured Streaming
# Late Data et Watermark
#
# Objectifs :
# - comprendre les événements retardataires
# - utiliser withWatermark()
# - contrôler la taille du State Store
# - observer les lignes supprimées par watermark
# - inspecter les métriques batch par batch

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
    sum as _sum,
    expr,
)


# -------------------------------------------------------
# 1. SparkSession
# -------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RATP-Streaming-Late-Data-Watermark")
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
# 2. Checkpoint Linux natif
# -------------------------------------------------------

checkpoint_dir = (
    Path.home()
    / "ratp_spark_checkpoints"
    / "18c_late_data_watermark"
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

    print("\n=== 1. SOURCE STREAMING ===")

    raw_df = (
        spark.readStream
        .format("rate")
        .option(
            "rowsPerSecond",
            3,
        )
        .load()
    )


    # ===================================================
    # 4. SIMULATION DE LATE DATA
    # ===================================================

    print(
        "\n=== 2. SIMULATION DE DONNÉES EN RETARD ==="
    )

    events_df = (
        raw_df

        .select(
            col("timestamp").alias(
                "source_timestamp"
            ),
            col("value").alias(
                "event_id"
            ),
        )

        # Tous les 10 événements,
        # nous simulons un événement vieux de 20 secondes.
        .withColumn(
            "event_timestamp",

            when(
                col("event_id") % 10 == 0,

                col("source_timestamp")
                - expr("INTERVAL 20 SECONDS")
            )

            .otherwise(
                col("source_timestamp")
            )
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

        .withColumn(
            "passenger_count",

            (
                col("event_id") * 10 + 100
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
        "\n=== 3. WATERMARK DE 10 SECONDES ==="
    )

    watermarked_df = (
        events_df

        .withWatermark(
            "event_timestamp",
            "10 seconds"
        )
    )


    # ===================================================
    # 6. AGRÉGATION PAR FENÊTRE
    # ===================================================

    windowed_df = (
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
            ),

            _sum(
                "passenger_count"
            ).alias(
                "total_passengers"
            )
        )

        .select(
            col("window.start").alias(
                "window_start"
            ),

            col("window.end").alias(
                "window_end"
            ),

            col("line"),

            col("total_events"),

            col("total_passengers"),
        )
    )


    # ===================================================
    # 7. WRITE STREAM
    # ===================================================

    print(
        "\n=== 4. DÉMARRAGE DU STREAM ==="
    )

    print(
        "Checkpoint :",
        checkpoint_dir
    )

    query = (
        windowed_df

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
            "ratp_late_data_watermark"
        )

        .start()
    )


    print(
        "Query active :",
        query.isActive
    )


    # ===================================================
    # 8. EXÉCUTION
    # ===================================================

    print(
        "\n=== 5. STREAM ACTIF PENDANT 30 SECONDES ==="
    )

    query.awaitTermination(
        30
    )


    # ===================================================
    # 9. HISTORIQUE WATERMARK PAR BATCH
    # ===================================================

    print(
        "\n=== 6. HISTORIQUE WATERMARK PAR BATCH ==="
    )

    total_dropped = 0

    for progress in query.recentProgress:

        batch_id = progress.get(
            "batchId"
        )

        input_rows = progress.get(
            "numInputRows"
        )

        event_time_metrics = progress.get(
            "eventTime",
            {}
        )

        watermark_value = (
            event_time_metrics.get(
                "watermark"
            )
        )

        event_time_max = (
            event_time_metrics.get(
                "max"
            )
        )

        state_operators = progress.get(
            "stateOperators",
            []
        )

        dropped = 0
        state_rows = 0
        updated_rows = 0

        if state_operators:

            state = state_operators[0]

            dropped = (
                state.get(
                    "numRowsDroppedByWatermark",
                    0
                )
                or 0
            )

            state_rows = (
                state.get(
                    "numRowsTotal",
                    0
                )
                or 0
            )

            updated_rows = (
                state.get(
                    "numRowsUpdated",
                    0
                )
                or 0
            )

        total_dropped += dropped

        print(
            f"Batch {batch_id:>2} | "
            f"input={input_rows:>2} | "
            f"state={state_rows:>2} | "
            f"updated={updated_rows:>2} | "
            f"dropped={dropped:>2}"
        )

        print(
            f"         max={event_time_max}"
        )

        print(
            f"         watermark={watermark_value}"
        )


    print(
        "\nTotal lignes supprimées par watermark :",
        total_dropped
    )


    # ===================================================
    # 10. DERNIER ÉTAT DE LA QUERY
    # ===================================================

    print(
        "\n=== 7. ÉTAT FINAL DE LA QUERY ==="
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
    # 11. ARRÊT PROPRE
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