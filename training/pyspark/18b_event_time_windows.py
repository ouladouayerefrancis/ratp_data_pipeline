# Bloc 18B — Structured Streaming
# Event Time et fenêtres temporelles
#
# Objectif :
# - comprendre Event Time
# - créer des fenêtres temporelles
# - faire une agrégation stateful
# - observer le State Store
# - utiliser un checkpoint Linux natif sous WSL

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
)


# -------------------------------------------------------
# 1. SparkSession
# -------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RATP-Streaming-Event-Time-Windows")
    .master("local[*]")

    .config(
        "spark.pyspark.python",
        sys.executable
    )

    .config(
        "spark.pyspark.driver.python",
        sys.executable
    )

    # Très important pour notre petit environnement local.
    #
    # Spark utilise souvent 200 partitions par défaut
    # pour les shuffles.
    #
    # Pour quelques événements pédagogiques,
    # 4 partitions suffisent largement.
    .config(
        "spark.sql.shuffle.partitions",
        "4"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# -------------------------------------------------------
# 2. Checkpoint Linux natif
# -------------------------------------------------------

# IMPORTANT :
# le code reste dans /mnt/c/... afin d'être visible
# depuis Windows / VS Code / Git.
#
# Mais les checkpoints stateful Spark sont stockés
# dans le filesystem Linux natif de WSL.

checkpoint_dir = (
    Path.home()
    / "ratp_spark_checkpoints"
    / "18b_event_time_windows"
)

checkpoint_dir.mkdir(
    parents=True,
    exist_ok=True
)


query = None


try:

    # ===================================================
    # 3. SOURCE STREAMING
    # ===================================================

    print("\n=== 1. SOURCE STREAMING ===")

    raw_stream_df = (
        spark.readStream
        .format("rate")
        .option(
            "rowsPerSecond",
            3
        )
        .load()
    )


    # ===================================================
    # 4. ÉVÉNEMENTS RATP SIMULÉS
    # ===================================================

    print(
        "\n=== 2. ÉVÉNEMENTS RATP SIMULÉS ==="
    )

    events_df = (
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
    # 5. FENÊTRES EVENT TIME
    # ===================================================

    print(
        "\n=== 3. AGRÉGATION PAR FENÊTRE DE 5 SECONDES ==="
    )

    windowed_df = (
        events_df

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
    # 6. STREAMING QUERY
    # ===================================================

    print(
        "\n=== 4. DÉMARRAGE DU STREAM ==="
    )

    print(
        "Checkpoint :",
        checkpoint_dir
    )

    print(
        "Partitions de shuffle :",
        spark.conf.get(
            "spark.sql.shuffle.partitions"
        )
    )


    query = (
        windowed_df

        .writeStream

        .format("console")

        # Une fenêtre peut être mise à jour
        # par plusieurs micro-batches.
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
            "ratp_event_time_windows"
        )

        .start()
    )


    print(
        "Query active :",
        query.isActive
    )


    # ===================================================
    # 7. EXÉCUTION
    # ===================================================

    print(
        "\n=== 5. STREAM ACTIF PENDANT 20 SECONDES ==="
    )

    query.awaitTermination(
        20
    )


    # ===================================================
    # 8. ÉTAT FINAL
    # ===================================================

    print(
        "\n=== 6. ÉTAT DE LA QUERY ==="
    )

    print(
        "Active :",
        query.isActive
    )

    print(
        "Status :",
        query.status
    )

    if query.lastProgress is not None:

        print(
            "Dernier batch ID :",
            query.lastProgress.get(
                "batchId"
            )
        )

        print(
            "Input rows :",
            query.lastProgress.get(
                "numInputRows"
            )
        )


finally:

    # ===================================================
    # 9. ARRÊT PROPRE
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