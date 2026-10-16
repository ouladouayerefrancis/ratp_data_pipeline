# Bloc 18F — Structured Streaming
# Checkpoint, restart et recovery
#
# Objectifs :
# - comprendre la reprise après arrêt
# - conserver les offsets/progrès
# - éviter de retraiter les anciens fichiers
# - démontrer le rôle du checkpoint

import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    IntegerType,
)


# -------------------------------------------------------
# 1. Répertoires
# -------------------------------------------------------

base_dir = Path(__file__).resolve().parent

input_dir = (
    base_dir
    / "data"
    / "streaming_recovery"
)

output_dir = (
    base_dir
    / "output"
    / "18f_checkpoint_recovery"
)

checkpoint_dir = (
    Path.home()
    / "ratp_spark_checkpoints"
    / "18f_checkpoint_recovery"
)

input_dir.mkdir(
    parents=True,
    exist_ok=True,
)

output_dir.mkdir(
    parents=True,
    exist_ok=True,
)

checkpoint_dir.mkdir(
    parents=True,
    exist_ok=True,
)


# -------------------------------------------------------
# 2. Schema explicite
# -------------------------------------------------------

schema = StructType([
    StructField(
        "event_id",
        LongType(),
        False,
    ),
    StructField(
        "line",
        StringType(),
        False,
    ),
    StructField(
        "passenger_count",
        IntegerType(),
        False,
    ),
])


# -------------------------------------------------------
# 3. SparkSession
# -------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RATP-Checkpoint-Recovery")
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


query = None


try:

    # ===================================================
    # 4. READ STREAM
    # ===================================================

    print(
        "\n=== 1. LECTURE STREAMING ==="
    )

    print(
        "Input :",
        input_dir
    )

    print(
        "Output :",
        output_dir
    )

    print(
        "Checkpoint :",
        checkpoint_dir
    )


    stream_df = (
        spark.readStream

        .schema(schema)

        .format("json")

        .load(
            str(input_dir)
        )
    )


    print(
        "isStreaming :",
        stream_df.isStreaming
    )


    # ===================================================
    # 5. WRITE STREAM
    # ===================================================

    query = (
        stream_df

        .writeStream

        .format("parquet")

        .outputMode("append")

        .option(
            "path",
            str(output_dir)
        )

        .option(
            "checkpointLocation",
            str(checkpoint_dir)
        )

        .trigger(
            availableNow=True
        )

        .queryName(
            "ratp_checkpoint_recovery"
        )

        .start()
    )


    # availableNow :
    # traite tout ce qui est actuellement disponible,
    # puis termine proprement.

    query.awaitTermination()


    # ===================================================
    # 6. MÉTRIQUES
    # ===================================================

    print(
        "\n=== 2. TRAITEMENT TERMINÉ ==="
    )

    for progress in query.recentProgress:

        print(
            "Batch ID :",
            progress.get(
                "batchId"
            )
        )

        print(
            "Input rows :",
            progress.get(
                "numInputRows"
            )
        )


    # ===================================================
    # 7. LECTURE DU RÉSULTAT
    # ===================================================

    print(
        "\n=== 3. CONTENU ACTUEL DE LA SORTIE ==="
    )

    result_df = (
        spark.read
        .parquet(
            str(output_dir)
        )
    )

    result_df.orderBy(
        "event_id"
    ).show(
        truncate=False
    )

    print(
        "Nombre total de lignes :",
        result_df.count()
    )


finally:

    if query is not None and query.isActive:

        query.stop()

    spark.stop()

    print(
        "\nSparkSession arrêtée proprement."
    )