# Bloc 16 — Driver, Jobs, Stages, Tasks et Shuffle

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop-3.3.4"
os.environ["PATH"] = (
    r"C:\hadoop-3.3.4\bin;"
    + os.environ["PATH"]
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    spark_partition_id,
    sum,
)


# ============================================================
# 1. SPARKSESSION — LE DRIVER
# ============================================================

spark = (
    SparkSession.builder
    .appName("RATP-Jobs-Stages-Tasks")
    .master("local[*]")
    .config(
        "spark.pyspark.python",
        sys.executable,
    )
    .config(
        "spark.pyspark.driver.python",
        sys.executable,
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


print("\n=== ENVIRONNEMENT SPARK ===")

print(
    "Application :",
    spark.sparkContext.appName,
)

print(
    "Master      :",
    spark.sparkContext.master,
)

print(
    "Parallelisme par defaut :",
    spark.sparkContext.defaultParallelism,
)


# ============================================================
# 2. DATAFRAME SOURCE
# ============================================================

data = [
    (1, "A", 100),
    (2, "A", 120),
    (3, "A", 140),
    (4, "B", 80),
    (5, "B", 90),
    (6, "B", 110),
    (7, "C", 50),
    (8, "C", 70),
]

df = spark.createDataFrame(
    data,
    [
        "event_id",
        "line",
        "passenger_count",
    ],
)


print("\n=== DATAFRAME SOURCE ===")

df.show()


print("\n=== PARTITIONS INITIALES ===")

print(
    df.rdd.getNumPartitions()
)


# ============================================================
# 3. REPARTITION EN 4 PARTITIONS
# ============================================================

df_4 = (
    df
    .repartition(4)
)


print("\n=== APRES repartition(4) ===")

print(
    df_4.rdd.getNumPartitions()
)


# ============================================================
# 4. IDENTIFIER LA PARTITION DE CHAQUE LIGNE
# ============================================================

df_partitions = (
    df_4

    .withColumn(
        "partition_id",
        spark_partition_id(),
    )

    .orderBy(
        "partition_id",
        "event_id",
    )
)


print("\n=== REPARTITION DES LIGNES ===")

df_partitions.show(
    truncate=False
)


# ============================================================
# 5. NARROW TRANSFORMATION
#
# filter() peut traiter chaque partition indépendamment.
# ============================================================

df_filtered = (
    df_4
    .filter(
        col("passenger_count") >= 90
    )
)


print("\n=== PLAN FILTER — NARROW ===")

df_filtered.explain(
    mode="formatted"
)


# ============================================================
# 6. WIDE TRANSFORMATION
#
# groupBy() doit réunir les lignes ayant la même clé.
# Cela nécessite généralement un shuffle.
# ============================================================

df_grouped = (
    df_4

    .groupBy(
        "line"
    )

    .agg(
        count("*").alias(
            "total_events"
        ),

        sum(
            "passenger_count"
        ).alias(
            "total_passengers"
        ),
    )
)


print("\n=== PLAN GROUP BY — WIDE ===")

df_grouped.explain(
    mode="formatted"
)


# ============================================================
# 7. ACTION
#
# show() déclenche réellement l'exécution.
# ============================================================

print("\n=== ACTION show() ===")

df_grouped.show(
    truncate=False
)


spark.stop()