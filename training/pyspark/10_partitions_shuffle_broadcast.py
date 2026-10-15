# Bloc 10 — Partitions, Shuffle, repartition, coalesce, broadcast join

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    count,
    sum,
)


spark = (
    SparkSession.builder
    .appName("RATP-Partitions-Shuffle-Broadcast")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 1. DATAFRAME EVENEMENTS
# ============================================================

events_data = [
    (7001, "A", 120),
    (7002, "B", 175),
    (7003, "C", 0),
    (7004, "A", 220),
    (7005, "B", 190),
    (7006, "C", 25),
]

events_columns = [
    "event_id",
    "line",
    "passenger_count",
]

df_events = spark.createDataFrame(
    events_data,
    events_columns,
)


print("\n=== PARTITIONS INITIALES ===")

print(
    df_events.rdd.getNumPartitions()
)


# ============================================================
# 2. REPARTITION
# ============================================================

df_repartitioned = df_events.repartition(
    4,
    "line",
)


print("\n=== APRES repartition(4, 'line') ===")

print(
    df_repartitioned.rdd.getNumPartitions()
)


# ============================================================
# 3. COALESCE
# ============================================================

df_coalesced = df_repartitioned.coalesce(2)


print("\n=== APRES coalesce(2) ===")

print(
    df_coalesced.rdd.getNumPartitions()
)


# ============================================================
# 4. GROUP BY = SHUFFLE POTENTIEL
# ============================================================

df_metrics = (
    df_events
    .groupBy("line")
    .agg(
        count("*").alias("total_events"),
        sum("passenger_count").alias(
            "total_passengers"
        ),
    )
)


print("\n=== RESULTAT GROUP BY ===")

df_metrics.show()


print("\n=== PLAN GROUP BY ===")

df_metrics.explain(
    mode="formatted"
)


# ============================================================
# 5. PETIT REFERENTIEL
# ============================================================

lines_data = [
    ("A", "RER A"),
    ("B", "RER B"),
    ("C", "Metro C"),
]

lines_columns = [
    "line",
    "line_name",
]

df_lines = spark.createDataFrame(
    lines_data,
    lines_columns,
)


# ============================================================
# 6. JOIN CLASSIQUE
# ============================================================

df_join_normal = df_events.join(
    df_lines,
    on="line",
    how="left",
)


print("\n=== PLAN JOIN NORMAL ===")

df_join_normal.explain(
    mode="formatted"
)


# ============================================================
# 7. BROADCAST JOIN
# ============================================================

df_join_broadcast = df_events.join(
    broadcast(df_lines),
    on="line",
    how="left",
)


print("\n=== RESULTAT BROADCAST JOIN ===")

df_join_broadcast.show()


print("\n=== PLAN BROADCAST JOIN ===")

df_join_broadcast.explain(
    mode="formatted"
)


spark.stop()