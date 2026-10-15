# Bloc 4 — Agrégations PySpark
# groupBy(), agg(), count(), sum(), avg()

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    min,
    max,
    col,
)


spark = (
    SparkSession.builder
    .appName("RATP-PySpark-Aggregations")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


data = [
    (7001, "A", "Nation", "validation", 120),
    (7002, "B", "Gare du Nord", "validation", 175),
    (7003, "C", "Bastille", "incident", 0),
    (7004, "A", "Nation", "validation", 220),
    (7005, "B", "Gare du Nord", "validation", 190),
    (7006, "C", "Bastille", "incident", 25),
]

columns = [
    "event_id",
    "line",
    "station",
    "event_type",
    "passenger_count",
]

df = spark.createDataFrame(data, columns)


# ============================================================
# 1. COUNT PAR LIGNE
# ============================================================

print("\n=== NOMBRE D'EVENEMENTS PAR LIGNE ===")

df_count_by_line = (
    df.groupBy("line")
    .agg(
        count("*").alias("total_events")
    )
    .orderBy("line")
)

df_count_by_line.show()


# ============================================================
# 2. SOMME DES PASSAGERS PAR LIGNE
# ============================================================

print("\n=== TOTAL PASSAGERS PAR LIGNE ===")

df_passengers_by_line = (
    df.groupBy("line")
    .agg(
        sum("passenger_count").alias("total_passengers")
    )
    .orderBy("line")
)

df_passengers_by_line.show()


# ============================================================
# 3. PLUSIEURS AGREGATIONS
# ============================================================

print("\n=== METRIQUES PAR LIGNE ===")

df_metrics = (
    df.groupBy("line")
    .agg(
        count("*").alias("total_events"),
        sum("passenger_count").alias("total_passengers"),
        avg("passenger_count").alias("avg_passengers"),
        min("passenger_count").alias("min_passengers"),
        max("passenger_count").alias("max_passengers"),
    )
    .orderBy("line")
)

df_metrics.show()


# ============================================================
# 4. AGREGATION PAR TYPE D'EVENEMENT
# ============================================================

print("\n=== METRIQUES PAR TYPE D'EVENEMENT ===")

df_by_event_type = (
    df.groupBy("event_type")
    .agg(
        count("*").alias("total_events"),
        sum("passenger_count").alias("total_passengers"),
    )
    .orderBy("event_type")
)

df_by_event_type.show()


spark.stop()