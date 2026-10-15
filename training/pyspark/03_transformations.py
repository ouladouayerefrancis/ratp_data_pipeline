# Bloc 3 — Transformations PySpark
# select(), filter(), withColumn(), when(), cast()

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


spark = (
    SparkSession.builder
    .appName("RATP-PySpark-Transformations")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


data = [
    (7001, "2026-10-14 07:30:00", "A", "Nation", "validation", 120),
    (7002, "2026-10-14 07:35:00", "B", "Gare du Nord", "validation", 175),
    (7003, "2026-10-14 07:40:00", "C", "Bastille", "incident", 0),
    (7004, "2026-10-14 07:45:00", "A", "Nation", "validation", 220),
]

columns = [
    "event_id",
    "event_timestamp",
    "line",
    "station",
    "event_type",
    "passenger_count",
]

df = spark.createDataFrame(data, columns)


# ============================================================
# 1. SELECT
# ============================================================

print("\n=== SELECT : event_id, line, passenger_count ===")

df_select = df.select(
    "event_id",
    "line",
    "passenger_count",
)

df_select.show()


# ============================================================
# 2. FILTER / WHERE
# ============================================================

print("\n=== FILTER : passenger_count >= 150 ===")

df_filtered = df.filter(
    col("passenger_count") >= 150
)

df_filtered.show()


# ============================================================
# 3. WITHCOLUMN + WHEN
# ============================================================

print("\n=== AJOUT D'UNE COLONNE traffic_level ===")

df_enriched = df.withColumn(
    "traffic_level",
    when(
        col("passenger_count") >= 150,
        "HIGH"
    ).otherwise("NORMAL")
)

df_enriched.show()


# ============================================================
# 4. FILTRE SUR TYPE EVENEMENT
# ============================================================

print("\n=== INCIDENTS UNIQUEMENT ===")

df_incidents = df.filter(
    col("event_type") == "incident"
)

df_incidents.show()


spark.stop()