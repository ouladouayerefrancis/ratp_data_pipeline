# Bloc 6 — Window Functions et détection de doublons
# row_number(), partitionBy(), orderBy()

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    row_number,
)


spark = (
    SparkSession.builder
    .appName("RATP-PySpark-Windows")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 1. DONNEES BRONZE
# ============================================================

data = [
    (1, 7001, "A", "Nation", 120),
    (2, 7002, "B", "Gare du Nord", 175),

    # doublon exact de 7002
    (3, 7002, "B", "Gare du Nord", 175),

    # versions conflictuelles de 7003
    (4, 7003, "C", "Bastille", 0),
    (5, 7003, "C", "Bastille", 45),

    (6, 7004, "A", "Nation", 220),
]

columns = [
    "bronze_id",
    "event_id",
    "line",
    "station",
    "passenger_count",
]

df = spark.createDataFrame(data, columns)


print("\n=== BRONZE ===")
df.show()


# ============================================================
# 2. COMPTER LES OCCURRENCES PAR event_id
# ============================================================

print("\n=== NOMBRE DE LIGNES PAR event_id ===")

df_counts = (
    df.groupBy("event_id")
    .agg(
        count("*").alias("row_count"),
        countDistinct("passenger_count").alias("version_count"),
    )
    .orderBy("event_id")
)

df_counts.show()


# ============================================================
# 3. CLASSIFICATION
# ============================================================

print("\n=== DOUBLONS EXACTS ===")

df_exact_duplicates = df_counts.filter(
    (col("row_count") > 1)
    & (col("version_count") == 1)
)

df_exact_duplicates.show()


print("\n=== DOUBLONS CONFLICTUELS ===")

df_conflicting_duplicates = df_counts.filter(
    (col("row_count") > 1)
    & (col("version_count") > 1)
)

df_conflicting_duplicates.show()


# ============================================================
# 4. WINDOW FUNCTION
# ============================================================

window_event = (
    Window
    .partitionBy("event_id")
    .orderBy("bronze_id")
)

df_ranked = df.withColumn(
    "row_num",
    row_number().over(window_event)
)


print("\n=== ROW_NUMBER PAR event_id ===")
df_ranked.show()


# ============================================================
# 5. COPIES SUPPLEMENTAIRES
# ============================================================

print("\n=== LIGNES AVEC row_num > 1 ===")

df_supplemental = df_ranked.filter(
    col("row_num") > 1
)

df_supplemental.show()


spark.stop()
