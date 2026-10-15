# Bloc 7 — Classification Data Quality PySpark
# PASS / EXACT_DUPLICATE / CONFLICTING_DUPLICATE

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
    struct,
    when,
)


spark = (
    SparkSession.builder
    .appName("RATP-PySpark-DQ")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 1. BRONZE
# ============================================================

data = [
    (1, 7001, "A", "Nation", "validation", 120),

    # 7002 : doublon exact
    (2, 7002, "B", "Gare du Nord", "validation", 175),
    (3, 7002, "B", "Gare du Nord", "validation", 175),

    # 7003 : doublon conflictuel
    (4, 7003, "C", "Bastille", "incident", 0),
    (5, 7003, "C", "Bastille", "incident", 45),

    # ligne normale
    (6, 7004, "A", "Nation", "validation", 220),
]

columns = [
    "bronze_id",
    "event_id",
    "line",
    "station",
    "event_type",
    "passenger_count",
]

df_bronze = spark.createDataFrame(data, columns)


print("\n=== BRONZE ===")
df_bronze.show()


# ============================================================
# 2. COLONNES METIER
# ============================================================

business_columns = [
    "event_id",
    "line",
    "station",
    "event_type",
    "passenger_count",
]


# ============================================================
# 3. PROFILING PAR event_id
# ============================================================

df_profile = (
    df_bronze
    .groupBy("event_id")
    .agg(
        count("*").alias("row_count"),

        countDistinct(
            struct(
                *[col(c) for c in business_columns]
            )
        ).alias("version_count")
    )
)


print("\n=== PROFILING PAR event_id ===")
df_profile.orderBy("event_id").show()


# ============================================================
# 4. ROW_NUMBER
# ============================================================

window_event = (
    Window
    .partitionBy("event_id")
    .orderBy("bronze_id")
)

df_ranked = (
    df_bronze
    .withColumn(
        "row_num",
        row_number().over(window_event)
    )
)


# ============================================================
# 5. JOINDRE PROFILING + BRONZE
# ============================================================

df_dq = (
    df_ranked
    .join(
        df_profile,
        on="event_id",
        how="left"
    )
)


# ============================================================
# 6. CLASSIFICATION DQ
# ============================================================

df_dq = df_dq.withColumn(
    "dq_status",

    when(
        (col("row_count") > 1)
        & (col("version_count") > 1),
        "CONFLICTING_DUPLICATE"
    )

    .when(
        (col("row_count") > 1)
        & (col("version_count") == 1)
        & (col("row_num") > 1),
        "EXACT_DUPLICATE"
    )

    .otherwise("PASS")
)


print("\n=== CLASSIFICATION DATA QUALITY ===")

df_dq.select(
    "bronze_id",
    "event_id",
    "passenger_count",
    "row_num",
    "row_count",
    "version_count",
    "dq_status",
).orderBy("bronze_id").show(truncate=False)


# ============================================================
# 7. COMPTER LES STATUTS
# ============================================================

print("\n=== COMPTE PAR STATUT DQ ===")

df_dq.groupBy("dq_status").count().show()


# ============================================================
# 8. QUARANTAINE
# ============================================================

print("\n=== QUARANTAINE ===")

df_quarantine = df_dq.filter(
    col("dq_status") == "CONFLICTING_DUPLICATE"
)

df_quarantine.select(
    "bronze_id",
    "event_id",
    "passenger_count",
    "dq_status",
).show()


# ============================================================
# 9. LIGNES ELIGIBLES SILVER
# ============================================================

print("\n=== ELIGIBLES SILVER ===")

df_silver_candidates = df_dq.filter(
    col("dq_status") == "PASS"
)

df_silver_candidates.select(
    "bronze_id",
    "event_id",
    "line",
    "station",
    "passenger_count",
).show()


spark.stop()