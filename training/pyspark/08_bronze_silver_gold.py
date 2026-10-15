# Bloc 8 — Pipeline PySpark Bronze → Silver → Gold

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
    to_timestamp,
    to_date,
    sum,
)


spark = (
    SparkSession.builder
    .appName("RATP-Bronze-Silver-Gold")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 1. BRONZE
# ============================================================

bronze_data = [
    (
        1,
        7001,
        "2026-10-14 07:30:00",
        "A",
        "Nation",
        "validation",
        120,
    ),

    (
        2,
        7002,
        "2026-10-14 07:35:00",
        "B",
        "Gare du Nord",
        "validation",
        175,
    ),

    # Doublon exact de 7002
    (
        3,
        7002,
        "2026-10-14 07:35:00",
        "B",
        "Gare du Nord",
        "validation",
        175,
    ),

    # Conflit 7003
    (
        4,
        7003,
        "2026-10-14 07:40:00",
        "C",
        "Bastille",
        "incident",
        0,
    ),

    (
        5,
        7003,
        "2026-10-14 07:40:00",
        "C",
        "Bastille",
        "incident",
        45,
    ),

    (
        6,
        7004,
        "2026-10-14 07:45:00",
        "A",
        "Nation",
        "validation",
        220,
    ),
]


bronze_columns = [
    "bronze_id",
    "event_id",
    "event_timestamp",
    "line",
    "station",
    "event_type",
    "passenger_count",
]


df_bronze = spark.createDataFrame(
    bronze_data,
    bronze_columns,
)


print("\n=== BRONZE ===")
df_bronze.show(truncate=False)


# ============================================================
# 2. PROFILING DES VERSIONS
# ============================================================

business_columns = [
    "event_id",
    "event_timestamp",
    "line",
    "station",
    "event_type",
    "passenger_count",
]


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


# ============================================================
# 3. ROW_NUMBER
# ============================================================

window_event = (
    Window
    .partitionBy("event_id")
    .orderBy("bronze_id")
)


df_ranked = df_bronze.withColumn(
    "row_num",
    row_number().over(window_event)
)


# ============================================================
# 4. JOINDRE PROFILING + BRONZE
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
# 5. CLASSIFICATION DATA QUALITY
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


print("\n=== CLASSIFICATION DQ ===")

df_dq.select(
    "bronze_id",
    "event_id",
    "passenger_count",
    "dq_status",
).orderBy("bronze_id").show()


# ============================================================
# 6. QUARANTAINE
# ============================================================

df_quarantine = df_dq.filter(
    col("dq_status") == "CONFLICTING_DUPLICATE"
)


print("\n=== QUARANTAINE ===")

df_quarantine.select(
    "bronze_id",
    "event_id",
    "passenger_count",
    "dq_status",
).show()


# ============================================================
# 7. SILVER
# ============================================================

df_silver = (
    df_dq
    .filter(
        col("dq_status") == "PASS"
    )

    .withColumn(
        "event_timestamp",
        to_timestamp(
            col("event_timestamp"),
            "yyyy-MM-dd HH:mm:ss",
        )
    )

    .select(
        "event_id",
        "event_timestamp",
        "line",
        "station",
        "event_type",
        "passenger_count",
        "bronze_id",
    )
)


print("\n=== SILVER ===")
df_silver.show(truncate=False)


print("\n=== SCHEMA SILVER ===")
df_silver.printSchema()


# ============================================================
# 8. GOLD
# ============================================================

df_gold = (
    df_silver

    .withColumn(
        "event_date",
        to_date("event_timestamp")
    )

    .groupBy(
        "event_date",
        "line",
    )

    .agg(
        count("*").alias("total_events"),

        sum(
            "passenger_count"
        ).alias("total_passengers"),

        sum(
            when(
                col("event_type") == "validation",
                1
            ).otherwise(0)
        ).alias("validation_events"),

        sum(
            when(
                col("event_type") == "incident",
                1
            ).otherwise(0)
        ).alias("incident_events"),
    )

    .orderBy(
        "event_date",
        "line",
    )
)


print("\n=== GOLD ===")
df_gold.show(truncate=False)


# ============================================================
# 9. RECONCILIATION BRONZE
# ============================================================

bronze_count = df_bronze.count()

silver_count = df_silver.count()

exact_duplicate_count = (
    df_dq
    .filter(
        col("dq_status") == "EXACT_DUPLICATE"
    )
    .count()
)

conflict_count = df_quarantine.count()


explained_count = (
    silver_count
    + exact_duplicate_count
    + conflict_count
)


print("\n=== RECONCILIATION BRONZE ===")

print("Bronze              :", bronze_count)
print("Silver              :", silver_count)
print("Exact duplicates    :", exact_duplicate_count)
print("Quarantaine         :", conflict_count)
print("Total expliqué      :", explained_count)


if bronze_count == explained_count:
    print("Réconciliation      : PASS")
else:
    print("Réconciliation      : FAIL")


# ============================================================
# 10. RECONCILIATION SILVER / GOLD
# ============================================================

gold_event_count = (
    df_gold
    .agg(
        sum("total_events")
        .alias("gold_events")
    )
    .collect()[0]["gold_events"]
)


print("\n=== RECONCILIATION SILVER / GOLD ===")

print("Silver events :", silver_count)
print("Gold events   :", gold_event_count)


if silver_count == gold_event_count:
    print("Silver / Gold : PASS")
else:
    print("Silver / Gold : FAIL")


spark.stop()