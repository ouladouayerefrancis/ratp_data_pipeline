# Bloc 12 — Schéma, parsing et Data Quality avec PySpark

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
    expr,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


spark = (
    SparkSession.builder
    .appName("RATP-Schema-DataQuality")
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


# ============================================================
# 1. BRONZE : TOUT LIRE COMME TEXTE
# ============================================================

raw_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("line", StringType(), True),
    StructField("station", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("passenger_count", StringType(), True),
])


raw_path = (
    "training/pyspark/data/"
    "events_bad.csv"
)


df_bronze = (
    spark.read
    .option("header", True)
    .schema(raw_schema)
    .csv(raw_path)
)


print("\n=== BRONZE — DONNEES BRUTES ===")

df_bronze.show(
    truncate=False
)

df_bronze.printSchema()


# ============================================================
# 2. PARSING POUR PREPARER SILVER
# ============================================================

df_parsed = (
    df_bronze

    .withColumn(
        "event_id_parsed",
        expr(
            "try_cast(event_id as BIGINT)"
        ),
    )

    .withColumn(
        "event_timestamp_parsed",
        to_timestamp(
            col("event_timestamp")
        ),
    )

    .withColumn(
        "passenger_count_parsed",
        expr(
            "try_cast(passenger_count as BIGINT)"
        ),
    )
)


print(
    "\n=== APRES PARSING ==="
)

df_parsed.show(
    truncate=False
)


# ============================================================
# 3. CLASSIFICATION DATA QUALITY
# ============================================================

df_dq = (
    df_parsed

    .withColumn(
        "dq_status",

        when(
            col("event_id_parsed").isNull(),
            "FAIL",
        )

        .when(
            col(
                "event_timestamp_parsed"
            ).isNull(),
            "FAIL",
        )

        .when(
            col("line").isNull()
            | (col("line") == ""),
            "FAIL",
        )

        .when(
            col(
                "passenger_count_parsed"
            ).isNull(),
            "FAIL",
        )

        .when(
            col(
                "passenger_count_parsed"
            ) < 0,
            "FAIL",
        )

        .otherwise(
            "PASS"
        )
    )

    .withColumn(
        "dq_reason",

        when(
            col("event_id_parsed").isNull(),
            "INVALID_EVENT_ID",
        )

        .when(
            col(
                "event_timestamp_parsed"
            ).isNull(),
            "INVALID_TIMESTAMP",
        )

        .when(
            col("line").isNull()
            | (col("line") == ""),
            "MISSING_LINE",
        )

        .when(
            col(
                "passenger_count_parsed"
            ).isNull(),
            "INVALID_PASSENGER_COUNT",
        )

        .when(
            col(
                "passenger_count_parsed"
            ) < 0,
            "NEGATIVE_PASSENGER_COUNT",
        )

        .otherwise(
            "OK"
        )
    )
)


print(
    "\n=== RESULTAT DATA QUALITY ==="
)

df_dq.select(
    "event_id",
    "event_timestamp",
    "line",
    "passenger_count",
    "dq_status",
    "dq_reason",
).show(
    truncate=False
)


# ============================================================
# 4. SILVER
# ============================================================

df_silver = (
    df_dq
    .filter(
        col("dq_status") == "PASS"
    )
    .select(
        col(
            "event_id_parsed"
        ).alias(
            "event_id"
        ),

        col(
            "event_timestamp_parsed"
        ).alias(
            "event_timestamp"
        ),

        "line",
        "station",
        "event_type",

        col(
            "passenger_count_parsed"
        ).alias(
            "passenger_count"
        ),
    )
)


print(
    "\n=== SILVER ==="
)

df_silver.show(
    truncate=False
)

df_silver.printSchema()


# ============================================================
# 5. QUARANTAINE
# ============================================================

df_quarantine = (
    df_dq
    .filter(
        col("dq_status") == "FAIL"
    )
)


print(
    "\n=== QUARANTAINE ==="
)

df_quarantine.select(
    "event_id",
    "event_timestamp",
    "line",
    "passenger_count",
    "dq_reason",
).show(
    truncate=False
)


# ============================================================
# 6. RECONCILIATION
# ============================================================

bronze_count = (
    df_bronze.count()
)

silver_count = (
    df_silver.count()
)

quarantine_count = (
    df_quarantine.count()
)


print(
    "\n=== RECONCILIATION ==="
)

print(
    "Bronze     :",
    bronze_count,
)

print(
    "Silver     :",
    silver_count,
)

print(
    "Quarantaine:",
    quarantine_count,
)


if (
    bronze_count
    == silver_count
    + quarantine_count
):
    print(
        "Réconciliation : PASS"
    )
else:
    print(
        "Réconciliation : FAIL"
    )


spark.stop()