# Bloc 13 — Data Quality multi-règles et Pipeline Gate robuste

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
    concat_ws,
    expr,
    lit,
    sum,
    to_timestamp,
    when,
)

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
)


# ============================================================
# 1. SPARKSESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("RATP-DQ-MultiRules-Gate")
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
# 2. SCHEMA BRONZE
#    Fidélité à la source : tout est lu comme texte
# ============================================================

bronze_schema = StructType([
    StructField(
        "event_id",
        StringType(),
        True,
    ),
    StructField(
        "event_timestamp",
        StringType(),
        True,
    ),
    StructField(
        "line",
        StringType(),
        True,
    ),
    StructField(
        "station",
        StringType(),
        True,
    ),
    StructField(
        "event_type",
        StringType(),
        True,
    ),
    StructField(
        "passenger_count",
        StringType(),
        True,
    ),
])


raw_path = (
    "training/pyspark/data/"
    "events_bad.csv"
)


df_bronze = (
    spark.read
    .option(
        "header",
        True,
    )
    .schema(
        bronze_schema
    )
    .csv(
        raw_path
    )
)


print(
    "\n=== BRONZE ==="
)

df_bronze.show(
    truncate=False
)


# ============================================================
# 3. PARSING
# ============================================================

df_parsed = (
    df_bronze

    .withColumn(
        "event_id_parsed",
        expr(
            "try_cast(event_id AS BIGINT)"
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
            "try_cast(passenger_count AS BIGINT)"
        ),
    )
)


# ============================================================
# 4. UNE COLONNE PAR REGLE DQ
# ============================================================

df_rules = (
    df_parsed

    .withColumn(
        "dq_invalid_event_id",

        when(
            col("event_id_parsed").isNull(),
            lit(1),
        ).otherwise(
            lit(0)
        ),
    )

    .withColumn(
        "dq_invalid_timestamp",

        when(
            col(
                "event_timestamp_parsed"
            ).isNull(),
            lit(1),
        ).otherwise(
            lit(0)
        ),
    )

    .withColumn(
        "dq_missing_line",

        when(
            col("line").isNull()
            | (col("line") == ""),
            lit(1),
        ).otherwise(
            lit(0)
        ),
    )

    .withColumn(
        "dq_invalid_passenger_count",

        when(
            col(
                "passenger_count_parsed"
            ).isNull(),
            lit(1),
        ).otherwise(
            lit(0)
        ),
    )

    .withColumn(
        "dq_negative_passenger_count",

        when(
            col(
                "passenger_count_parsed"
            ) < 0,
            lit(1),
        ).otherwise(
            lit(0)
        ),
    )
)


# ============================================================
# 5. CONSERVER TOUTES LES RAISONS
# ============================================================

df_dq = (
    df_rules

    .withColumn(
        "dq_reason",

        concat_ws(
            "|",

            when(
                col("dq_invalid_event_id") == 1,
                "INVALID_EVENT_ID",
            ),

            when(
                col("dq_invalid_timestamp") == 1,
                "INVALID_TIMESTAMP",
            ),

            when(
                col("dq_missing_line") == 1,
                "MISSING_LINE",
            ),

            when(
                col(
                    "dq_invalid_passenger_count"
                ) == 1,
                "INVALID_PASSENGER_COUNT",
            ),

            when(
                col(
                    "dq_negative_passenger_count"
                ) == 1,
                "NEGATIVE_PASSENGER_COUNT",
            ),
        ),
    )

    .withColumn(
        "dq_status",

        when(
            col("dq_reason") == "",
            "PASS",
        ).otherwise(
            "FAIL"
        ),
    )
)


print(
    "\n=== RESULTAT DATA QUALITY MULTI-REGLES ==="
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
# 6. SILVER
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


# ============================================================
# 7. QUARANTAINE
# ============================================================

df_quarantine = (
    df_dq
    .filter(
        col("dq_status") == "FAIL"
    )
)


print(
    "\n=== SILVER ==="
)

df_silver.show(
    truncate=False
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
# 8. METRIQUES PAR REGLE
# ============================================================

print(
    "\n=== METRIQUES DATA QUALITY ==="
)

df_metrics = (
    df_dq
    .agg(
        sum(
            "dq_invalid_event_id"
        ).alias(
            "invalid_event_id"
        ),

        sum(
            "dq_invalid_timestamp"
        ).alias(
            "invalid_timestamp"
        ),

        sum(
            "dq_missing_line"
        ).alias(
            "missing_line"
        ),

        sum(
            "dq_invalid_passenger_count"
        ).alias(
            "invalid_passenger_count"
        ),

        sum(
            "dq_negative_passenger_count"
        ).alias(
            "negative_passenger_count"
        ),
    )
)

df_metrics.show(
    truncate=False
)


# ============================================================
# 9. RECONCILIATION
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
    "Bronze      :",
    bronze_count,
)

print(
    "Silver      :",
    silver_count,
)

print(
    "Quarantaine :",
    quarantine_count,
)


# ============================================================
# 10. PIPELINE GATE ROBUSTE
# ============================================================

print(
    "\n=== PIPELINE GATE ==="
)


if bronze_count == 0:

    print(
        "FAIL : EMPTY_BRONZE"
    )

elif (
    bronze_count
    != silver_count
    + quarantine_count
):

    print(
        "FAIL : RECONCILIATION_ERROR"
    )

else:

    print(
        "PASS"
    )


spark.stop()