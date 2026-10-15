# Bloc 15 — Parquet partitionné et optimisations de lecture

import os
import sys
from datetime import date

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop-3.3.4"
os.environ["PATH"] = (
    r"C:\hadoop-3.3.4\bin;"
    + os.environ["PATH"]
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DateType,
)


# ============================================================
# 1. SPARKSESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("RATP-Parquet-Optimizations")
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
# 2. SCHEMA
# ============================================================

schema = StructType([
    StructField(
        "event_id",
        LongType(),
        False,
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
        LongType(),
        True,
    ),
    StructField(
        "event_date",
        DateType(),
        False,
    ),
])


# ============================================================
# 3. DONNEES DE PLUSIEURS JOURS
# ============================================================

data = [
    (
        10001,
        "A",
        "Nation",
        "validation",
        120,
        date(2026, 10, 13),
    ),
    (
        10002,
        "B",
        "Bastille",
        "validation",
        170,
        date(2026, 10, 13),
    ),
    (
        10003,
        "A",
        "Nation",
        "validation",
        140,
        date(2026, 10, 14),
    ),
    (
        10004,
        "B",
        "Gare du Nord",
        "validation",
        90,
        date(2026, 10, 14),
    ),
    (
        10005,
        "C",
        "Chatelet",
        "incident",
        0,
        date(2026, 10, 14),
    ),
    (
        10006,
        "A",
        "Bastille",
        "validation",
        200,
        date(2026, 10, 15),
    ),
]


df = spark.createDataFrame(
    data,
    schema,
)


print(
    "\n=== DATAFRAME SOURCE ==="
)

df.show(
    truncate=False
)


# ============================================================
# 4. ECRITURE PARQUET PARTITIONNEE
# ============================================================

output_path = (
    "training/pyspark/output/"
    "events_partitioned"
)


(
    df.write
    .mode("overwrite")
    .partitionBy(
        "event_date"
    )
    .parquet(
        output_path
    )
)


print(
    "\n=== PARQUET PARTITIONNE ECRIT ==="
)

print(
    output_path
)


# ============================================================
# 5. RELECTURE
# ============================================================

df_parquet = (
    spark.read
    .parquet(
        output_path
    )
)


print(
    "\n=== PARQUET RELU ==="
)

df_parquet.show(
    truncate=False
)


print(
    "\n=== SCHEMA PARQUET ==="
)

df_parquet.printSchema()


# ============================================================
# 6. REQUETE OPTIMISEE
#
# Nous ne demandons que 4 colonnes
# et seulement une date.
# ============================================================

df_filtered = (
    df_parquet

    .select(
        "event_id",
        "line",
        "passenger_count",
        "event_date",
    )

    .filter(
        (
            col("event_date")
            == lit("2026-10-14")
        )
        &
        (
            col("passenger_count")
            >= 100
        )
    )
)


print(
    "\n=== RESULTAT FILTRE ==="
)

df_filtered.show(
    truncate=False
)


# ============================================================
# 7. PLAN D'EXECUTION
# ============================================================

print(
    "\n=== PLAN D'EXECUTION ==="
)

df_filtered.explain(
    mode="formatted"
)


spark.stop()