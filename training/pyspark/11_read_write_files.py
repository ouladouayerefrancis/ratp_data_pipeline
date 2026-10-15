# Bloc 11 — Lecture et écriture de fichiers avec PySpark
# CSV → schéma explicite → Parquet

import os
import sys

# ============================================================
# 0. CONFIGURATION PYTHON POUR PYSPARK SOUS WINDOWS
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from pyspark.sql import SparkSession

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)


# ============================================================
# 1. CREER LA SPARKSESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("RATP-PySpark-Files")
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
# 2. CHEMINS
# ============================================================

raw_path = (
    "data/raw/"
    "ratp_events_20261013.csv"
)

parquet_path = (
    "training/pyspark/output/"
    "ratp_events_20261013_parquet"
)


# ============================================================
# 3. LECTURE CSV AVEC inferSchema
# ============================================================

print(
    "\n=== CSV AVEC inferSchema ==="
)

df_inferred = (
    spark.read
    .option(
        "header",
        True,
    )
    .option(
        "inferSchema",
        True,
    )
    .csv(
        raw_path
    )
)

df_inferred.show(
    truncate=False
)


print(
    "\n=== SCHEMA INFERE PAR SPARK ==="
)

df_inferred.printSchema()


# ============================================================
# 4. DEFINIR UN SCHEMA EXPLICITE BRONZE
# ============================================================

bronze_schema = StructType([
    StructField(
        "event_id",
        LongType(),
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
        LongType(),
        True,
    ),
])


# ============================================================
# 5. LECTURE CSV AVEC SCHEMA EXPLICITE
# ============================================================

print(
    "\n=== CSV AVEC SCHEMA EXPLICITE ==="
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

df_bronze.show(
    truncate=False
)


print(
    "\n=== SCHEMA BRONZE EXPLICITE ==="
)

df_bronze.printSchema()


# ============================================================
# 6. CONTROLE DU NOMBRE DE LIGNES
# ============================================================

bronze_count = (
    df_bronze.count()
)

print(
    "\nNombre de lignes Bronze :",
    bronze_count,
)


# ============================================================
# 7. CREER LE REPERTOIRE DE SORTIE
# ============================================================

os.makedirs(
    "training/pyspark/output",
    exist_ok=True,
)


# ============================================================
# 8. ECRITURE EN PARQUET
# ============================================================

(
    df_bronze
    .write
    .mode("overwrite")
    .parquet(
        parquet_path
    )
)

print(
    "\n=== PARQUET ECRIT ==="
)

print(
    parquet_path
)


# ============================================================
# 9. RELIRE LE PARQUET
# ============================================================

df_parquet = (
    spark.read
    .parquet(
        parquet_path
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
# 10. RECONCILIATION CSV / PARQUET
# ============================================================

parquet_count = (
    df_parquet.count()
)


print(
    "\n=== RECONCILIATION CSV / PARQUET ==="
)

print(
    "CSV     :",
    bronze_count,
)

print(
    "Parquet :",
    parquet_count,
)


if bronze_count == parquet_count:
    print(
        "Réconciliation : PASS"
    )
else:
    print(
        "Réconciliation : FAIL"
    )


spark.stop()