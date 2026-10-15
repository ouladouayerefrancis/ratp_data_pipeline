# Bloc 9 — Lazy Evaluation, explain() et cache()

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("RATP-Lazy-Evaluation")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


data = [
    (7001, "A", 120),
    (7002, "B", 175),
    (7003, "C", 0),
    (7004, "A", 220),
    (7005, "B", 190),
    (7006, "C", 25),
]

columns = [
    "event_id",
    "line",
    "passenger_count",
]

df = spark.createDataFrame(
    data,
    columns,
)


print("\n=== NOMBRE DE PARTITIONS INITIAL ===")
print(df.rdd.getNumPartitions())


# ============================================================
# 1. TRANSFORMATIONS
# ============================================================

df_high_traffic = (
    df
    .filter(
        col("passenger_count") >= 150
    )
    .select(
        "event_id",
        "line",
        "passenger_count",
    )
)


print("\n=== TRANSFORMATIONS DEFINIES ===")
print("Aucune action Spark n'a encore demandé les données.")


# ============================================================
# 2. PLAN D'EXECUTION
# ============================================================

print("\n=== PLAN SPARK ===")

df_high_traffic.explain(
    mode="formatted"
)


# ============================================================
# 3. CACHE
# ============================================================

print("\n=== MISE EN CACHE ===")

df_high_traffic.cache()


# Première action :
# Spark calcule réellement les données
# et remplit le cache.

print("\n=== PREMIERE ACTION : count() ===")

count_1 = df_high_traffic.count()

print(
    "Nombre de lignes :",
    count_1,
)


# Deuxième action :
# Spark peut réutiliser les données en cache.

print("\n=== DEUXIEME ACTION : show() ===")

df_high_traffic.show()


# Troisième action :
# réutilisation du cache

print("\n=== TROISIEME ACTION : count() ===")

count_2 = df_high_traffic.count()

print(
    "Nombre de lignes :",
    count_2,
)


# ============================================================
# 4. LIBERER LE CACHE
# ============================================================

df_high_traffic.unpersist()

print("\nCache libéré.")


spark.stop()