# Bloc 2 — Premier DataFrame PySpark
# Configuration explicite du Python utilisé par les workers Spark

import os
import sys

# Utiliser exactement le même Python pour le driver et les workers Spark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RATP-DataFrame-Basics")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


print("\n=== PYTHON UTILISÉ PAR SPARK ===")
print(sys.executable)


data = [
    (7001, "2026-10-14 07:30:00", "A", "Nation", "validation", 120),
    (7002, "2026-10-14 07:35:00", "B", "Gare du Nord", "validation", 175),
    (7003, "2026-10-14 07:40:00", "C", "Bastille", "incident", 0),
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


print("\n=== DATAFRAME RATP ===")
df.show(truncate=False)


print("\n=== SCHEMA ===")
df.printSchema()


print("\n=== NOMBRE DE LIGNES ===")
print(df.count())


spark.stop()
