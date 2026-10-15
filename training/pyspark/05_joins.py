# Bloc 5 — JOIN PySpark
# inner, left, left_anti

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RATP-PySpark-Joins")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Table A : événements
events_data = [
    (7001, "A", "Nation", 120),
    (7002, "B", "Gare du Nord", 175),
    (7003, "C", "Bastille", 0),
    (7004, "D", "Inconnue", 90),
]

events_columns = [
    "event_id",
    "line",
    "station",
    "passenger_count",
]

df_events = spark.createDataFrame(
    events_data,
    events_columns,
)


# Table B : référentiel des lignes
lines_data = [
    ("A", "RER A", "RER"),
    ("B", "RER B", "RER"),
    ("C", "Metro C", "METRO"),
]

lines_columns = [
    "line",
    "line_name",
    "transport_type",
]

df_lines = spark.createDataFrame(
    lines_data,
    lines_columns,
)


print("\n=== EVENEMENTS A ===")
df_events.show()


print("\n=== REFERENTIEL B ===")
df_lines.show()


# INNER JOIN = A ∩ B
print("\n=== INNER JOIN ===")

df_inner = df_events.join(
    df_lines,
    on="line",
    how="inner",
)

df_inner.show()


# LEFT JOIN = tout A + B si correspondance
print("\n=== LEFT JOIN ===")

df_left = df_events.join(
    df_lines,
    on="line",
    how="left",
)

df_left.show()


# LEFT ANTI JOIN = A \ B
print("\n=== LEFT ANTI JOIN ===")

df_left_anti = df_events.join(
    df_lines,
    on="line",
    how="left_anti",
)

df_left_anti.show()


spark.stop()