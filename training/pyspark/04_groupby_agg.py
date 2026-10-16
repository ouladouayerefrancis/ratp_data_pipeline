# Bloc 4 — groupBy() et agrégations avec PySpark
# Objectif :
# transformer des événements détaillés en indicateurs métier.

import os
import sys

# Configuration locale Windows :
# Driver et Workers utilisent le même interpréteur Python.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum as _sum,
    avg,
    min as _min,
    max as _max,
)


# -------------------------------------------------------
# 1. Création de la SparkSession
# -------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RATP-PySpark-GroupBy-Aggregations")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:

    # ---------------------------------------------------
    # 2. Création du DataFrame d'événements
    # ---------------------------------------------------

    data = [
        (1, "A", "Chatelet", "ENTRY", 120),
        (2, "A", "Nation", "ENTRY", 80),
        (3, "A", "Chatelet", "EXIT", 150),
        (4, "B", "Gare du Nord", "ENTRY", 200),
        (5, "B", "Gare du Nord", "EXIT", 180),
        (6, "B", "Republique", "ENTRY", 100),
    ]

    columns = [
        "event_id",
        "line",
        "station",
        "event_type",
        "passenger_count",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )


    print("\n=== DATAFRAME INITIAL ===")

    df.show(truncate=False)


    # ---------------------------------------------------
    # 3. Agrégation simple par ligne
    # ---------------------------------------------------

    print("\n=== PASSAGERS PAR LIGNE ===")

    passengers_by_line_df = (
        df
        .groupBy("line")
        .agg(
            _sum("passenger_count").alias("total_passengers")
        )
        .orderBy("line")
    )

    passengers_by_line_df.show()


    # ---------------------------------------------------
    # 4. Plusieurs agrégations en même temps
    # ---------------------------------------------------

    print("\n=== STATISTIQUES PAR LIGNE ===")

    statistics_by_line_df = (
        df
        .groupBy("line")
        .agg(
            count("*").alias("total_events"),
            _sum("passenger_count").alias("total_passengers"),
            avg("passenger_count").alias("average_passengers"),
            _min("passenger_count").alias("minimum_passengers"),
            _max("passenger_count").alias("maximum_passengers"),
        )
        .orderBy("line")
    )

    statistics_by_line_df.show(
        truncate=False
    )


    # ---------------------------------------------------
    # 5. Agrégation avec plusieurs clés
    # ---------------------------------------------------

    print(
        "\n=== PASSAGERS PAR LIGNE ET TYPE D'ÉVÉNEMENT ==="
    )

    passengers_by_line_event_df = (
        df
        .groupBy(
            "line",
            "event_type",
        )
        .agg(
            count("*").alias("total_events"),
            _sum("passenger_count").alias(
                "total_passengers"
            ),
        )
        .orderBy(
            "line",
            "event_type",
        )
    )

    passengers_by_line_event_df.show(
        truncate=False
    )


    # ---------------------------------------------------
    # 6. Agrégation par station
    # ---------------------------------------------------

    print("\n=== PASSAGERS PAR STATION ===")

    passengers_by_station_df = (
        df
        .groupBy(
            "line",
            "station",
        )
        .agg(
            _sum("passenger_count").alias(
                "total_passengers"
            )
        )
        .orderBy(
            "line",
            "station",
        )
    )

    passengers_by_station_df.show(
        truncate=False
    )


    # ---------------------------------------------------
    # 7. Observation du plan Spark
    # ---------------------------------------------------

    print("\n=== PLAN D'EXÉCUTION ===")

    passengers_by_line_df.explain(
        mode="formatted"
    )


finally:

    # ---------------------------------------------------
    # 8. Arrêt propre de Spark
    # ---------------------------------------------------

    spark.stop()

    print(
        "\nSparkSession arrêtée proprement."
    )
