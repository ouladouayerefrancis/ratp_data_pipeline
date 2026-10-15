# Bloc 14 — Spark SQL et équivalence avec l'API DataFrame

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
    sum,
    count,
)


spark = (
    SparkSession.builder
    .appName("RATP-Spark-SQL")
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
# 1. DONNEES
# ============================================================

data = [
    (9001, "A", "Nation", "validation", 120),
    (9002, "A", "Bastille", "validation", 180),
    (9003, "B", "Gare du Nord", "validation", 150),
    (9004, "B", "Chatelet", "incident", 0),
    (9005, "C", "Nation", "validation", 80),
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


print("\n=== DATAFRAME SOURCE ===")

df.show(
    truncate=False
)


# ============================================================
# 2. CREER UNE VUE TEMPORAIRE SQL
# ============================================================

df.createOrReplaceTempView(
    "events"
)


# ============================================================
# 3. VERSION DATAFRAME API
# ============================================================

df_api = (
    df
    .filter(
        col("event_type") == "validation"
    )
    .groupBy(
        "line"
    )
    .agg(
        count("*").alias(
            "total_events"
        ),
        sum(
            "passenger_count"
        ).alias(
            "total_passengers"
        ),
    )
    .orderBy(
        "line"
    )
)


print(
    "\n=== VERSION DATAFRAME API ==="
)

df_api.show()


# ============================================================
# 4. VERSION SPARK SQL
# ============================================================

df_sql = spark.sql(
    """
    SELECT
        line,
        COUNT(*) AS total_events,
        SUM(passenger_count) AS total_passengers
    FROM events
    WHERE event_type = 'validation'
    GROUP BY line
    ORDER BY line
    """
)


print(
    "\n=== VERSION SPARK SQL ==="
)

df_sql.show()


# ============================================================
# 5. VERIFIER QUE LES RESULTATS SONT IDENTIQUES
# ============================================================

api_rows = df_api.collect()
sql_rows = df_sql.collect()


print(
    "\n=== COMPARAISON ==="
)

if api_rows == sql_rows:

    print(
        "DataFrame API = Spark SQL : PASS"
    )

else:

    print(
        "DataFrame API = Spark SQL : FAIL"
    )


# ============================================================
# 6. PLAN D'EXECUTION SQL
# ============================================================

print(
    "\n=== PLAN D'EXECUTION SPARK SQL ==="
)

df_sql.explain(
    mode="formatted"
)


spark.stop()