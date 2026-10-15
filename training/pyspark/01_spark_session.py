# Bloc 1 — Première SparkSession

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RATP-PySpark-Training")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Spark démarré avec succès.")
print("Version Spark :", spark.version)

spark.stop()