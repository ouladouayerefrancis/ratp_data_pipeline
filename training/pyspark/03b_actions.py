# Bloc 3B — Actions Spark
# Objectif :
# comprendre ce qui déclenche réellement l'exécution d'un plan Spark.

import os
import sys

# -------------------------------------------------------
# 0. Configuration Python locale pour PySpark
# -------------------------------------------------------
# On force le Driver et les Workers Spark à utiliser
# exactement le même interpréteur Python que ce script.

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from pyspark.sql import SparkSession


# -------------------------------------------------------
# 1. Création de la SparkSession
# -------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RATP-PySpark-Actions")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


print("\n=== ENVIRONNEMENT PYTHON ===")
print("Python utilisé :", sys.executable)


try:

    # ---------------------------------------------------
    # 2. Création d'un petit DataFrame
    # ---------------------------------------------------

    data = [
        (1, "A", 120),
        (2, "A", 80),
        (3, "B", 200),
        (4, "B", 50),
    ]

    df = spark.createDataFrame(
        data,
        ["event_id", "line", "passenger_count"]
    )


    # ---------------------------------------------------
    # 3. ACTION : show()
    # ---------------------------------------------------

    print("\n=== DATAFRAME INITIAL ===")

    df.show()


    # ---------------------------------------------------
    # 4. TRANSFORMATION
    # ---------------------------------------------------

    filtered_df = df.filter(
        df.passenger_count >= 100
    )

    print("\n=== TRANSFORMATION CRÉÉE ===")

    print(
        "filter() définit une transformation."
    )

    print(
        "Spark prépare le plan grâce à la Lazy Evaluation."
    )


    # ---------------------------------------------------
    # 5. ACTION : show()
    # ---------------------------------------------------

    print("\n=== ACTION show() ===")

    filtered_df.show()


    # ---------------------------------------------------
    # 6. ACTION : count()
    # ---------------------------------------------------

    print("\n=== ACTION count() ===")

    number_of_rows = filtered_df.count()

    print(
        "Nombre de lignes :",
        number_of_rows
    )


    # ---------------------------------------------------
    # 7. ACTION : first()
    # ---------------------------------------------------

    print("\n=== ACTION first() ===")

    first_row = filtered_df.first()

    print(
        "Première ligne :",
        first_row
    )


    # ---------------------------------------------------
    # 8. ACTION : take()
    # ---------------------------------------------------

    print("\n=== ACTION take(2) ===")

    rows = filtered_df.take(2)

    print(rows)


    # ---------------------------------------------------
    # 9. ACTION : collect()
    # ---------------------------------------------------

    print("\n=== ACTION collect() ===")

    all_rows = filtered_df.collect()

    print(all_rows)


    # ---------------------------------------------------
    # 10. Risque de collect()
    # ---------------------------------------------------

    print("\n=== ATTENTION collect() ===")

    print(
        "collect() ramène toutes les lignes vers le Driver."
    )

    print(
        "Sur un très gros DataFrame, cela peut saturer "
        "la mémoire du Driver."
    )


finally:

    # ---------------------------------------------------
    # 11. Arrêt garanti
    # ---------------------------------------------------

    spark.stop()

    print("\nSparkSession arrêtée proprement.")