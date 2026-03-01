"""
TP4 — Big Data (PySpark)
Analyse du dataset StudentsPerformance.csv

Fichier attendu : StudentsPerformance.csv (dans le même dossier).
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

CSV_FILE = "StudentsPerformance.csv"

# -----------------------------
# 1) Création du SparkSession
# -----------------------------
spark = (
    SparkSession.builder
    .appName("TP_Students")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

print("✅ SparkSession OK")

# -----------------------------
# 2) Chargement du dataset
# -----------------------------
if not os.path.exists(CSV_FILE):
    raise FileNotFoundError(
        f"❌ Fichier '{CSV_FILE}' introuvable. "
        "Télécharge le dataset Kaggle et mets StudentsPerformance.csv dans le même dossier."
    )

df = (
    spark.read.csv(
        CSV_FILE,
        header=True,
        inferSchema=True
    )
)

print("\n--- Aperçu (show) ---")
df.show(5, truncate=False)

print("\n--- Schéma ---")
df.printSchema()

print("\nNombre de lignes :", df.count())

# -----------------------------
# 3) Affichage propre (Pandas)
# -----------------------------
print("\n--- Tableau Pandas (10 premières lignes) ---")
try:
    print(df.toPandas().head(10))
except Exception as e:
    print("⚠️ Pandas non disponible ou erreur d'affichage:", e)

# -----------------------------
# 4) Exploration rapide
# -----------------------------
print("\n--- Describe (moy/min/max/std) ---")
try:
    print(df.describe().toPandas())
except Exception as e:
    print("⚠️ Impossible d'afficher describe en Pandas:", e)
    df.describe().show()

# -----------------------------
# 5) Nettoyage : suppression des lignes nulles
# -----------------------------
df_clean = df.na.drop()
print("\nAprès nettoyage (na.drop) :", df_clean.count())

# -----------------------------
# 6) Colonne moyenne générale
# average_score = (math + reading + writing) / 3
# -----------------------------
df2 = df_clean.withColumn(
    "average_score",
    (col("math score") + col("reading score") + col("writing score")) / 3
)

print("\n--- Avec average_score (5 lignes) ---")
df2.select("gender", "math score", "reading score", "writing score", "average_score").show(5)

# -----------------------------
# 7) Analyses principales (groupBy)
# -----------------------------

# 7.1 Moyenne des notes par genre
print("\n=== 1) Moyenne des notes par genre ===")
result_gender = (
    df2.groupBy("gender")
    .avg("math score", "reading score", "writing score", "average_score")
)
result_gender.show()

# 7.2 Moyenne générale par type de repas (lunch)
print("\n=== 2) Moyenne générale par type de repas ===")
result_lunch = (
    df2.groupBy("lunch")
    .avg("average_score")
    .orderBy(col("avg(average_score)").desc())
)
result_lunch.show()

# 7.3 Score selon l’éducation des parents
print("\n=== 3) Score selon l’éducation des parents ===")
result_parent = (
    df2.groupBy("parental level of education")
    .avg("average_score")
    .orderBy(col("avg(average_score)").desc())
)
result_parent.show(truncate=False)

# -----------------------------
# 8) Spark SQL
# -----------------------------
df2.createOrReplaceTempView("students")

print("\n=== SQL 1) Top 10 meilleurs élèves ===")
spark.sql("""
SELECT gender,
       `math score`,
       `reading score`,
       `writing score`,
       average_score
FROM students
ORDER BY average_score DESC
LIMIT 10
""").show(truncate=False)

print("\n=== SQL 2) Score moyen par groupe ethnique ===")
spark.sql("""
SELECT `race/ethnicity`,
       AVG(average_score) AS avg_score
FROM students
GROUP BY `race/ethnicity`
ORDER BY avg_score DESC
""").show(truncate=False)

print("\n=== SQL 3) Impact du cours de préparation ===")
spark.sql("""
SELECT `test preparation course`,
       AVG(average_score) AS avg_score
FROM students
GROUP BY `test preparation course`
ORDER BY avg_score DESC
""").show(truncate=False)

# -----------------------------
# 9) Bonus : colonne success/fail
# Seuil proposé par l’énoncé : average_score >= 70
# -----------------------------
df3 = df2.withColumn(
    "success",
    when(col("average_score") >= 70, "success").otherwise("fail")
)

print("\n=== Bonus : Comptage success/fail ===")
df3.groupBy("success").count().show()

spark.stop()
print("\n✅ Fin du TP4")
