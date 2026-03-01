# TP4 — Analyse du dataset *Students Performance* (PySpark) — Correction

Référence : **TP 4.pdf** fileciteturn9file1

Dataset : *Students Performance in Exams* (Kaggle) — fichier : `StudentsPerformance.csv` (1000 lignes) fileciteturn9file1

---

## ✅ Objectifs attendus
- Créer un **SparkSession**
- Charger un **CSV** (header + inferschema)
- Afficher les données (Spark + tableau Pandas)
- Nettoyer (`na.drop`)
- Ajouter des colonnes dérivées (moyenne générale)
- Analyses `groupBy`
- Requêtes **Spark SQL**
- Bonus : colonne `success/fail` fileciteturn9file1

---

## 1) Pré-requis
- PySpark stable (ex : 3.5.x)
- Java 11 (selon TP)
- Mettre `StudentsPerformance.csv` dans le **même dossier** que le script `tp4_analysis.py` fileciteturn9file1

Installation :
```bash
pip install pyspark pandas
```

---

## 2) Exécution
```bash
python tp4_analysis.py
```

> Le script affiche des tableaux (via `.toPandas()`) si Pandas est installé.
