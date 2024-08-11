from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ArbresRemarquables").getOrCreate()

df = spark.read.option("delimiter", ";").csv("file:///root/arbresremarquablesparis.csv", header=False)

columns = [
    "GPS", "col2", "col3", "Arrondissement", "Famille", "Annee_Plantation",
    "Adresse", "Circonference", "Hauteur", "col10", "col11", "Genre", "Espece", "col14",
    "col15", "col16", "col17", "col18", "col19", "col20", "col21", "col22", "col23",
    "col24", "col25", "col26", "col27", "col28", "col29", "col30", "col31", "col32",
    "col33", "col34", "col35", "col36"
]
df = df.toDF(*columns)

df_filtered = df.filter(df.Hauteur != '').withColumn("Hauteur", col("Hauteur").cast("float"))
arbre_plus_grand = df_filtered.orderBy(desc("Hauteur")).first()
print(f"Coordonnées GPS : {arbre_plus_grand['GPS']}")
print(f"Hauteur : {arbre_plus_grand['Hauteur']} m")
print(f"Adresse : {arbre_plus_grand['Adresse']}")

df_filtered_Circ = df_filtered.filter(df.Circonference != '').withColumn("Circonference", col("Circonference").cast("float"))
window = Window.partitionBy("Arrondissement").orderBy(desc("Circonference"))
df_with_rank = df_filtered_Circ.withColumn("rank", row_number().over(window))
df_top_circonference = df_with_rank.filter(col("rank") == 1).select("Adresse", "GPS","Arrondissement", "Hauteur", "Circonference")
df_top_circonference.show(truncate=False)

df_especes = df_filtered.select("Genre", "Espece").distinct().orderBy("Genre", "Espece")

for row in df_especes.collect():
    print(row.asDict())
result = df

spark.stop()