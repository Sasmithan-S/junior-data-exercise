
from pyspark.sql import SparkSession
 
# On crée la session Spark, exactement comme dans test_spark.py.
spark = SparkSession.builder.appName("aphp-pipeline").getOrCreate()
 
# On lit le fichier patients.csv.
# header=True dit à Spark : "la première ligne du fichier, ce n'est pas de
# la donnée, ce sont les noms de colonnes". Sans ça, Spark inventerait des
# noms génériques (_c0, _c1, _c2...) et traiterait ta ligne d'en-tête comme
# si c'était un vrai patient.
patients = spark.read.csv("resources/patients.csv", header=True)
 
# .columns : liste simplement les noms de colonnes que Spark a détectés.
print(patients.columns)
 
# .show() : affiche le contenu sous forme de tableau dans le terminal.
patients.show()
 
spark.stop()
 