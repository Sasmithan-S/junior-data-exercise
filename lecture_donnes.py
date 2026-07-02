
from pyspark.sql import SparkSession
 

spark = SparkSession.builder.appName("aphp-pipeline").getOrCreate()
 
#Pour eviter que Spark s'arrête au
# premier "" qu'il trouve et coupe la ligne au mauvais endroit dès qu'il
# rencontre une virgule ensuite , on ajoute escape = ' "" '

patients = spark.read.csv("resources/patients.csv", header=True , escape='"')

#on supprime la colonne "_c8" du dataframe
patients = patients.drop("_c8")
print(patients.columns)
 
#  affichage  du  contenu 
patients.show()
 
spark.stop()
 