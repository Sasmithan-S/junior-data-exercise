
from pyspark.sql import SparkSession
 

spark = SparkSession.builder.appName("aphp-pipeline").getOrCreate()
 

#Pour eviter que Spark s'arrête au
# premier "" qu'il trouve et coupe la ligne au mauvais endroit dès qu'il
# rencontre une virgule ensuite , on ajoute escape = ' " '

df_patients = spark.read.csv("resources/patients.csv", header=True , escape='"')

#on supprime la colonne "_c8" du data
df_patients = df_patients.drop("_c8")

#Lecture des 3 fichiers 
df_identifiants_ipp = spark.read.csv("resources/identifiants_ipp.csv", header=True, escape='"')
df_adresses = spark.read.csv("resources/adresses.csv", header=True, escape='"')
df_opposition = spark.read.csv("resources/opposition_recherche.csv", header=True, escape='"') 

print(df_patients.columns)

df_patients.show()

print(df_adresses.columns)
df_adresses.show()

print(df_identifiants_ipp.columns)
df_identifiants_ipp.show()

print(df_opposition.columns)
df_opposition.show()


spark.stop()
 