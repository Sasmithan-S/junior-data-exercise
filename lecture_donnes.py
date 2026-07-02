
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json , trim, transform, upper
from pyspark.sql.types import ArrayType, StringType


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

#df_patients.show()

print(df_adresses.columns)
#df_adresses.show()

print(df_identifiants_ipp.columns)
#df_identifiants_ipp.show()

print(df_opposition.columns)
#df_opposition.show()

#join de la table df_indentifiants avec elle même

a = df_identifiants_ipp.alias("a")
b = df_identifiants_ipp.alias("b")

resolution_ipp = a.join( b ,  on= col("a.ipp_principal") == col("b.ipp"), how = "left")

resolution_ipp = resolution_ipp.withColumn( "ipp_trouve", when(col("a.ipp_principal").isNotNull(), col("a.ipp_principal"))
    .otherwise(col("a.ipp"))
)



mapping_ipp = resolution_ipp.select( col("a.ipp").alias("ipp"),
    col("ipp_trouve")
)



# jointure entre patients et mapping pr avoir 1 ligne par ipp actif


patients_bon_ipp = df_patients.join(
    mapping_ipp,
    on = "ipp",
    how = "left"
)


patients_sans_doublons_ipp = patients_bon_ipp.filter( col("ipp") == col("ipp_trouve"))





#schema pour liste de txt prenom
schema_prenoms = ArrayType(StringType())

#conversion prenoms en listes spark
patients_prenoms_liste = patients_sans_doublons_ipp.withColumn(
    "prenoms_liste",
    from_json(col("prenoms"), schema_prenoms)
)

patients_prenoms_liste.select("ipp", "prenoms", "prenoms_liste").show(truncate=False)
#application de trim pr chaque prenom
patients_prenoms_liste = patients_prenoms_liste.withColumn( "prenoms_liste", transform(col("prenoms_liste"), lambda prenom: trim(prenom))
)

patients_prenoms_liste.select("ipp", "prenoms_liste").show(truncate=False)


#retrait des doublons mtn que les cases avec espaces sont similaires 

patients_sans_doublons = patients_prenoms_liste.drop_duplicates()
patients_sans_doublons.select("ipp", "prenoms_liste").show(truncate=False)


# resolution du sexe des infividus en fonction selon les normes fhir, mise en majuscule avec upper pour pas lister variantes 

patients_sexe_normalise = patients_sans_doublons.withColumn("sexe_fhir", when(upper(trim(col("sexe"))).isin("M", "HOMME", "MALE", "1"), "male")
    .when(upper(trim(col("sexe"))).isin("F", "FEMME", "FEMALE", "2"), "female")
    .otherwise("unknown")
)

patients_sexe_normalise.select("ipp", "sexe", "sexe_fhir").show()

spark.stop()
 