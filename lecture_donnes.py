
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json , trim, transform, upper , try_to_date , coalesce , max as s_max, struct, array, lit
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

#patients_prenoms_liste.show(truncate=False)
#application de trim pr chaque prenom
patients_prenoms_liste = patients_prenoms_liste.withColumn( "prenoms_liste", transform(col("prenoms_liste"), lambda prenom: trim(prenom))
)

#patients_prenoms_liste.show(truncate=False)


#retrait des doublons mtn que les cases avec espaces sont similaires 

patients_prenoms_liste = patients_prenoms_liste.drop("prenoms")

patients_sans_doublons = patients_prenoms_liste.drop_duplicates()
#patients_sans_doublons.select("ipp", "prenoms_liste").show(truncate=False)


# resolution du sexe des infividus en fonction selon les normes fhir, mise en majuscule avec upper pour pas lister variantes 

patients_sexe_normalise = patients_sans_doublons.withColumn("sexe_fhir", when(upper(trim(col("sexe"))).isin("M", "HOMME", "MALE", "1"), "male")
    .when(upper(trim(col("sexe"))).isin("F", "FEMME", "FEMALE", "2"), "female")
    .otherwise("unknown")
)

#patients_sexe_normalise.show()


#resolution du format des date de naissance selon les normes fhir
#utilisation de coalesce pour traiter les differents format de la table
patients_dates_naissance = patients_sexe_normalise.withColumn(
    "date_naissance_fhir",
    coalesce( try_to_date(col("date_naissance"),"yyyy-MM-dd"),
        try_to_date(col("date_naissance"),"dd/MM/yyyy"),
        try_to_date(col("date_naissance"),"dd-MM-yyyy"),
        try_to_date(col("date_naissance"),"yyyy/MM/dd")
    )
)
#patients_dates_naissance.show()

# Meme chose pour les date de deces
patients_dates = patients_dates_naissance.withColumn(
    "date_deces_fhir",
    coalesce( try_to_date(col("date_deces"),"yyyy-MM-dd"),
        try_to_date(col("date_deces"), "dd/MM/yyyy"),
        try_to_date(col("date_deces"),"dd-MM-yyyy"),
        try_to_date(col("date_deces"), "yyyy/MM/dd")
    )
)



patients_dates.show(truncate=False)

#regroupement des addresses sous le meme ipp_trouve pr trouver adresse actuelle

adresses_bon_ipp = df_adresses.join(  mapping_ipp,
    on="ipp",
    how="left"
)

adresses_bon_ipp.select("ipp", "ligne_adresse", "type_adresse", "date_debut", "date_fin", "ipp_trouve").show(truncate=False)

#resolution de la l'adresse  la plus recente 
# tri date la plus recente + doublons
derniere_date_patient = adresses_bon_ipp.groupBy("ipp_trouve").agg(
    s_max("date_debut").alias("date_debut_max")
)

#derniere_date_patient.show()

#join sur ipp trouve pour recopier la date max a chaque ligne d'un patient , INNER pour retirer ceux qui ont pas de correspodance
adresses_actuelles = adresses_bon_ipp.join(  derniere_date_patient, on="ipp_trouve",
    how="inner"
)


#filtre pour garder que les ligne ou la date de debut correspon a celle debut_max
adresses_actuelles = adresses_actuelles.filter(
    col("date_debut") == col("date_debut_max")
)

adresses_actuelles.show(truncate=False)



#Oposition

#jointure entre opposition et mapping_ipp
opposition_bon_ipp = df_opposition.join(mapping_ipp, on="ipp", how="left"
)
#opposition_bon_ipp.show()

#filtrage du patient qui n'a aucune IPP 

opposition_sans_null = opposition_bon_ipp.filter(col("ipp_trouve").isNotNull())
#opposition_sans_null.show()


#resolution du format de opposition selon la fhir
opposition_resolu  = opposition_sans_null.withColumn( "opposition_fhir", when(upper(trim(col("opposition"))).isin("O", "OUI", "TRUE", "OPPOSÉ"), True)
    .when(upper(trim(col("opposition"))).isin("N", "NON", "FALSE", "0"), False)
    .otherwise(None)
)

opposition_resolu = opposition_resolu.filter(col("ipp") == col("ipp_trouve"))

opposition_resolu.show()

#assemblage de la table des patients
 
patient_assemble = patients_dates.join( adresses_actuelles,
    on="ipp_trouve",
 how="left"
)
patient_assemble = patient_assemble.join( opposition_resolu,
    on="ipp_trouve",
    how="left"
)

patient_assemble.show(truncate=False)


#reduction adresse_actuelle et opposition_resolu avant jointure, on a vraiment besoin de joindre que ipp_trouve + les colonnes utiles

adresses_reduit = adresses_actuelles.select( "ipp_trouve",
    "ligne_adresse",
    "code_postal",
    "ville",
    "pays"
)

opposition_reduit = opposition_resolu.select(
    "ipp_trouve",
    "opposition_fhir"
)


patient_assemble = patients_dates.join(
    adresses_reduit,
    on="ipp_trouve",
    how="left"
)

patient_assemble = patient_assemble.join(
    opposition_reduit,
    on="ipp_trouve",
    how="left"
)


patient_final = patient_assemble.select( col("ipp_trouve").alias("ipp"),  "nom_naissance",
    "nom_usuel",
    "prenoms_liste",
    "sexe_fhir",
    "date_naissance_fhir",
    "date_deces_fhir",
    "ligne_adresse",
  "code_postal",
    "ville",
    "pays",
    "opposition_fhir"
)

patient_final.show(truncate=False)




####JSON FHIR

# a partir du lien fournis : 

patient_fhir = patient_final.withColumn(
    "identifier",
    array(
      struct( lit("uri").alias("system"),
            col("ipp").alias("value")
    )
    )
)


patient_fhir.select("ipp", "identifier").show(truncate=False)

patient_fhir = patient_fhir.withColumn(
    "name",
    array(  struct(
          col("nom_naissance").alias("family"),
            col("prenoms_liste").alias("given"),
            lit("official").alias("use")
        )
    )
)

patient_fhir.select("ipp", "name").show(truncate=False)


patient_fhir = patient_fhir.withColumn(
    "address",
    array(
        struct(
            array(col("ligne_adresse")).alias("line"),
            col("ville").alias("city"),
            col("code_postal").alias("postalCode"),
            col("pays").alias("country")
        )
    )
)

patient_fhir.select("ipp", "address").show(truncate=False)


spark.stop()
 