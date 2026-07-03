## lancement

Pour lancer le pipeline :
```bash
python3 lecture_donnes.py
```

La sortie  au format FHIr qui est  un objet JSON par ligne est
généree dans output/patient_fhir/. Le script est rejouable grace au mode overwrite.

L'un des premiers exercice Spark pour moi, j'ai pris PySpark plutôt que Scala car je suis beaucoup plus a l'aise en Python. J'ai essayé de faire des étapes séparées et lisibles plutôt que d'enchaîner les transformations

## Hypothèses / choix faits

Pour les IPP deprecie dans identifiants_ipp.csv , un ipp deprecie pointe vers
un ipp_principal qui est l'IPP actif. J'ai résolu ça avec une jointure de la
table sur elle-mêm, donc si  ipp_principal est vide je garde l'IPP lui-même (il y a aussi le cas d'un IPP déprécié sans cible par exemplee 700000099 qui existe dans identifiants_ipp mais nulle part ailleurs ).

Pour fusionner les deux versions d'un même patient (actif + déprécié), j'ai
 gardé la ligne active et viré la déprécié. Par manque de temps, je n'ai pas géré le cas où la version active aurait un champ vide que la version dépréciée avait
rempli je garde juste  la version active tel quelle. 

il y avait  doublon  pour  800000124 (a un espace pres dans  un prénom) je
l'ai réglé en nettoyant les espaces dans chaque prénom avant de dédupliquer,
sinon dropDuplicates() ne le voyait pas comme un doublon.

Pour le sexe, mapping vers male/female/unknown,  tout ce qui n'est pas reconnu (vide ou autre) est  en unknown.

Les dates avaient 4 formats différents dans le meme fichier. J'ai utilise
try_to_date avec coalesce pour essayer plusieurs format  jusqu'à ce qu'un marche. 

Pour adresses.csv  on retrouvait plusieurs adresses possibles par patient ,
donc là je n'ai pas filtré comme pour patient (une seule ligne par IPP
actif), j'ai gardé tout l'historique rattaché au bon ipp_trouve, et choisi
l'adresse actuelle en prenant la date_debut la plus récente par patient
(groupBy + max, puis un join pour retrouver la ligne complète qui
correspond à cette date maxi). Poru le cas du patient 800000127 qui a deux adresses
actuelle sans date_fin : j'ai pris celle avec la date_debut la plus
récente.

Pour opposition_recherche.csv, j'ai fais un filtrage dans opposition du patient dont
l'ipp ne correspond a personne (le 800000199 n'existe nulle part
ailleurs dans les autre fichiers). Pareil que pour le sexe des
patients, j'ai mis en majuscule les opposition des clients pour associer un
boolean en fonction de la valeur.
Quand la valeur est vide (par ex pour le patient 800000135) je laisse
NULL plutôt que de forcer à false car cest plys une  absence d'info qu'un refus 

## Construction du JSON FHIR

D'apres le lien fourni, j'ai pu faire  la structure de identifier/ name/address/deceased.
 
Pour identifier par defaut, j'ai mis "uri" comme valeur de system car je ne connaissais pas la valeur a mettre.

- nom_usuel n'est pas utilise dans la sortie finale, seul nom_naissance
est utilisé pour family. En relisant le script je me suis rendu compte que 
la colonne nom_usuel  était calculée mais jamais utilisée dans la construction
de name, je l'ai laissée de côté plutôt que d'ajouter un deuxième 
HumanName avec use=usual, par manque de temps.

-Pour address , dans json adresse vide représentée par une liste vide plutôt
qu'un objet avec des champs null, pour éviter d'implier une donnée
existante mais incomplète (cetait pour le patient 800000125 ).

deceased : Si on a une date de décès donc date_deces_fhir non vide,  on
utilise deceasedDateTime. Sinon deceasedBoolean = false, 
on suppose que il n'y a pas de date de décès
connue = vivant , j'avais vu que  FHIR interdit de remplir les deux en même temps.

- opposition_fhir : Le README ne détaille pas ce mécanisme sur la page
  Patient donnée, je suis allé voir ailleurs dans la doc FHIR  pour comprendre comment ca marche en général. 

## Format de sortie
J'ai pris Json car FHIR est un format JSON  (les
exemples de la doc HL7 sont en JSON), et le README demande un résultat
directement exploitable par une API ,  un fichier Parquet demanderait une
étape de conversion en plus avant de pouvoir être servi par une API REST,
alors que le JSON peut être utilisé tel quel. de plus je n'ai manipulé que des JSON jusque la


## Anomalies trouvées dans les CSV

- Virgule en trop à la fin du header de patients.csv → colonne "_c8" 
  vide, supprimée après lecture.
- La colonne prenoms contient un tableau JSON écrit en texte
  (["Marie","Claire"]), avec plusieurs  guillemets  dedans. Réglé avec l'option escape='"'
- Sexe écrit de 6-7 façons différentes 
- Formats de date incohérents
- Deux adresses "actuelle" pour le même patient (800000127) sans date_fin
  sur aucune des deux.resolu en prenant la date_debut la plus récente.
- Code postal de Lyon écrit sur 4 chiffres  pour le
  patient 800000134 je l'ai pas corrigé, laissé tel quel dans la sortie (a faire
  avec plus de temps)

- opposition_recherche avec plein de formats différents pour la même info
- 4 patients actifs (800000127, 800000132, 800000134, 800000136) sans
  aucune ligne dans opposition_recherche.csv , donc laissee avec opposition_fhir
  = NULL 
- IPP dans opposition_recherche.csv (800000199) qui n'existe nulle
  part ailleurs je lai retiré du résultat final.

## Avec plus de temps

- Corriger le code postal  (Lyon, 800000134)
- Resoudre le probreme des majuscule  des noms (MARTIN vs Martin, paris vs Paris)
- Ajouter un deuxième HumanName  pour utiliser  nom_usuel quand il est renseigné

