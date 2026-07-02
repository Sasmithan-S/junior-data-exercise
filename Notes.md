## lancement

Pour lancer le pipeline :
```bash
python3 lecture_donnes.py
```

La sortie  au format FHIr qui est  un objet JSON par ligne est
généree dans output/patient_fhir/. Le script est rejouable grace au mode overwrite.

L'un des premiers exercice Spark pour moi, j'ai pris PySpark plutôt que Scala car je suis beaucoup plus a l'aise en Python. J'ai essayé de faire des étapes séparées et lisibles plutôt que d'enchaîner les transformations

## Hypothèses / choix faits

Pour les IPP deprecie dans identifiants_ipp.csv , un IPP déprécié pointe vers
un ipp_principal qui est l'IPP actif. J'ai résolu ça avec une jointure de la
table sur elle-mêm, donc si  ipp_principal est vide je garde l'IPP lui-même (cas
actif, il y a aussi le cas d'un IPP déprécié sans cible par exemplee 700000099 qui existe dans identifiants_ipp mais nulle part ailleurs ).

Pour fusionner les deux versions d'un même patient (actif + déprécié), j'ai
 gardé la ligne active et viré la déprécié. Par manque de temps, je n'ai pas géré le cas où la version active aurait un champ vide que la version dépréciée avait
rempli je garde juste la version active telle quelle. 

il y avait  doublon  pour  800000124 (a un espace pres dans  un prénom) je
l'ai réglé en nettoyant les espaces dans chaque prénom avant de dédupliquer,
sinon dropDuplicates() ne le voyait pas comme un doublon.

Pour le sexe, mapping vers male/female/unknown,  tout ce qui n'est pas reconnu (vide ou autre) est  en unknown.

Les dates avaient 4 formats différents dans le meme fichier. J'ai utilise
try_to_date avec coalesce pour essayer plusieurs format dans l'ordre
jusqu'à ce qu'un marche. 

Pour adresses.csv, il y avait plusieurs adresses possibles par patient (historique),
donc là je n'ai pas filtré comme pour patients (une seule ligne par IPP
actif), j'ai gardé tout l'historique rattaché au bon ipp_trouve, et choisi
l'adresse actuelle en prenant la date_debut la plus récente par patient
(groupBy + max, puis un join pour retrouver la ligne complète qui
correspond à cette date maxi). Poru le cas du patient 800000127 qui a deux adresses
actuelle sans date_fin : j'ai pris celle avec la date_debut la plus
récente.

Pour opposition_recherche.csv, j'ai fais un filtrage dans opposition du patient dont
l'ipp ne correspond a personne (800000199 n'existe nulle part
ailleurs dans les autres fichiers). Pareil que pour le sexe des
patients, j'ai mis en majuscule les opposition des clients pour associer un
boolean en fonction de la valeur.
Quand la valeur est vide (par ex pour le patient 800000135) je laisse
NULL plutôt que de forcer à false car une absence d'info n'est pas pareil
qu'un refus .

## Construction du JSON FHIR

Je me suis basé sur https://hl7.org/fhir/R4/patient.html (le lien donné
dans le README) pour la structure de identifier/name/address/deceased[x].

Pour identifier, j'ai mis "uri" comme valeur de system, c'est une valeur arbitraire 

- nom_usuel n'est pas exploité dans la sortie finale, seul nom_naissance
est utilisé pour family. En relisant le script je me suis rendu compte
que la colonne nom_usuel (présente pour certains patients par ex: 800000124
BERNARD/DUPONT) était calculée mais jamais utilisée dans la construction
de name, je l'ai laissée de côté plutôt que d'ajouter un deuxième 
HumanName avec use=usual, par manque de temps.

-Pour address , dans json adresse vide représentée par une liste vide plutôt
qu'un objet avec des champs null, pour éviter d'implier une donnée
existante mais incomplète (cetait pour le patient 800000125 ).
deceased[x] : Si on a une date de décès donc date_deces_fhir non vide,  on
utilise deceasedDateTime. Sinon deceasedBoolean = false, 
on suppose que il n'y a pas de date de décès
connue = vivant. FHIR interdit de remplir les deux en même temps.

- opposition_fhir : je l'ai laissé en colonne à part, pas intégré dans la
  structure Patient. Normalement en FHIR ça devrait passer par une
  extension (mécanisme standard pour ajouter un champ custom, avec un url
  et une valeur typée genre valueBoolean), mais je ne l'ai pas fait -
  j'ai identifié où ça devrait aller mais pas implémenté pour rester dans
  les delais. Le README ne détaille pas ce mécanisme sur la page
  Patient donnée, je suis allé voir ailleurs dans la doc FHIR
  extensibility.html pour comprendre comment ça marche en général.

## Format de sortie
J'ai pris Json car FHIR est un format JSON  (les
exemples de la doc HL7 sont en JSON), et le README demande un résultat
directement exploitable par une API ,  un fichier Parquet demanderait une
étape de conversion en plus avant de pouvoir être servi par une API REST,
alors que le JSON peut être utilisé tel quel.

Écriture avec mode("overwrite") pour que le job soit rejouable : si on
relance le script



## Anomalies trouvées dans les CSV

- Virgule en trop à la fin du header de patients.csv → colonne "_c8" 
  vide, supprimée après lecture.
- La colonne prenoms contient un tableau JSON écrit en texte
  (["Marie","Claire"]), avec plusieurs  guillemets  à l'intérieur. Par
  défaut Spark n'interprète pas ça correctement du coup ça
  coupait les lignes au mauvais endroit. Réglé avec l'option escape='"'
- Sexe écrit de 6-7 façons différentes 
- Formats de date incohérents
- Deux adresses "actuelle" pour le même patient (800000127) sans date_fin
  sur aucune des deux - résolu en prenant la date_debut la plus récente.
- Code postal de Lyon écrit sur 4 chiffres  pour le
  patient 800000134 je l'ai pas corrigé, laissé tel quel dans la sortie (à faire
  avec plus de temps)

- opposition_recherche avec plein de formats différents pour la même info
- 4 patients actifs (800000127, 800000132, 800000134, 800000136) sans
  aucune ligne dans opposition_recherche.csv , donc laissee avec opposition_fhir
  = NULL 
- IPP dans opposition_recherche.csv (800000199) qui n'existe nulle
  part ailleurs je lai retiré du résultat final.

## Avec plus de temps

- Gérer la fusion des champs entre version active/dépréciée au lieu de
  juste jeter la dépréciée
- Des vrais tests plutôt que des vérifications à l'oeil avec .show()
- Implémenter l'extension FHIR pour opposition_recherche au lieu de la
  laisser en colonne à part
- Corriger le code postal tronqué (Lyon, 800000134)
- Resoudre le probreme des majuscule  des noms (MARTIN vs Martin, paris vs Paris)
- Ajouter un deuxième HumanName (use=usual) pour utiliser  nom_usuel quand il est renseigné

