# NOTES.md

j'ai pris PySpark plutôt que Scala. J'ai essayé de faire des étapes séparées et lisibles plutôt que d'enchaîner les
transformations, pour garder le fil de ce que fait chaque bloc.

## Hypothèses / choix faits

Pour les IPP dépréciés dans identifiants_ipp.csv, un IPP déprécié pointe vers
un ipp_principal qui est l'IPP actif. J'ai résolu ça avec une jointure de la
table sur elle-même. Si ipp_principal est vide je garde l'IPP lui-même (cas
actif, ou cas d'un IPP déprécié sans cible par ex 700000099 qui existe dans
identifiants_ipp mais nulle part ailleurs, doit être une  donnée en trop).
 
Pour fusionner les deux versions d'un même patient (actif + déprécié), j'ai
juste gardé la ligne active et retiré la déprécié. Je n'ai pas géré le cas où
la version active aurait un champ vide que la version dépréciée avait
rempli -je garde juste la version active telle quelle. À améliorer si
j'avais plus de temps

Le doublon exact sur 800000124 (juste un espace en trop dans un prénom) je
l'ai réglé en nettoyant les espaces dans chaque prénom avant de dédupliquer,
sinon dropDuplicates() ne le voyait pas comme un doublon 
Pour le sexe, mapping vers male/female/unknown (FHIR n'accepte que
male/female/other/unknown). Tout ce qui n'est pas reconnu (vide ou autre)
part en unknown, j'ai pas voulu deviner

Les dates avaient 4 formats différents dans le même fichier. J'ai utilisé
try_to_date avec coalesce pour essayer plusieurs formats dans l'ordre
jusqu'à ce qu'un marche. Point à noter : l'ordre entre yyyy-MM-dd et
dd-MM-yyyy peut être ambigu sur certaines dates (genre 05-03-1990), j'ai mis
le format ISO en premier mais c'est une hypothèse, pas une certitude à 100%

filtrage dans opposition du patient dont l'ipp ne correspond a personne
De la meme facon que pour le sexe des patients, j'ai mis en majuscule les opposition des clients pour associer un boolean aux choix

-> dans json adresse vide représentée par une liste vide plutôt qu'un objet avec des champs null, pour éviter d'implier une donnée existante mais incomplète

-> Si on a une date de décès (date_deces_fhir non vide) on utilise deceasedDateTime, plus précis, on ne le gâche pas.
Sinon -> deceasedBoolean = false, on suppose que il n'y a pas de date de décès connue = vivant 




Pour la table des patients assemblé, je fais une double jointure left a partir de patient_dates car il y a des patient sans adresse / sans oppositions.
## Anomalies  dans  CSV

 Virgule en trop à la fin du header de patients.csv colonne _c8
  vide, supprimée après lecture.
La colonne prenoms contient un tableau JSON écrit en texte
  (["Marie","Claire"]), avec des guillemets doublés à l'intérieur. Réglé avec  escape='"'.
 Sexe écrit de 6-7 façons différente.
Formats de date incohérents

dans opposition il y a un patient dont l'ipp ne correspond a personne 
