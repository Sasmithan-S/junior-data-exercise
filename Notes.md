# Hypothèses et arbitrages
- Résolution des IPP dépréciés: un IPP est "résolu" vers
  `ipp_principal` s'il est renseigné, sinon vers son propre `ipp` (cas des IPP
  actifs, ou des IPP dépréciés  sans cible connue). Une seul ligne
  par patient réel est conservé : celle dont `ipp == ipp_trouve` 
  (du coup la version "active"). Les champs de la version dépréciée ne sont pas
  fusionnés avec ceux de la version active — seule la version active est
  gardée. Limite assumée : si la version active avait un champ vide que la
  version dépréciée avait rempli, cette information serait perdue (non traité
  ici, cf. section "avec plus de temps").

- Dédoublonnage exact: après nettoyage des espace  dans les
  prénoms (trim appliqué à chaque élément de la liste), les lignes
   pareil sur toutes les colonnes sont dédupliquées
  (dropDuplicates(), sans argument, pour ne fusionner que des lignes
  réellement identiques et ne pas choisir  entre deux versions
  différentes).

- Sexe selon la fhir : mapping vers les 4 valeurs autorisées par FHIR
  R4 (`male`, `female`, `other`, `unknown`). Toute valeur non reconnue
  (vide, ou format imprévu) est mappée vers `unknown` plutôt que de deviner
  ou de faire planter le pipeline.

 # Anomalies détectées et leur traitement

- Colonne _c8 dans patients.csv: une virgule e, trop dans la ligne
  d'en-tête crée une 9ème colonne sans nom, toujours vide. Supprimée après
  lecture (.drop("_c8")).

- Parsing CSV des champs contenant des virgules entre 
  (colonne `prenoms`, tableau JSON en texte du type `["Marie","Claire"]`) :
  l'option Spark par défaut pour l'échappement des guillemets (`\`) ne
  correspond pas au format utilisé dans le fichier (`""`), Corrigé en spécifiant `escape='"'` à la lecture.

- IPP dépréciés multi-formats : les mêmes informations patient existent
  en double (IPP actif / IPP déprécié), avec des divergences de casse
  (`MARTIN` / `Martin`), d'espaces parasites, de format de sexe (`M` / `1`),
  et de format de date. Résolues via une auto-jointure sur
  `identifiants_ipp.csv` (cf. hypothèses ci-dessus).

- IPP déprécié orphelin (`700000099`) : marqué `DEPRECIE` mais sans
  `ipp_principal` renseigné, et absent de `patients.csv`. Il ne génère donc
  aucune ligne patient

- Doublon exact avec espace parasite (800000124) : deux lignes
  identiques à un espace près dans le prnom Marie.
  Réglé par  avec un `trim` sur chaque élément de la liste de prénoms
  avant dédoublonnage.

- Formats de sexe : `M`, `F`, `1`, `2`, `Homme`, `Femme`,
  `male`, valeur vide — 8 variantes pour un champ à 2 valeurs utiles.
  Normalisées vers `male`/`female`/`unknown` (cf. hypothèses).

