Ce code Python est destiné à être utilisé avec Apache Spark, une plateforme de traitement des données distribuée et évolutive. Il propose des fonctions utiles pour la gestion et l'adaptation des schémas de données dans les DataFrames Spark.

Fonctionnalités principales
get_field_type(field_name): Cette fonction détermine le type de données Spark approprié (tel que DoubleType, LongType, StringType, etc.) en fonction du suffixe du nom de champ. Par exemple, elle identifie automatiquement les types de données pour des noms de champ tels que $numberDecimal, $date, $oid, etc.
add_struct_field(df, struct_path, field_name, field_type): Permet d'ajouter un nouveau champ à une structure existante dans un DataFrame Spark ou de créer une nouvelle structure si nécessaire. Il prend en charge les chemins de structures imbriquées en créant ou en modifiant les sous-structures requises.
add_struct_fields_recursive(df, field_path, field_type): Cette fonction est récursive et permet de décomposer les chemins de champs structurés (par exemple, a.b.c) et d'ajouter chaque niveau de champ dans la structure, en utilisant add_struct_field pour chaque niveau d'imbrication.
unifySchema(df: DataFrame, columns: list) -> DataFrame: Cette fonction unifie le schéma d'un DataFrame en ajoutant des colonnes structurées et non structurées manquantes. Elle repose sur add_struct_fields_recursive pour gérer les champs structurés et ajoute directement les champs non structurés au DataFrame.
Utilisation
Ces fonctions peuvent être utilisées pour faciliter la gestion des schémas de données dans des projets Spark, en automatisant la détection et l'ajout de champs, en particulier dans des environnements où les schémas de données peuvent évoluer rapidement.


