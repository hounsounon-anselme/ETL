# Documentation du Projet

## Description

Ce projet implémente deux classes principales, `TableGenerator` et `DataInserter`, qui permettent de parcourir récursivement des répertoires, d'analyser les fichiers et de manipuler une base de données SQL Server pour créer des tables et insérer les données correspondantes.

---

## Fonctionnalités Principales

### 1. Classe `TableGenerator`

- **Objectif :**
  Crée automatiquement des tables dans une base de données SQL Server en fonction des noms des dossiers contenant les fichiers.

- **Fonctionnement :**

  - Parcourt récursivement un répertoire racine.
  - Pour chaque dossier détecté, génère un nom de table correspondant (avec normalisation pour SQL Server).
  - Crée les tables si elles n'existent pas déjà.

- **Avantages :**
  - Automatisation de la création des tables.
  - Normalisation des noms des tables pour respecter les conventions SQL Server.

### 2. Classe `DataInserter`

- **Objectif :**
  Insère automatiquement les données des fichiers dans les tables correspondantes.

- **Fonctionnement :**

  - Parcourt récursivement un répertoire racine.
  - Lit les fichiers contenus dans chaque dossier (formats pris en charge : `.xlsx`, `.xls`, `.csv`).
  - Identifie la table correspondante en fonction du nom du dossier.
  - Insère les données dans la table en respectant la structure des colonnes existantes.
  - Supprime les fichiers traités après une insertion réussie.

- **Avantages :**
  - Gestion automatique des données sans intervention manuelle.
  - Supprime les fichiers traités pour éviter les doublons ou erreurs futures.

---

## Structure du Projet

- **`root_directory` :** Répertoire racine contenant les dossiers et fichiers à analyser.
- **`TableGenerator` :** Classe pour la création des tables.
- **`DataInserter` :** Classe pour l’insertion des données.

---

### Configuration de Base

1. Définir le chemin du répertoire racine contenant les fichiers.
2. Configurer les paramètres de connexion à SQL Server :
   ```python
   root_directory = '/home/anselme/ETL/SOURCE'
    db_params = {
        "driver": "ODBC Driver 17 for SQL Server",
        "server": "localhost",
        "database": "master",
        "user": "sa",
        "password": "1234"
    }
   ```

---

## Prérequis

- Python 3.8 ou supérieur.
- Bibliothèques Python :
  - `os`
  - `pandas`
  - `pyodbc`
  - `re`
- Base de données SQL Server.
- Droits d’accès en lecture et écriture sur le système de fichiers.

---

## Installation du projet (Sous Windows (cmd) :

- Naviger dans le dossier du projet (ETL)
- Créer un environnement:

```
 python -m venv env_etl
```

- Activer l'environnement

```
env_etl\Scripts\activate
```

- Installer les dépendances

```
pip install -r requirements.txt
```

- Tester la connexion à SQL server

```
python test_conn.py
```

- Si connexion reussi créer les tables

```
python TableGenerator.py
```

- Si table créée, inserer les données:

```
python DataInsert.py
```

## Points Importants

- Les noms des tables sont normalisés pour éviter les conflits avec SQL Server (tout en minuscules, espaces remplacés par des underscores, etc.).
- Le traitement ne prend en charge que les fichiers `.csv`, `.xls`, et `.xlsx`.
- Les fichiers sont supprimés après une insertion réussie pour éviter les redondances.

---

## Limitations Connues

- Les fichiers contenant un nombre de colonnes différent de celui des tables correspondantes génèrent une erreur.
- Les fichiers avec des formats non pris en charge sont ignorés.

---

## Auteurs

- **Anselme HOUNSOUNON** - Développeur principal du projet.
