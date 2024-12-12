import os
import pandas as pd
import pyodbc
import re
import csv




class DataInserter:
    def __init__(self):
        """
        Initialisation de la classe avec un répertoire racine et les paramètres de la base de données.
        :param root_dir: Répertoire principal à scanner.
        :param db_params: Dictionnaire contenant les paramètres de connexion à SQL Server.
        """
        self.root_dir = os.getenv("ROOT_DIR")
        self.db_params = {
            "Driver": os.getenv("DB_DRIVER"),
            "Server": os.getenv("DB_SERVER"),
            "Database": os.getenv("DB_DATABASE"),
            "UID": os.getenv("DB_USER"),
            "PWD": os.getenv("DB_PASSWORD"),
        }



    def scan_and_insert(self):
        """
        Parcourt récursivement les dossiers et traite ceux ne contenant pas `emails_attachments`.
        """
        for dirpath, dirnames, filenames in os.walk(self.root_dir):
            dirnames[:] = [d for d in dirnames if d != "emails_attachments"]

            for dirname in dirnames:
                folder_path = os.path.join(dirpath, dirname)
                normalized_name = self.normalize_table_name(dirname)
                self.process_folder(folder_path, normalized_name)

    def process_folder(self, folder_path, table_name):
        """
        Cette methode traite un dossier, détecte les fichiers, et insère les données dans la table correspondante.
        :param folder_path: Chemin du dossier.
        :param table_name: Nom de la table dans laquelle les données seront insérées.
        """
        files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        if not files:
            print(f"Aucun fichier trouvé dans {folder_path}")
            return

        for file in files:
            file_path = os.path.join(folder_path, file)
            print(f"Traitement du fichier : {file_path}")

            try:
                if file.endswith((".xlsx", ".xls")):
                    df = pd.read_excel(file_path, header=None)
                elif file.endswith(".csv"):
                    #df = pd.read_csv(file_path, header=None)
                    with open(file_path, 'r') as file_csv:
                        dialect = csv.Sniffer().sniff(file_csv.read(1024))
                        file_csv.seek(0)
                        df = pd.read_csv(file_csv, sep=dialect.delimiter)  
                else:
                    print(f"Format non supporté pour le fichier : {file_path}")
                    continue

                success = self.insert_data_into_table(table_name, df)

                if success:
                    self.delete_file(file_path)

            except Exception as e:
                print(f"Erreur lors de la lecture ou du traitement du fichier {file_path}: {e}")

    def insert_data_into_table(self, table_name, df):
        """
        Insère les données dans une table SQL Server existante sans inclure la colonne 'id'.
        :param table_name: Nom de la table.
        :param df: DataFrame contenant les données à insérer.
        """
        try:
            # Connexion à la base de données
            conn = pyodbc.connect(**self.db_params)
            cur = conn.cursor()

            # Récupérer les colonnes de la table
            query = f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ? AND COLUMN_NAME != 'id'
                ORDER BY ORDINAL_POSITION;
            """
            cur.execute(query, table_name)
            columns = [row[0] for row in cur.fetchall()]
            print(f"Colonnes détectées dans la table '{table_name}': {columns}")

            # Vérification du nombre de colonnes
            if len(columns) != len(df.columns):
                raise ValueError(f"Le nombre de colonnes du DataFrame ({len(df.columns)}) "
                                f"ne correspond pas à celui de la table ({len(columns)}).")

            # Construire dynamiquement la requête SQL d'insertion
            placeholders = ", ".join(["?"] * len(columns))
            columns_str = ", ".join([f"[{col}]" for col in columns])  # Gérer les noms de colonnes
            insert_query = f'INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});'

            print(f"Requête générée : {insert_query}")

            # Insérer les données ligne par ligne
            for row in df.itertuples(index=False):
                cur.execute(insert_query, row)

            conn.commit()
            print(f"Données insérées avec succès dans la table '{table_name}'.")
            return True  

        except Exception as e:
            print(f"Erreur lors de l'insertion des données dans la table {table_name}: {e}")
            return False  

        finally:
            if conn:
                cur.close()
                conn.close()
                
    def delete_file(self, file_path):
        """
        Cette methode supprime un fichier après le traitement réussi.
        :param file_path: Chemin complet du fichier à supprimer.
        """
        try:
            os.remove(file_path)
            print(f"Fichier {file_path} supprimé avec succès.")
        except Exception as e:
            print(f"Erreur lors de la suppression du fichier {file_path}: {e}")

    def normalize_table_name(self, name):
        """
        Cette methode normalise le nom de la table pour SQL Server :
        - Convertit en minuscules
        - Remplace les espaces et tirets par des underscores
        - Supprime les caractères non alphanumériques
        """
        name = name.lower()
        name = re.sub(r"[-\s]+", "_", name) 
        name = re.sub(r"[^a-z0-9_]", "", name) 
        return name

# Utilisation
if __name__ == "__main__":
    inserter = DataInserter()
    inserter.scan_and_insert()
