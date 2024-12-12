import os
import pandas as pd
import pyodbc
import re
import csv

class TableGenerator:
    def __init__(self, root_dir, db_params):
        """
        Cette methode initialise la classe avec un répertoire racine et les paramètres de la base de données.
        :param root_dir: Répertoire principal à scanner.
        :param db_params: Dictionnaire contenant les paramètres de connexion à SQL Server.
        """
        self.root_dir = root_dir
        self.db_params = db_params

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

    def scan_and_process(self):
        for dirpath, dirnames, filenames in os.walk(self.root_dir):
            for dirname in dirnames:
                folder_path = os.path.join(dirpath, dirname)
                normalized_name = self.normalize_table_name(dirname)
                self.process_folder(folder_path, normalized_name)

    def process_folder(self, folder_path, table_name):
        """
        Cette methode traite un dossier, détecte le premier fichier, et génère la table correspondante.
        :param folder_path: Chemin du dossier.
        :param table_name: Nom de la table à créer.
        """
        files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        if not files:
            print(f"Aucun fichier trouvé dans {folder_path}")
            return
        
        # On lit le premier fichier dans le dossier
        first_file = os.path.join(folder_path, files[0])
        print(f"Traitement du fichier : {first_file}")

        # Lecture du fichier (Excel ou CSV)
        try:
            if first_file.endswith(".xlsx") or first_file.endswith(".xls"):
                df = pd.read_excel(first_file)  
            elif first_file.endswith(".csv"):
                with open(first_file, 'r') as file:
                    dialect = csv.Sniffer().sniff(file.read(1024))
                    file.seek(0)
                    df = pd.read_csv(file, sep=dialect.delimiter)  
            else:
                print(f"Format non supporté pour le fichier : {first_file}")
                return
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier {first_file}: {e}")
            return

        # Demander les noms de colonnes
        columns = {}
        for column in df.columns:
            new_name = input(f"Entrez un nom pour la colonne '{column}' : ")
            columns[column] = new_name
        
        df.rename(columns=columns, inplace=True)

        # CréATION la table et insérer les données
        self.create_and_insert_table(table_name, df)

    def create_and_insert_table(self, table_name, df):
        """
        Crée une table dans SQL Server et insère les données.
        :param table_name: Nom de la table.
        :param df: DataFrame contenant les données à insérer.
        """
        try:
            # Connexion à la base de données
            conn = pyodbc.connect(
                f"DRIVER={self.db_params['driver']};"
                f"SERVER={self.db_params['server']};"
                f"DATABASE={self.db_params['database']};"
                f"UID={self.db_params['user']};"
                f"PWD={self.db_params['password']}"
            )
            cur = conn.cursor()

            # Création de la table avec une colonne 'id' auto-incrémentée
            column_definitions = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
            create_table_query = f"""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
                CREATE TABLE {table_name} (
                    id INT IDENTITY(1,1) PRIMARY KEY,  
                    {column_definitions}
                );
            """
            cur.execute(create_table_query)
            conn.commit()
            print(f"Table '{table_name}' créée et données insérées avec succès.")
        except Exception as e:
            print(f"Erreur lors de la création ou insertion dans la table {table_name}: {e}")
        finally:
            if conn:
                cur.close()
                conn.close()

# Utilisation
if __name__ == "__main__":
    root_directory = '/home/anselme/ETL/SOURCE'
    db_params = {
        "driver": "ODBC Driver 17 for SQL Server",
        "server": "localhost",
        "database": "master",
        "user": "sa",
        "password": "1234"
    }
    generator = TableGenerator(root_directory, db_params)
    generator.scan_and_process()