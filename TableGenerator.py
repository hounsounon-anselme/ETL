import os
import pandas as pd
import pyodbc
import re
import csv



class TableGenerator:
    def __init__(self):
        """
        Initialise la classe avec un répertoire racine et les paramètres de la base de données.
        """
        self.root_dir = os.getenv("ROOT_DIR")
        self.db_params = {
            "Driver": os.getenv("DB_DRIVER"),
            "Server": os.getenv("DB_SERVER"),
            "Database": os.getenv("DB_DATABASE"),
            "UID": os.getenv("DB_USER"),
            "PWD": os.getenv("DB_PASSWORD"),
        }

    def normalize_table_name(self, name):
        """
        Normalise le nom de la table pour SQL Server :
        - Convertit en minuscules.
        - Remplace les espaces et tirets par des underscores.
        - Supprime les caractères non alphanumériques.
        """
        name = name.lower()
        name = re.sub(r"[-\s]+", "_", name)  
        name = re.sub(r"[^a-z0-9_]", "", name)
        return name

    def scan_and_process(self):
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
        Traite un dossier, détecte le premier fichier, et génère la table correspondante.
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
                with open(first_file, "r") as file:
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

        # Création de la table et insertion des données
        self.create_table(table_name, df)

    def create_table(self, table_name, df):
        """
        Crée une table dans SQL Server et insère les données.
        :param table_name: Nom de la table.
        :param df: DataFrame contenant les données à insérer.
        """
        try:
            conn = pyodbc.connect(**self.db_params)
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
            print(f"Table '{table_name}' créée avec succès.")
        except Exception as e:
            print(f"Erreur lors de la créatio  {table_name}: {e}")
        finally:
            if conn:
                cur.close()
                conn.close()


# Utilisation
if __name__ == "__main__":
    generator = TableGenerator()
    generator.scan_and_process()
