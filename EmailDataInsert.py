import imaplib
import email
from email.header import decode_header
import os
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import csv


class EmailDataInserter:
    def __init__(self):
        """
        Initialise la classe avec les paramètres de connexion IMAP et la base de données.
        :param imap_server: Serveur IMAP pour récupérer les emails.
        :param email_user: Adresse email pour la connexion.
        :param email_password: Mot de passe de l'adresse email.
        :param db_params: Paramètres de connexion à la base de données SQL Server.
        :param root_dir: Dossier racine pour construire le chemin du répertoire de sauvegarde.
        """
        self.save_dir = os.path.join(os.getenv("ROOT_DIR"), "emails_attachments")
        self.imap_server = os.getenv("IMAP_SERVER")
        self.email_user = os.getenv("EMAIL_USER")
        self.email_password = os.getenv("EMAIL_PASSWORD")        
        self.db_params = {
            "Driver": os.getenv("DB_DRIVER"),
            "Server": os.getenv("DB_SERVER"),
            "Database": os.getenv("DB_DATABASE"),
            "UID": os.getenv("DB_USER"),
            "PWD": os.getenv("DB_PASSWORD")
        }

        # Créer le répertoire si il n'existe pas
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    def connect_to_email(self):
        """
        Connexion à la boîte de réception via IMAP et sélectionne la boîte de réception.
        """
        try:
            mail = imaplib.IMAP4_SSL(self.imap_server, 993)
            mail.login(self.email_user, self.email_password)
            mail.select("inbox")  # Sélection de la boîte de réception
            return mail
        except Exception as e:
            print(f"Erreur de connexion à l'email : {e}")
            return None

    def retrieve_emails(self):
        """
        Récupère les emails reçus dans la dernière heure, qu'ils soient lus ou non.
        """
        mail = self.connect_to_email()
        if not mail:
            return []

        try:
            since_time = (datetime.now() - timedelta(hours=1)).strftime("%d-%b-%Y")  
            print(since_time)

            # Recherche des emails reçus depuis l'heure calculée
            status, messages = mail.search(None, f"SINCE {since_time}")
            if status != "OK" or not messages[0]:  # Vérifier que la recherche a retourné des emails
                print("Aucun email trouvé dans la dernière heure.")
                return []

            email_ids = messages[0].split()
            emails_with_attachments = []

            for email_id in email_ids:
                status, msg_data = mail.fetch(email_id, "(RFC822)")
                for response_part in msg_data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_bytes(response_part[1])

                        subject, encoding = decode_header(msg["Subject"])[0]
                        if isinstance(subject, bytes):
                            subject = subject.decode(encoding if encoding else "utf-8")

                        if msg.is_multipart():
                            for part in msg.walk():
                                content_type = part.get_content_type()
                                content_disposition = str(part.get("Content-Disposition"))

                                if "attachment" in content_disposition:
                                    filename = part.get_filename()

                                    if filename:
                                        decoded_filename, encoding = decode_header(filename)[0]
                                        if isinstance(decoded_filename, bytes):
                                            decoded_filename = decoded_filename.decode(encoding or 'utf-8')

                                        filepath = os.path.join(self.save_dir, decoded_filename)
                                        with open(filepath, "wb") as f:
                                            f.write(part.get_payload(decode=True))

                                        emails_with_attachments.append({
                                            "subject": subject,
                                            "file_path": filepath
                                        })
            return emails_with_attachments
        except Exception as e:
            print(f"Erreur lors de la récupération des emails : {e}")
            return []

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

    def check_table(self, table_name, df):
        """
        Vérifie si une table existe, et la crée si elle n'existe pas.
        :param table_name: Nom de la table.
        :param df: DataFrame contenant les données pour définir les colonnes.
        """
        try:
            with pyodbc.connect(**self.db_params) as conn:
                cursor = conn.cursor()
                # Vérification de l'existence de la table
                query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?;"
                cursor.execute(query, table_name)
                table_exists = cursor.fetchone()[0]

                if table_exists:
                    print(f"La table '{table_name}' existe déjà.")
                else:
                    try:
                        columns = {}
                        for column in df.columns:
                            new_name = input(f"Entrez un nom pour la colonne '{column}' : ")
                            columns[column] = new_name

                        df.rename(columns=columns, inplace=True)

                        # Création de la table avec une colonne 'id' auto-incrémentée
                        column_definitions = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
                        create_table_query = f"""
                            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
                            CREATE TABLE {table_name} (
                                id INT IDENTITY(1,1) PRIMARY KEY,
                                {column_definitions}
                            );
                        """
                        cursor.execute(create_table_query)
                        conn.commit()
                        print(f"Table '{table_name}' créée avec succès.")
                    except Exception as e:
                        print(f"Erreur lors de la création de la table '{table_name}': {e}")
        except Exception as e:
            print(f"Erreur lors de la vérification ou de la création de la table '{table_name}': {e}")

    def delete_file(self, file_path):
        """
        Supprime un fichier après le traitement réussi.
        :param file_path: Chemin complet du fichier à supprimer.
        """
        try:
            os.remove(file_path)
            print(f"Fichier {file_path} supprimé avec succès.")
        except Exception as e:
            print(f"Erreur lors de la suppression du fichier {file_path}: {e}")

    def process_email_attachments(self):
        """
        Traitement des emails reçus dans la dernière heure, récupère les fichiers joints et insère leurs données dans la table.
        """
        emails_with_attachments = self.retrieve_emails()

        for email_info in emails_with_attachments:
            file_path = email_info["file_path"]
            print(f"Traitement du fichier joint : {file_path}")

            # Lire le fichier (Excel ou CSV)
            try:
                if file_path.endswith((".xlsx", ".xls")):
                    df = pd.read_excel(file_path)
                elif file_path.endswith(".csv"):
                    with open(file_path, 'r') as file_csv:
                        dialect = csv.Sniffer().sniff(file_csv.read(1024))
                        file_csv.seek(0)
                        df = pd.read_csv(file_csv, sep=dialect.delimiter)
                else:
                    print(f"Format non supporté pour le fichier : {file_path}")
                    continue
            except Exception as e:
                continue

            # Vérifier le nom du fichier pour insérer dans la bonne table
            filename = os.path.basename(file_path)
            if filename.startswith("solde"):
                print(f"Le fichier {filename} commence par 'solde'. Vérification et insertion dans 'solde_per_heure'.")
                self.check_table('solde_per_heure', df)
                success = self.insert_data_into_table('solde_per_heure', df)
                if success:
                    self.delete_file(file_path)
            elif filename.startswith("TELEPIN_BALANCE"):
                print(f"Le fichier {filename} commence par 'TELEPIN_BALANCE'. Vérification et insertion dans 'telepin_balance'.")
                self.check_table('telepin_balance', df)
                success = self.insert_data_into_table('telepin_balance', df)
                if success:
                    self.delete_file(file_path)
            else:
                print(f"Le fichier {filename} ne correspond à aucun format connu.")
# Utilisation
if __name__ == "__main__":
    inserter = EmailDataInserter()
    inserter.process_email_attachments()
