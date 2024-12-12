import imaplib
import email
from email.header import decode_header
import os
import pandas as pd
import pyodbc
from datetime import datetime, timedelta

class EmailDataInserter:
    def __init__(self, imap_server, email_user, email_password, db_params, save_dir="emails_attachments"):
        """
        Cette methode initialise la classe avec les paramètres de connexion IMAP et la base de données.
        :param imap_server: Serveur IMAP pour récupérer les emails.
        :param email_user: Adresse email pour la connexion.
        :param email_password: Mot de passe de l'adresse email.
        :param db_params: Paramètres de connexion à la base de données SQL Server.
        :param save_dir: Dossier où les fichiers joints seront enregistrés avant traitement.
        """
        self.imap_server = imap_server
        self.email_user = email_user
        self.email_password = email_password
        self.db_params = db_params
        self.save_dir = save_dir

        # Créer le répertoire si il n'existe pas
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

    def connect_to_email(self):
        """
        Connexion à la boîte de réception via IMAP et sélectionne la boîte de réception.
        """
        try:
            # Connexion au serveur IMAP
            mail = imaplib.IMAP4_SSL(self.imap_server)
            mail.login(self.email_user, self.email_password)
            mail.select("inbox")  # selection de la boîte de réception
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
            # Calcule la date et l'heure d'une heure avant
            since_time = (datetime.now() - timedelta(hours=1)).strftime("%d-%b-%Y %H:%M:%S")

            # Recherche des emails reçus depuis l'heure calculée
            status, messages = mail.search(None, f"SINCE {since_time}")
            if status != "OK":
                print("Aucun email trouvé dans la dernière heure.")
                return []

            email_ids = messages[0].split()
            emails_with_attachments = []

            for email_id in email_ids:
                status, msg_data = mail.fetch(email_id, "(RFC822)")
                for response_part in msg_data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_bytes(response_part[1])

                        # Décoder le sujet
                        subject, encoding = decode_header(msg["Subject"])[0]
                        if isinstance(subject, bytes):
                            subject = subject.decode(encoding if encoding else "utf-8")

                        # Vérifier les pièces jointes
                        if msg.is_multipart():
                            for part in msg.walk():
                                content_type = part.get_content_type()
                                content_disposition = str(part.get("Content-Disposition"))

                                if "attachment" in content_disposition:
                                    filename = part.get_filename()
                                    if filename:
                                        filepath = os.path.join(self.save_dir, filename)
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
        Cette methode insere des données dans une table SQL Server existante.
        :param table_name: Nom de la table.
        :param df: DataFrame contenant les données à insérer.
        """
        try:
            conn = pyodbc.connect(**self.db_params)
            cursor = conn.cursor()

            for _, row in df.iterrows():
                placeholders = ", ".join(["?"] * len(row))
                insert_query = f"INSERT INTO {table_name} VALUES ({placeholders});"
                cursor.execute(insert_query, tuple(row))

            conn.commit()
            print(f"Données insérées dans la table '{table_name}' avec succès.")
        except Exception as e:
            print(f"Erreur lors de l'insertion des données dans la table {table_name}: {e}")
        finally:
            if conn:
                cursor.close()
                conn.close()

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
                    df = pd.read_csv(file_path)
                else:
                    print(f"Format non supporté pour le fichier : {file_path}")
                    continue
            except Exception as e:
                print(f"Erreur lors de la lecture du fichier {file_path}: {e}")
                continue

            # Insérer les données dans la table 'email' (ou une autre table appropriée)
            self.insert_data_into_table("email", df)

# Utilisation
if __name__ == "__main__":
    imap_server = "imap.mail.ovh.net"  # Serveur IMAP OVH
    email_user = 'ahounsounon@groupmediacontact.com'
    email_password = '1234'

    db_params = {
        "Driver": "{ODBC Driver 17 for SQL Server}",
        "Server": "localhost",  
        "Database": "master",
        "UID": "sa",
        "PWD": "1234"
    }

    inserter = EmailDataInserter(imap_server, email_user, email_password, db_params)
    inserter.process_email_attachments()
