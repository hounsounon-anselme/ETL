import os
import imaplib


# Informations de connexion
imap_server = os.getenv("IMAP_SERVER")
imap_port = os.getenv("EMAIL_PORT")
email_user = os.getenv("EMAIL_USER")
email_password = os.getenv("EMAIL_PASSWORD")

try:
    # Se connecter au serveur IMAP via SSL
    mail = imaplib.IMAP4_SSL(imap_server, imap_port)
    
    # Se connecter avec l'email et le mot de passe
    mail.login(email_user, email_password)
    
    # Sélectionner la boîte de réception (Inbox)
    status, messages = mail.select("inbox") 
    
    # Vérifier le statut de la sélection
    if status == "OK":
        print("Connexion et sélection de la boîte de réception réussies !")
    else:
        print("Échec de la sélection de la boîte de réception.")
    
    # Fermer la connexion
    mail.logout()

except Exception as e:
    print(f"Erreur de connexion ou de sélection : {e}")
