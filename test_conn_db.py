import os
import pyodbc

def test_sql_server_connection():
    """
    On teste ici la connexion à un serveur SQL Server.
    :param server: Nom ou adresse du serveur SQL Server.
    :param database: Nom de la base de données.
    :param username: Nom d'utilisateur pour la connexion.
    :param password: Mot de passe de l'utilisateur.
    """
    
    try:
        connection_string = (
            f"DRIVER={os.getenv('DB_DRIVER')};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_DATABASE')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASSWORD')};"
        )
        
        # Établir la connexion
        with pyodbc.connect(connection_string) as conn:
            print("Connexion réussie à SQL Server.")
            # Vérification en exécutant une requête simple pour contrôler la version
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION;")
            result = cursor.fetchone()
            print(f"Version de SQL Server : {result[0]}")
    
    except Exception as e:
        print(f"Erreur lors de la connexion à SQL Server : {e}")

# Utilisation
if __name__ == "__main__":
    test_sql_server_connection()
