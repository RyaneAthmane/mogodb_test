import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json

# Charger les variables d'environnement
load_dotenv()

class MongoDBConnection:
    def __init__(self):
        self.uri = os.getenv('MONGODB_URI')
        self.client = None
        self.db = None

    def connect(self):
        """Établit la connexion à MongoDB"""
        try:
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))
            self.db = self.client[os.getenv('DATABASE_NAME')]
            # Tester la connexion
            self.client.admin.command('ping')
            print("Connexion à MongoDB établie avec succès!")
            return self.db
        except Exception as e:
            print(f"Erreur de connexion: {e}")
            raise

    def drop_collections(self):
        """Supprime les collections existantes"""
        try:
            self.db.users.drop()
            self.db.transactions.drop()
            print("Collections existantes supprimées avec succès")
        except Exception as e:
            print(f"Erreur lors de la suppression des collections: {e}")

    def import_data(self, file_path):
        """Importe les données depuis le fichier JSON"""
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Le fichier {file_path} n'existe pas")
            
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            # Supprimer d'abord les collections existantes
            self.drop_collections()
            
            # Créer un index unique sur email pour users
            self.db.users.create_index("email", unique=True)
            
            # Insertion des utilisateurs
            if 'users' in data:
                try:
                    self.db.users.insert_many(data['users'], ordered=False)
                except Exception as e:
                    print(f"Certains utilisateurs n'ont pas pu être insérés (doublons ignorés): {e}")
                finally:
                    print(f"Import des utilisateurs terminé")
            
            # Insertion des transactions
            if 'transactions' in data:
                try:
                    self.db.transactions.insert_many(data['transactions'], ordered=False)
                except Exception as e:
                    print(f"Certaines transactions n'ont pas pu être insérées: {e}")
                finally:
                    print(f"Import des transactions terminé")
            
            # Afficher les statistiques finales
            print(f"\nStatistiques d'import:")
            print(f"Utilisateurs importés: {self.db.users.count_documents({})}")
            print(f"Transactions importées: {self.db.transactions.count_documents({})}")
            
        except Exception as e:
            print(f"Erreur lors de l'import: {e}")
            raise

    def close(self):
        """Ferme la connexion"""
        if self.client:
            self.client.close()
            print("Connexion fermée")

def main():
    mongo_connection = MongoDBConnection()
    try:
        db = mongo_connection.connect()
        # Import des données si nécessaire
        mongo_connection.import_data('users_transactions_with_issues.json')
    finally:
        mongo_connection.close()

if __name__ == "__main__":
    main()