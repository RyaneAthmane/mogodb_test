from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv
from tabulate import tabulate
from pprint import pprint

load_dotenv()

class DatabaseChecker:
    def __init__(self):
        self.uri = os.getenv('MONGODB_URI')
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))
            # Vérifier la connexion
            self.client.admin.command('ping')
            print("✓ Connexion à MongoDB établie avec succès!\n")
        except Exception as e:
            print(f"✗ Erreur de connexion: {e}")
            raise

    def check_database(self):
        print("=== BASES DE DONNÉES DISPONIBLES ===")
        databases = self.client.list_database_names()
        for db_name in databases:
            if db_name not in ['admin', 'local']:  
                print(f"\nBase de données: {db_name}")
                db = self.client[db_name]
                collections = db.list_collection_names()
                for coll in collections:
                    count = db[coll].count_documents({})
                    print(f"  └── Collection: {coll} ({count} documents)")

    def check_test_database(self):
        """Vérifie spécifiquement la base test_database"""
        print("\n=== DÉTAILS DE TEST_DATABASE ===")
        self.db = self.client.test_database
        
        collections = self.db.list_collection_names()
        if not collections:
            print("La base test_database est vide!")
            return

        print("\n1. Collections présentes:")
        for coll in collections:
            count = self.db[coll].count_documents({})
            print(f"   ✓ {coll}: {count} documents")

        print("\n2. Échantillon des données:")
        for coll in collections:
            print(f"\n--- {coll.upper()} (5 premiers documents) ---")
            sample_data = list(self.db[coll].find().limit(5))
            headers = sample_data[0].keys() if sample_data else []
            rows = [[str(doc.get(key, ''))[:50] + '...' if len(str(doc.get(key, ''))) > 50 
                    else str(doc.get(key, '')) for key in headers] 
                   for doc in sample_data]
            print(tabulate(rows, headers=headers, tablefmt='grid'))

        print("\n3. Statistiques des collections:")
        for coll in collections:
            print(f"\nStatistiques pour {coll}:")
            stats = self.db.command("collstats", coll)
            print(f"   • Taille: {stats['size']/1024/1024:.2f} MB")
            print(f"   • Nombre de documents: {stats['count']}")
            print(f"   • Moyenne par document: {stats['avgObjSize']/1024:.2f} KB")

    def run_checks(self):
        try:
            self.connect()
            print("=== VÉRIFICATION DES BASES DE DONNÉES ===")
            self.check_database()
            self.check_test_database()
        except Exception as e:
            print(f"Une erreur s'est produite: {e}")
        finally:
            if self.client:
                self.client.close()
                print("\n✓ Connexion fermée")

def main():
    checker = DatabaseChecker()
    checker.run_checks()

if __name__ == "__main__":
    main()