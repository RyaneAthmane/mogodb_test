import os
import time
from datetime import datetime
from init_connection import MongoDBConnection
from data_cleaning import DataCleaner
from aggregations import DataAggregator

class DataPipeline:
    def __init__(self):
        self.start_time = None
        self.mongo_conn = None
        self.cleaner = None
        self.aggregator = None
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

    def print_section(self, title):
        print("\n" + "="*80)
        print(f" {title} ".center(80, "="))
        print("="*80 + "\n")

    def start_timer(self):
        self.start_time = time.time()

    def get_elapsed_time(self):
        return time.time() - self.start_time

    def init_connection(self):
        self.print_section("ÉTAPE 1: Connexion à MongoDB")
        try:
            self.mongo_conn = MongoDBConnection()
            db = self.mongo_conn.connect()
            print("✓ Connexion établie avec succès")
            
            print("\nImport des données initial...")
            json_path = os.path.join(self.current_dir, 'users_transactions_with_issues.json')
            self.mongo_conn.import_data(json_path)
            print("✓ Import des données terminé")
                
            return True
        except Exception as e:
            print(f"✗ Erreur lors de la connexion: {e}")
            return False


    def clean_data(self):
        self.print_section("ÉTAPE 2: Nettoyage des données")
        try:
            self.cleaner = DataCleaner()
            
            print("Nettoyage des utilisateurs...")
            initial_users = self.cleaner.db.users.count_documents({})
            self.cleaner.clean_users()
            final_users = self.cleaner.db.users.count_documents({})
            print(f"✓ {initial_users - final_users} utilisateurs nettoyés")
            
            print("\nNettoyage des transactions...")
            initial_transactions = self.cleaner.db.transactions.count_documents({})
            self.cleaner.clean_transactions()
            final_transactions = self.cleaner.db.transactions.count_documents({})
            print(f"✓ {initial_transactions - final_transactions} transactions nettoyées")
            
            print("\nValidation des relations...")
            self.cleaner.validate_relationships()
            print("✓ Relations validées")
            
            return True
        except Exception as e:
            print(f"✗ Erreur lors du nettoyage: {e}")
            return False

    def run_aggregations(self):
        self.print_section("ÉTAPE 3: Analyses et Agrégations")
        try:
            self.aggregator = DataAggregator()
            
            # Agrégation 1
            print("1. Top 10 utilisateurs par montant total dépensé:")
            print("-" * 50)
            for idx, result in enumerate(list(self.aggregator.total_spent_by_user())[:10], 1):
                print(f"{idx}. {result['first_name']} {result['last_name']}: {result['total_spent']:.2f}€")
            
            print("\n2. Utilisateurs avec 3 transactions ou plus:")
            print("-" * 50)
            for idx, result in enumerate(self.aggregator.users_with_multiple_transactions(), 1):
                print(f"{idx}. {result['first_name']} {result['last_name']}: {result['transaction_count']} transactions")
            
            print("\n3. Analyse des statuts de transaction:")
            print("-" * 50)
            for result in self.aggregator.transaction_status_patterns():
                print(f"Status {result['_id']:<10}: {result['count']:>5} transactions, "
                      f"montant moyen: {result['avg_amount']:>8.2f}€")
            
            print("\n4. Utilisateurs sans transactions:")
            print("-" * 50)
            for idx, result in enumerate(self.aggregator.users_without_transactions(), 1):
                print(f"{idx}. {result['first_name']} {result['last_name']}")
            
            return True
        except Exception as e:
            print(f"✗ Erreur lors des agrégations: {e}")
            return False

    def generate_report(self):
        self.print_section("RAPPORT FINAL")
        elapsed_time = self.get_elapsed_time()
        
        print(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Temps d'exécution total: {elapsed_time:.2f} secondes")
        print("\nStatistiques finales:")
        print("-" * 50)
        print(f"Nombre d'utilisateurs: {self.cleaner.db.users.count_documents({})}")
        print(f"Nombre de transactions: {self.cleaner.db.transactions.count_documents({})}")

    def cleanup(self):
        """Nettoie les ressources"""
        if self.mongo_conn:
            self.mongo_conn.close()

    def run(self):
        """Exécute le pipeline complet"""
        self.start_timer()
        
        try:
            if not self.init_connection():
                return
            
            if not self.clean_data():
                return
            
            if not self.run_aggregations():
                return
            
            self.generate_report()
            
        finally:
            self.cleanup()

def main():
    pipeline = DataPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()