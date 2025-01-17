import re
from datetime import datetime
from init_connection import MongoDBConnection

class DataCleaner:
    def __init__(self):
        self.mongo_conn = MongoDBConnection()
        self.db = self.mongo_conn.connect()

    def clean_email(self, user):
        """Nettoie l'email d'un utilisateur"""
        try:
            email = user.get('email', '')
            if not isinstance(email, str):
                return f"{str(user.get('first_name', '')).lower()}.{str(user.get('last_name', '')).lower()}@example.com"
            if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
                return f"{str(user.get('first_name', '')).lower()}.{str(user.get('last_name', '')).lower()}@example.com"
            return email
        except Exception as e:
            print(f"Erreur lors du nettoyage de l'email pour l'utilisateur {user.get('_id')}: {e}")
            return f"user.{user.get('_id')}@example.com"

    def clean_phone(self, phone):
        """Nettoie un numéro de téléphone"""
        try:
            if not isinstance(phone, str):
                return '+213000000000'
            cleaned = re.sub(r'[^\d+]', '', phone)
            # Si le numéro ne commence pas par +213
            if not cleaned.startswith('+213'):
                if cleaned.startswith('+'):
                    cleaned = '+213' + cleaned[1:][-9:]
                else:
                    cleaned = '+213' + cleaned[-9:]
            if len(cleaned) > 13:  # +213 + 9 chiffres
                cleaned = cleaned[:13]
            elif len(cleaned) < 13:
                cleaned = '+213' + '0' * (9 - len(cleaned[4:])) + cleaned[4:]
            return cleaned
        except Exception as e:
            print(f"Erreur lors du nettoyage du numéro de téléphone {phone}: {e}")
            return '+213000000000'

    def clean_gender(self, gender):
        try:
            if not isinstance(gender, str):
                return 'Unknown'
            gender = str(gender).upper()
            if gender in ['MALE', 'M']:
                return 'Male'
            elif gender in ['FEMALE', 'F']:
                return 'Female'
            return 'Unknown'
        except Exception as e:
            print(f"Erreur lors du nettoyage du genre {gender}: {e}")
            return 'Unknown'

    def clean_users(self):
        """Nettoie les données utilisateurs"""
        print("Début du nettoyage des utilisateurs...")
        count = 0
        
        for user in self.db.users.find():
            try:
                update_fields = {
                    'email': self.clean_email(user),
                    'phone': self.clean_phone(user.get('phone', '')),
                    'gender': self.clean_gender(user.get('gender', ''))
                }
                
                if not isinstance(user.get('first_name'), str) or not user.get('first_name'):
                    update_fields['first_name'] = 'Unknown'
                if not isinstance(user.get('last_name'), str) or not user.get('last_name'):
                    update_fields['last_name'] = 'User'

                self.db.users.update_one(
                    {'_id': user['_id']},
                    {'$set': update_fields}
                )
                count += 1
                if count % 100 == 0:
                    print(f"Traitement de {count} utilisateurs...")
                
            except Exception as e:
                print(f"Erreur lors du nettoyage de l'utilisateur {user.get('_id')}: {e}")
                continue

        print(f"✓ Nettoyage terminé pour {count} utilisateurs")

    def clean_transactions(self):
        """Nettoie les données transactions"""
        print("\nDébut du nettoyage des transactions...")
        count = 0
        
        self.db.transactions.delete_many({
            '$or': [
                {'amount': {'$lte': 0}},
                {'amount': {'$exists': False}},
                {'amount': None}
            ]
        })

        for tx in self.db.transactions.find():
            try:
                update_fields = {}
                
                if 'timestamp' in tx:
                    try:
                        datetime.strptime(str(tx['timestamp']), '%Y-%m-%dT%H:%M:%S.%f')
                    except:
                        update_fields['timestamp'] = datetime.now().isoformat()

                if 'status' not in tx or not isinstance(tx['status'], str) or \
                   tx['status'] not in ['SUCCESS', 'PENDING', 'FAILED']:
                    update_fields['status'] = 'PENDING'

                if update_fields:
                    self.db.transactions.update_one(
                        {'_id': tx['_id']},
                        {'$set': update_fields}
                    )
                count += 1
                
                if count % 100 == 0:
                    print(f"Traitement de {count} transactions...")
                    
            except Exception as e:
                print(f"Erreur lors du nettoyage de la transaction {tx.get('_id')}: {e}")
                continue

        print(f"✓ Nettoyage terminé pour {count} transactions")

    def validate_relationships(self):
        print("\nValidation des relations...")
        valid_users = set(u['_id'] for u in self.db.users.find({}, {'_id': 1}))
        deleted = self.db.transactions.delete_many({
            'user_id': {'$nin': list(valid_users)}
        }).deleted_count
        print(f"✓ {deleted} transactions avec user_id invalide supprimées")

    def clean_all(self):
        try:
            self.clean_users()
            self.clean_transactions()
            self.validate_relationships()
            return True
        except Exception as e:
            print(f"Erreur lors du nettoyage: {e}")
            return False
        finally:
            self.mongo_conn.close()

def main():
    cleaner = DataCleaner()
    cleaner.clean_all()

if __name__ == "__main__":
    main()