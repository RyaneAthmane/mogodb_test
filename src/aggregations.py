from init_connection import MongoDBConnection

class DataAggregator:
    def __init__(self):
        self.mongo_conn = MongoDBConnection()
        self.db = self.mongo_conn.connect()

    def total_spent_by_user(self):
        """Tâche d'agrégation 1: Montant total dépensé par utilisateur"""
        return self.db.transactions.aggregate([
            {
                '$group': {
                    '_id': '$user_id',
                    'total_spent': {'$sum': '$amount'}
                }
            },
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'user_info'
                }
            },
            {
                '$project': {
                    'first_name': {'$arrayElemAt': ['$user_info.first_name', 0]},
                    'last_name': {'$arrayElemAt': ['$user_info.last_name', 0]},
                    'total_spent': 1
                }
            },
            {'$sort': {'total_spent': -1}}
        ])

    def users_with_multiple_transactions(self):
        """Tâche d'agrégation 2: Utilisateurs avec 3+ transactions"""
        return self.db.transactions.aggregate([
            {
                '$group': {
                    '_id': '$user_id',
                    'transaction_count': {'$sum': 1}
                }
            },
            {
                '$match': {
                    'transaction_count': {'$gte': 3}
                }
            },
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'user_info'
                }
            },
            {
                '$project': {
                    'first_name': {'$arrayElemAt': ['$user_info.first_name', 0]},
                    'last_name': {'$arrayElemAt': ['$user_info.last_name', 0]},
                    'transaction_count': 1
                }
            }
        ])

    def transaction_status_patterns(self):
        return self.db.transactions.aggregate([
            {
                '$group': {
                    '_id': '$status',
                    'count': {'$sum': 1},
                    'avg_amount': {'$avg': '$amount'}
                }
            }
        ])

    def users_without_transactions(self):
        return self.db.users.aggregate([
            {
                '$lookup': {
                    'from': 'transactions',
                    'localField': '_id',
                    'foreignField': 'user_id',
                    'as': 'transactions'
                }
            },
            {
                '$match': {
                    'transactions': {'$size': 0}
                }
            },
            {
                '$project': {
                    'first_name': 1,
                    'last_name': 1
                }
            }
        ])

    def run_all_aggregations(self):
        print("\n=== Montant total dépensé par utilisateur ===")
        for result in self.total_spent_by_user():
            print(f"{result['first_name']} {result['last_name']}: {result['total_spent']:.2f}")

        print("\n=== Utilisateurs avec 3+ transactions ===")
        for result in self.users_with_multiple_transactions():
            print(f"{result['first_name']} {result['last_name']}: {result['transaction_count']} transactions")

        print("\n=== Patterns des statuts de transaction ===")
        for result in self.transaction_status_patterns():
            print(f"Status {result['_id']}: {result['count']} transactions, montant moyen: {result['avg_amount']:.2f}")

        print("\n=== Utilisateurs sans transactions ===")
        for result in self.users_without_transactions():
            print(f"{result['first_name']} {result['last_name']}")

def main():
    aggregator = DataAggregator()
    aggregator.run_all_aggregations()
    aggregator.mongo_conn.close()

if __name__ == "__main__":
    main()