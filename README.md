## Description
Ce projet répond au test technique Data Engineer pour TemTem

## Structure du Projet
```
mogodbtest/
├── README.md               # Documentation du projet
├── requirements.txt        # Dépendances Python
├── .env                   # Variables d'environnement
├── src/
│   ├── init_connection.py  # Script de connexion MongoDB
│   ├── data_cleaning.py    # Script de nettoyage des données
│   ├── aggregations.py     # Script d'agrégations MongoDB
│   ├── main.py            # Script principal d'exécution
│   └── check_database.py   # Script de vérification de la base
├── test_temtem_AED.ipynb  # Notebook d'analyse exploratoire
└── users_transactions_with_issues.json  # Données source
```

## Prérequis
- Python 3.8+
- MongoDB Atlas account
- pip (gestionnaire de paquets Python)
- Virtual environment (recommandé)

## Installation

1. Cloner le repository :
```bash
git clone [url-du-repo]
cd mogodbtest
```

2. Créer et activer l'environnement virtuel :
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows
```

3. Installer les dépendances :
```bash
pip install -r requirements.txt
```

4. Configurer les variables d'environnement :
   - Créer un fichier `.env` à la racine du projet avec :
```
MONGODB_URI=votre_uri_mongodb_atlas
DATABASE_NAME=test_database
```

## Utilisation

### 1. Exécution du Pipeline Complet
Pour exécuter l'ensemble du pipeline de traitement :
```bash
python src/main.py
```

Cette commande va :
- Se connecter à MongoDB
- Importer les données
- Effectuer le nettoyage
- Exécuter les agrégations
- Générer un rapport

### 2. Vérification de la Base de Données
```bash
python src/check_database.py
```

### 3. Scripts Individuels
Vous pouvez exécuter chaque script individuellement :
```bash
python src/init_connection.py  # Pour la connexion et l'import
python src/data_cleaning.py    # Pour le nettoyage
python src/aggregations.py     # Pour les agrégations
```

## Analyse Exploratoire des Données (EDA)

Le notebook Jupyter `test_temtem_AED.ipynb` contient une analyse exploratoire détaillée incluant :

### Données Utilisateurs
- Distribution des genres
- Analyse des emails
- Validation des numéros de téléphone
- Statistiques descriptives

### Données Transactions
- Distribution des montants
- Analyse temporelle
- Patterns de statuts
- Relations utilisateurs-transactions

Pour utiliser le notebook :
```bash
pip install jupyter
jupyter notebook
```

## Nettoyage des Données

### Utilisateurs
- Correction des emails invalides
- Standardisation des numéros de téléphone au format algérien (+213)
- Normalisation des genres
- Gestion des valeurs manquantes

### Transactions
- Suppression des montants négatifs ou nuls
- Correction des formats de date
- Standardisation des statuts
- Validation des relations avec les utilisateurs

## Agrégations

### Total dépensé par utilisateur
- Somme des montants par utilisateur
- Tri par montant total décroissant

### Utilisateurs fréquents
- Identification des utilisateurs avec 3+ transactions
- Analyse des comportements

### Patterns de statuts
- Distribution des statuts
- Montants moyens par statut

### Utilisateurs inactifs
- Liste des utilisateurs sans transactions
- Analyse des causes potentielles

## Tests et Validation
Pour vérifier les résultats :
- Utiliser le script `check_database.py`
- Consulter les logs d'exécution
- Vérifier les métriques dans le rapport final
