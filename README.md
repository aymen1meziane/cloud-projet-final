# Pipeline Cloud NYC Mobility

Architecture médaillon (Bronze / Silver / Gold) sur AWS pour l'analyse de la mobilité urbaine new-yorkaise, croisant les trajets taxi, les signalements citoyens et la météo.

Projet réalisé dans le cadre du Master 2 Data Engineering & IA — EFREI Paris, en vue de la validation du titre RNCP36739 (Blocs 1, 2, 3, 5).

**Binôme :** MEZIANE Aymen · AIT MALEK Ramine Mahmoud

## Contexte et problématiques métier

Le projet utilise NYC comme laboratoire urbain pour répondre à trois problématiques :

1. **Optimisation de la demande et rentabilité chauffeur** — quels créneaux et zones sont les plus rentables, entre flottes Yellow et Green Taxi ?
2. **Impact des conditions météo et incidents sur la mobilité** — la météo et les signalements 311 influencent-ils la demande de courses ?
3. **Détection d'anomalies tarifaires** (bonus ML) — quelles courses présentent des tarifs ou durées statistiquement anormaux ?

## Sources de données

| Source | Format | Stockage cible | Période |
|---|---|---|---|
| NYC TLC Taxi Trips (Yellow + Green) | Parquet | S3 Bronze | Janvier–Février 2024 |
| NYC 311 Service Requests | JSON (API Socrata) | MongoDB Atlas | Janvier–Février 2024 |
| Open-Meteo (météo historique NYC) | JSON (REST API) | S3 Bronze | Janvier–Février 2024 |
| Taxi Zone Lookup (référentiel zones/boroughs) | CSV | PostgreSQL | Statique |

## Architecture

```
Sources → Bronze (S3 + MongoDB) → Silver (AWS Glue/PySpark) → Gold (Athena)
                                                                    ↓
                                          Dashboard (Streamlit) + API (FastAPI)
```

Orchestration par Airflow, monitoring par CloudWatch, qualité des données vérifiée par Great Expectations.

## Structure du repo

```
.
├── .github/workflows/      # CI/CD GitHub Actions
├── ingestion/               # Scripts d'ingestion Bronze
│   ├── ingest_bronze_taxi.py        # Backfill historique (Mode 1, manuel)
│   ├── test_ingest_bronze_taxi.py   # Tests unitaires (mocks boto3/requests)
│   └── requirements.txt
├── silver/                  # Jobs AWS Glue (à venir)
├── gold/                    # API FastAPI + Dashboard Streamlit (à venir)
├── dags/                    # DAGs Airflow (à venir)
├── .env.example             # Modèle de variables d'environnement
└── .gitignore
```

## Installation

```bash
git clone https://github.com/VOTRE_USERNAME/VOTRE_REPO.git
cd VOTRE_REPO/ingestion
pip install -r requirements.txt
cp ../.env.example ../.env
# Remplir .env avec vos propres credentials AWS
```

## Ingestion Bronze — backfill historique

Le script `ingest_bronze_taxi.py` télécharge les fichiers Parquet TLC (Yellow + Green, Jan-Fév 2024) et les upload vers S3 avec un partitionnement `year=/month=/taxi_type=` :

```bash
python ingestion/ingest_bronze_taxi.py
```

Ce script est lancé manuellement car il s'agit d'un chargement historique ponctuel (full load) — les mois 2024 déjà publiés ne changent plus. L'ingestion incrémentale des mois futurs est gérée séparément via un job AWS Glue orchestré par Airflow (Mode 2).

## Tests

```bash
cd ingestion
pytest test_ingest_bronze_taxi.py -v
```

Tous les appels réseau et AWS sont mockés — aucune vraie requête n'est effectuée pendant les tests.

## CI/CD

Le workflow `.github/workflows/ci-ingestion.yml` se déclenche automatiquement à chaque push touchant `ingestion/**` : lint flake8 puis exécution des tests unitaires.

## Sécurité et RGPD

- Accès S3 restreint via politique IAM personnalisée (principe du moindre privilège)
- Credentials jamais committés (`.gitignore` + `.env`)
- Pseudonymisation des champs identifiants lors de la transformation Bronze → Silver
- Région AWS : eu-west-3 (Paris), pour limiter la localisation des données à l'UE

## FinOps

Volume volontairement limité à 2 mois de données (~7M lignes, soit 7x le minimum requis de 1M lignes) pour garantir des itérations rapides et un coût de développement maîtrisé. Détail du comparatif de coûts dans le rapport technique.

## Licence

Projet académique — EFREI Paris, M2 Data Engineering & IA, 2025-2026.
