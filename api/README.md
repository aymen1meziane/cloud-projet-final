
# NYC Mobility — API REST (couche Gold)

API FastAPI exposant les 3 datamarts Gold (Parquet sur S3) avec documentation
Swagger auto-générée.

## Endpoints

| Méthode | Route | Description |
|---|---|---|
| GET | `/` | Point d'entrée (liste des datamarts) |
| GET | `/health` | État de santé + accessibilité des 3 sources |
| GET | `/stats` | Statistiques globales (revenu, courses, anomalies) |
| GET | `/rentabilite` | Rentabilité par zone/créneau/type |
| GET | `/meteo-demande` | Impact météo & 311 |
| GET | `/anomalies` | Anomalies tarifaires (z-score) |
| GET | `/docs` | Documentation interactive Swagger |
| GET | `/redoc` | Documentation ReDoc |

Tous les endpoints de datamarts supportent : **filtres**, **tri** (`sort_by`,
`order`), **pagination** (`limit`, `offset`).

### Exemples

```
GET /rentabilite?borough=Manhattan&taxi_type=yellow&sort_by=revenu_total&limit=20
GET /meteo-demande?borough=Brooklyn&tranche_pluie=3_pluie_forte
GET /anomalies?z_score_min=10&sort_by=z_score&order=desc&limit=50
```

## Lancement local

```bash
pip install -r requirements.txt
uvicorn main:app --reload
# → http://localhost:8000/docs
```

Les identifiants AWS sont lus via la chaîne standard boto3 : variables
d'environnement / fichier `.env` en local, **rôle IAM** une fois déployé sur AWS
(aucune clé en dur).

## Déploiement AWS (conteneur)

```bash
docker build -t nyc-mobility-api .
docker run -p 8000:8000 --env-file .env nyc-mobility-api
```

Cible recommandée : **AWS App Runner** ou **ECS Fargate** (image Docker),
avec un rôle IAM autorisant la lecture du bucket `nyc-mobility-gold-am2024`.

## Variables d'environnement

| Variable | Défaut | Rôle |
|---|---|---|
| `GOLD_BUCKET` | `nyc-mobility-gold-am2024` | Bucket des datamarts |
| `CACHE_TTL` | `3600` | Durée du cache mémoire (s) |
| `AWS_DEFAULT_REGION` | — | Région AWS (eu-west-3) |