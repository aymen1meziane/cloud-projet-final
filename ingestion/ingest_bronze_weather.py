"""
Script d'ingestion Bronze — Météo historique NYC (Open-Meteo)
================================================================
Récupère les données météo horaires de New York pour janvier et
février 2024 via l'API Open-Meteo (gratuite, sans clé), et upload
le résultat en JSON dans le bucket S3 Bronze.

Usage :
    pip install boto3 requests python-dotenv
    python ingest_bronze_weather.py
"""

import os
import sys
import json
import requests
import boto3
from pathlib import Path
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# ── Configuration ──────────────────────────────────────────────
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
S3_BUCKET = os.getenv("S3_BUCKET_BRONZE")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET]):
    print("ERREUR : variables manquantes dans le fichier .env")
    sys.exit(1)

# Coordonnées NYC (Manhattan, point central)
NYC_LATITUDE = 40.71
NYC_LONGITUDE = -74.01

# Période d'ingestion — alignée sur les données Taxi (Jan + Fév 2024)
PERIODS = [
    {"start_date": "2024-01-01", "end_date": "2024-01-31", "label": "2024-01"},
    {"start_date": "2024-02-01", "end_date": "2024-02-29", "label": "2024-02"},
]

# Variables météo horaires demandées
HOURLY_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "windspeed_10m",
    "weathercode",
]

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

LOCAL_TMP_DIR = Path("./tmp_downloads")
LOCAL_TMP_DIR.mkdir(exist_ok=True)

# ── Client S3 ───────────────────────────────────────────────────
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def fetch_weather_data(start_date: str, end_date: str) -> dict | None:
    """Interroge l'API Open-Meteo pour une période donnée et retourne le JSON."""
    params = {
        "latitude": NYC_LATITUDE,
        "longitude": NYC_LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "America/New_York",
    }

    try:
        print(f"  Requête Open-Meteo : {start_date} → {end_date}")
        response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "error" in data and data["error"]:
            print(f"  ERREUR API : {data.get('reason', 'erreur inconnue')}")
            return None

        nb_points = len(data.get("hourly", {}).get("time", []))
        print(f"  Reçu : {nb_points} points horaires")
        return data

    except requests.exceptions.HTTPError as e:
        print(f"  ERREUR HTTP : {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  ERREUR réseau : {e}")
        return None


def upload_to_s3(local_path: Path, s3_key: str) -> bool:
    """Upload un fichier local vers S3."""
    try:
        print(f"  Upload vers s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(str(local_path), S3_BUCKET, s3_key)
        print(f"  Upload réussi.")
        return True
    except ClientError as e:
        print(f"  ERREUR S3 : {e}")
        return False


def ingest_weather_period(start_date: str, end_date: str, label: str) -> dict:
    """
    Récupère la météo pour une période, l'écrit en JSON local,
    puis l'upload vers S3 Bronze avec partitionnement year/month.
    """
    status = {"period": label, "success": False}
    print(f"\n--- Météo NYC {label} ---")

    data = fetch_weather_data(start_date, end_date)
    if data is None:
        return status

    filename = f"weather_nyc_{label}.json"
    local_path = LOCAL_TMP_DIR / filename

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    year, month = label.split("-")
    s3_key = f"bronze/weather/year={year}/month={month}/{filename}"

    if upload_to_s3(local_path, s3_key):
        status["success"] = True
        status["s3_path"] = f"s3://{S3_BUCKET}/{s3_key}"

    if local_path.exists():
        local_path.unlink()

    return status


def main():
    print("=" * 60)
    print("INGESTION BRONZE — Météo historique NYC (Open-Meteo)")
    print(f"Bucket cible : {S3_BUCKET}")
    print(f"Coordonnées : {NYC_LATITUDE}, {NYC_LONGITUDE}")
    print(f"Variables : {', '.join(HOURLY_VARIABLES)}")
    print("=" * 60)

    results = []
    for period in PERIODS:
        result = ingest_weather_period(
            period["start_date"], period["end_date"], period["label"]
        )
        results.append(result)

    print("\n" + "=" * 60)
    print("RAPPORT D'INGESTION")
    print("=" * 60)
    success_count = sum(1 for r in results if r["success"])
    print(f"Total : {success_count}/{len(results)} périodes ingérées avec succès\n")

    for r in results:
        status_icon = "OK " if r["success"] else "FAIL"
        print(f"  [{status_icon}] {r['period']}")

    if success_count < len(results):
        print("\nATTENTION : certaines périodes n'ont pas pu être ingérées.")
        sys.exit(1)

    try:
        LOCAL_TMP_DIR.rmdir()
    except OSError:
        pass

    print("\nIngestion météo terminée avec succès.")


if __name__ == "__main__":
    main()
