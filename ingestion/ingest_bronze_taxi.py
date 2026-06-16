"""
Script d'ingestion Bronze — Projet Pipeline Cloud NYC Mobility
================================================================
Télécharge les fichiers Parquet NYC TLC (Yellow + Green Taxi) pour
janvier et février 2024, et les upload directement dans le bucket
S3 Bronze, avec partitionnement par date et type de taxi.

Usage :
    pip install boto3 requests python-dotenv
    python ingest_bronze_taxi.py
"""

import os
import sys
import requests
import boto3
from pathlib import Path
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# ── Configuration ──────────────────────────────────────────────
load_dotenv()  # charge les variables depuis le fichier .env

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
S3_BUCKET = os.getenv("S3_BUCKET_BRONZE")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET]):
    print("ERREUR : variables manquantes dans le fichier .env")
    print("Vérifiez AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_BRONZE")
    sys.exit(1)

# Mois à ingérer (format YYYY-MM)
MONTHS = ["2024-01", "2024-02"]

# Types de taxi à ingérer
TAXI_TYPES = ["yellow", "green"]

# URL de base TLC (S3 public CloudFront)
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Dossier temporaire local pour le téléchargement avant upload
LOCAL_TMP_DIR = Path("./tmp_downloads")
LOCAL_TMP_DIR.mkdir(exist_ok=True)

# ── Client S3 ───────────────────────────────────────────────────
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def download_file(url: str, local_path: Path) -> bool:
    """Télécharge un fichier depuis une URL avec suivi de progression simple."""
    try:
        print(f"  Téléchargement : {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    pct = (downloaded / total_size) * 100
                    print(f"\r  Progression : {pct:.1f}%", end="", flush=True)

        print(f"\n  Téléchargé : {local_path.name} ({downloaded / 1_048_576:.1f} Mo)")
        return True

    except requests.exceptions.HTTPError as e:
        print(f"  ERREUR HTTP : {e} — le fichier n'existe peut-être pas encore pour ce mois")
        return False
    except requests.exceptions.RequestException as e:
        print(f"  ERREUR réseau : {e}")
        return False


def upload_to_s3(local_path: Path, s3_key: str) -> bool:
    """Upload un fichier local vers S3 avec la clé (chemin) spécifiée."""
    try:
        print(f"  Upload vers s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(str(local_path), S3_BUCKET, s3_key)
        print(f"  Upload réussi.")
        return True
    except ClientError as e:
        print(f"  ERREUR S3 : {e}")
        return False


def ingest_taxi_month(taxi_type: str, month: str) -> dict:
    """
    Télécharge et upload un fichier Parquet TLC pour un type de taxi et un mois donnés.
    Retourne un dict de statut pour le rapport d'ingestion.
    """
    filename = f"{taxi_type}_tripdata_{month}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"
    local_path = LOCAL_TMP_DIR / filename

    # Partitionnement Bronze : year=YYYY/month=MM/taxi_type=yellow/fichier.parquet
    year, mon = month.split("-")
    s3_key = f"bronze/taxi/year={year}/month={mon}/taxi_type={taxi_type}/{filename}"

    status = {"taxi_type": taxi_type, "month": month, "filename": filename, "success": False}

    print(f"\n--- {taxi_type.upper()} {month} ---")

    if not download_file(url, local_path):
        return status

    if upload_to_s3(local_path, s3_key):
        status["success"] = True
        status["s3_path"] = f"s3://{S3_BUCKET}/{s3_key}"

    # Nettoyage local après upload pour ne pas saturer le disque
    if local_path.exists():
        local_path.unlink()

    return status


def main():
    print("=" * 60)
    print("INGESTION BRONZE — NYC TLC Taxi Trip Data")
    print(f"Bucket cible : {S3_BUCKET}")
    print(f"Région : {AWS_REGION}")
    print(f"Mois : {', '.join(MONTHS)}")
    print(f"Types : {', '.join(TAXI_TYPES)}")
    print("=" * 60)

    results = []
    for taxi_type in TAXI_TYPES:
        for month in MONTHS:
            result = ingest_taxi_month(taxi_type, month)
            results.append(result)

    # Rapport final
    print("\n" + "=" * 60)
    print("RAPPORT D'INGESTION")
    print("=" * 60)
    success_count = sum(1 for r in results if r["success"])
    print(f"Total : {success_count}/{len(results)} fichiers ingérés avec succès\n")

    for r in results:
        status_icon = "OK " if r["success"] else "FAIL"
        print(f"  [{status_icon}] {r['taxi_type']:8s} {r['month']}")

    if success_count < len(results):
        print("\nATTENTION : certains fichiers n'ont pas pu être ingérés.")
        print("Vérifiez la disponibilité du mois sur https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
        sys.exit(1)

    # Nettoyage final du dossier temporaire
    try:
        LOCAL_TMP_DIR.rmdir()
    except OSError:
        pass  # dossier non vide ou autre, pas critique

    print("\nIngestion Bronze terminée avec succès.")


if __name__ == "__main__":
    main()
