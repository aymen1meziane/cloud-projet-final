"""
Script d'ingestion Bronze — NYC 311 Service Requests → DynamoDB
==================================================================
Récupère les signalements NYC 311 pour janvier et février 2024 via
l'API Socrata (JSON), pagine automatiquement les résultats, et les
écrit dans une table DynamoDB (clé de partition : unique_key).

Usage :
    pip install boto3 requests python-dotenv
    python ingest_bronze_311.py
"""

import os
import sys
import time
import requests
import boto3
from datetime import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# ── Configuration ──────────────────────────────────────────────
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE_311", "nyc-311-bronze")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
    print("ERREUR : variables manquantes dans le fichier .env")
    print("Vérifiez AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
    sys.exit(1)

# Période d'ingestion — alignée sur Taxi et Météo (Jan-Fév 2024*)
START_DATE = "2024-01-01T00:00:00"
END_DATE = "2024-03-01T00:00:00"  # exclusif, couvre janvier + février

SOCRATA_BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

# Socrata limite à 1000 lignes par page par défaut -> on pagine
PAGE_SIZE = 1000

# Champs conservés (évite de stocker des colonnes inutiles dans DynamoDB)
FIELDS_TO_KEEP = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "borough",
    "incident_zip",
    "latitude",
    "longitude",
    "status",
]

# ── Clients AWS ─────────────────────────────────────────────────
dynamodb = boto3.resource(
    "dynamodb",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
table = dynamodb.Table(DYNAMODB_TABLE)


def fetch_311_page(offset: int, limit: int = PAGE_SIZE) -> list:
    """Récupère une page de résultats depuis l'API Socrata NYC 311."""
    params = {
        "$where": f"created_date between '{START_DATE}' and '{END_DATE}'",
        "$limit": limit,
        "$offset": offset,
        "$order": "created_date ASC",
    }

    try:
        response = requests.get(SOCRATA_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"  ERREUR HTTP à l'offset {offset} : {e}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"  ERREUR réseau à l'offset {offset} : {e}")
        return []


def pseudonymize_coordinate(value: str, ndigits: int = 2) -> str:
    """
    RGPD — Pseudonymisation par arrondi des coordonnées GPS.

    Arrondir à 2 décimales réduit la précision à environ 1 km, ce qui
    suffit pour des analyses par borough/zone (notre cas d'usage),
    tout en empêchant l'identification d'une adresse précise (privacy
    by design appliqué dès l'ingestion Bronze, pas seulement en Silver).

    Retourne "unknown" si la valeur n'est pas un nombre valide,
    cohérent avec le reste de clean_record().
    """
    try:
        return str(round(float(value), ndigits))
    except (ValueError, TypeError):
        return "unknown"


def clean_record(record: dict) -> dict:
    """
    Filtre un enregistrement 311 pour ne garder que les champs utiles,
    pseudonymise les coordonnées GPS (RGPD), et convertit les valeurs
    manquantes en chaîne vide (DynamoDB n'accepte pas certaines valeurs
    ambiguës comme None implicite).
    """
    cleaned = {}
    for field in FIELDS_TO_KEEP:
        value = record.get(field, "")
        cleaned[field] = value if value not in (None, "") else "unknown"

    # Pseudonymisation RGPD : on écrase les coordonnées précises par
    # une version arrondie, AVANT toute écriture en base (Bronze).
    if cleaned.get("latitude") != "unknown":
        cleaned["latitude"] = pseudonymize_coordinate(cleaned["latitude"])
    if cleaned.get("longitude") != "unknown":
        cleaned["longitude"] = pseudonymize_coordinate(cleaned["longitude"])

    return cleaned


def write_batch_to_dynamodb(records: list) -> int:
    """
    Écrit une liste d'enregistrements dans DynamoDB via batch_writer,
    qui gère automatiquement le découpage en lots de 25 (limite DynamoDB)
    et les retries en cas de throttling.
    """
    written = 0
    try:
        with table.batch_writer(overwrite_by_pkeys=["unique_key"]) as batch:
            for record in records:
                cleaned = clean_record(record)
                if not cleaned.get("unique_key") or cleaned["unique_key"] == "unknown":
                    continue  # on ignore les enregistrements sans clé valide
                batch.put_item(Item=cleaned)
                written += 1
        return written
    except ClientError as e:
        print(f"  ERREUR DynamoDB lors de l'écriture batch : {e}")
        return written


def ingest_all_pages() -> dict:
    """
    Boucle de pagination complète : récupère toutes les pages jusqu'à
    épuisement des résultats, et les écrit progressivement dans DynamoDB.
    """
    offset = 0
    total_fetched = 0
    total_written = 0
    page_number = 1

    print(f"Période ciblée : {START_DATE} → {END_DATE}")
    print(f"Table DynamoDB cible : {DYNAMODB_TABLE}\n")

    while True:
        print(f"Page {page_number} (offset={offset})...")
        page_data = fetch_311_page(offset)

        if not page_data:
            print("  Aucune donnée supplémentaire — fin de la pagination.")
            break

        nb_records = len(page_data)
        total_fetched += nb_records

        written = write_batch_to_dynamodb(page_data)
        total_written += written

        print(f"  {nb_records} reçus, {written} écrits dans DynamoDB")

        if nb_records < PAGE_SIZE:
            # dernière page (moins de résultats que la taille de page demandée)
            break

        offset += PAGE_SIZE
        page_number += 1
        time.sleep(0.5)  # pause légère pour ne pas surcharger l'API publique

    return {
        "total_fetched": total_fetched,
        "total_written": total_written,
        "pages": page_number,
    }


def main():
    print("=" * 60)
    print("INGESTION BRONZE — NYC 311 Service Requests → DynamoDB")
    print(f"Région AWS : {AWS_REGION}")
    print("=" * 60 + "\n")

    start_time = datetime.now()
    result = ingest_all_pages()
    elapsed = (datetime.now() - start_time).total_seconds()

    print("\n" + "=" * 60)
    print("RAPPORT D'INGESTION")
    print("=" * 60)
    print(f"Pages traitées      : {result['pages']}")
    print(f"Enregistrements reçus : {result['total_fetched']}")
    print(f"Enregistrements écrits : {result['total_written']}")
    print(f"Temps écoulé         : {elapsed:.1f} secondes")

    if result["total_fetched"] == 0:
        print("\nATTENTION : aucune donnée récupérée. Vérifiez la connectivité")
        print("à l'API Socrata ou les dates de la période ciblée.")
        sys.exit(1)

    print("\nIngestion 311 terminée avec succès.")


if __name__ == "__main__":
    main()