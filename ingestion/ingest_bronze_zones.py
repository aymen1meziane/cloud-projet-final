"""
Script d'ingestion Bronze — Référentiel zones TLC (RDS PostgreSQL)
======================================================================
Télécharge le fichier officiel taxi_zone_lookup.csv (265 zones TLC)
et le charge dans une table PostgreSQL sur RDS. Cette table relationnelle
sert de dimension pour traduire les LocationID des trajets Taxi en
noms de boroughs/zones lors de la jointure Silver.

Usage :
    pip install psycopg2-binary requests python-dotenv
    python ingest_bronze_zones.py
"""

import os
import sys
import csv
import io
import requests
import psycopg2
from dotenv import load_dotenv

# ── Configuration ──────────────────────────────────────────────
load_dotenv()

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT", "5432")
RDS_DBNAME = os.getenv("RDS_DBNAME", "postgres")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

if not all([RDS_HOST, RDS_USER, RDS_PASSWORD]):
    print("ERREUR : variables manquantes dans le fichier .env")
    print("Vérifiez RDS_HOST, RDS_USER, RDS_PASSWORD")
    sys.exit(1)

ZONES_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS zones (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR(50),
    zone_name VARCHAR(100),
    service_zone VARCHAR(50)
);
"""

UPSERT_SQL = """
INSERT INTO zones (location_id, borough, zone_name, service_zone)
VALUES (%s, %s, %s, %s)
ON CONFLICT (location_id) DO UPDATE SET
    borough = EXCLUDED.borough,
    zone_name = EXCLUDED.zone_name,
    service_zone = EXCLUDED.service_zone;
"""


def get_db_connection():
    """Établit et retourne une connexion à l'instance RDS PostgreSQL."""
    return psycopg2.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        dbname=RDS_DBNAME,
        user=RDS_USER,
        password=RDS_PASSWORD,
        connect_timeout=10,
    )


def download_zones_csv() -> list:
    """
    Télécharge le fichier taxi_zone_lookup.csv et le parse en liste
    de dictionnaires (un par zone).
    """
    print(f"Téléchargement : {ZONES_CSV_URL}")
    try:
        response = requests.get(ZONES_CSV_URL, timeout=30)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"ERREUR HTTP : {e}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"ERREUR réseau : {e}")
        return []

    csv_text = response.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    print(f"Téléchargé : {len(rows)} zones")
    return rows


def create_zones_table(conn) -> bool:
    """Crée la table zones si elle n'existe pas déjà."""
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        print("Table 'zones' prête (créée ou déjà existante).")
        return True
    except psycopg2.Error as e:
        print(f"ERREUR lors de la création de la table : {e}")
        conn.rollback()
        return False


def load_zones_to_db(conn, rows: list) -> int:
    """
    Charge les lignes du référentiel dans la table zones via upsert
    (insère ou met à jour si la zone existe déjà — idempotent).
    """
    written = 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    UPSERT_SQL,
                    (
                        int(row["LocationID"]),
                        row["Borough"],
                        row["Zone"],
                        row["service_zone"],
                    ),
                )
                written += 1
        conn.commit()
        return written
    except (psycopg2.Error, ValueError, KeyError) as e:
        print(f"ERREUR lors du chargement : {e}")
        conn.rollback()
        return 0


def main():
    print("=" * 60)
    print("INGESTION BRONZE — Référentiel zones TLC (RDS PostgreSQL)")
    print(f"Host : {RDS_HOST}")
    print(f"Base : {RDS_DBNAME}")
    print("=" * 60 + "\n")

    rows = download_zones_csv()
    if not rows:
        print("\nATTENTION : aucune donnée téléchargée. Arrêt du script.")
        sys.exit(1)

    try:
        conn = get_db_connection()
    except psycopg2.OperationalError as e:
        print(f"\nERREUR DE CONNEXION RDS : {e}")
        print("Vérifiez : endpoint, credentials, et que le security group")
        print("autorise votre IP actuelle sur le port 5432.")
        sys.exit(1)

    try:
        if not create_zones_table(conn):
            sys.exit(1)

        written = load_zones_to_db(conn, rows)

        print("\n" + "=" * 60)
        print("RAPPORT D'INGESTION")
        print("=" * 60)
        print(f"Zones téléchargées : {len(rows)}")
        print(f"Zones chargées (upsert) : {written}")

        if written == 0:
            print("\nATTENTION : aucune zone chargée.")
            sys.exit(1)

        print("\nIngestion référentiel zones terminée avec succès.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
