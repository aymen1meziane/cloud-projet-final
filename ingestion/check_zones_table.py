"""
Script utilitaire — Vérification du contenu de la table zones (RDS)
========================================================================
Affiche un échantillon et des statistiques sur la table zones pour
vérifier visuellement que l'ingestion s'est bien déroulée.

Usage :
    python check_zones_table.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("RDS_HOST"),
    port=os.getenv("RDS_PORT", "5432"),
    dbname=os.getenv("RDS_DBNAME", "postgres"),
    user=os.getenv("RDS_USER"),
    password=os.getenv("RDS_PASSWORD"),
    connect_timeout=10,
)

with conn.cursor() as cur:
    # Nombre total de lignes
    cur.execute("SELECT COUNT(*) FROM zones;")
    total = cur.fetchone()[0]
    print(f"Nombre total de zones : {total}\n")

    # Échantillon des 10 premières lignes
    print("Échantillon (10 premières lignes) :")
    cur.execute("SELECT * FROM zones ORDER BY location_id LIMIT 10;")
    for row in cur.fetchall():
        print(f"  {row}")

    # Répartition par borough
    print("\nRépartition par borough :")
    cur.execute("""
        SELECT borough, COUNT(*) as nb_zones
        FROM zones
        GROUP BY borough
        ORDER BY nb_zones DESC;
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:20s} : {row[1]} zones")

conn.close()
