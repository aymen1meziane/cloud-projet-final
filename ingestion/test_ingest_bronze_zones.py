"""
Tests unitaires — Script d'ingestion Bronze référentiel zones (RDS)
=======================================================================
Valide la logique de téléchargement, création de table et chargement,
sans jamais faire de vrai appel réseau ni de vraie connexion PostgreSQL.

Usage :
    pip install pytest
    pytest test_ingest_bronze_zones.py -v
"""

import sys
import os
from unittest.mock import patch, MagicMock
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

os.environ["RDS_HOST"] = "fake-host.eu-west-3.rds.amazonaws.com"
os.environ["RDS_PORT"] = "5432"
os.environ["RDS_DBNAME"] = "postgres"
os.environ["RDS_USER"] = "fake_user"
os.environ["RDS_PASSWORD"] = "fake_password"

import ingest_bronze_zones as ingest


# ── Fixtures ────────────────────────────────────────────────────

@pytest.fixture
def fake_csv_content():
    """Simule le contenu du fichier taxi_zone_lookup.csv (3 lignes)."""
    return (
        '"LocationID","Borough","Zone","service_zone"\n'
        '1,"EWR","Newark Airport","EWR"\n'
        '2,"Queens","Jamaica Bay","Boro Zone"\n'
        '4,"Manhattan","Alphabet City","Yellow Zone"\n'
    )


@pytest.fixture
def fake_response_ok(fake_csv_content):
    """Simule une réponse HTTP 200 avec le CSV en contenu."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.content = fake_csv_content.encode("utf-8")
    return mock_resp


# ── Tests : download_zones_csv ─────────────────────────────────

class TestDownloadZonesCsv:

    @patch("ingest_bronze_zones.requests.get")
    def test_download_success_parses_csv_correctly(self, mock_get, fake_response_ok):
        """Un téléchargement réussi doit retourner une liste de dicts bien parsée."""
        mock_get.return_value = fake_response_ok

        rows = ingest.download_zones_csv()

        assert len(rows) == 3
        assert rows[0]["LocationID"] == "1"
        assert rows[0]["Borough"] == "EWR"
        assert rows[2]["Zone"] == "Alphabet City"

    @patch("ingest_bronze_zones.requests.get")
    def test_download_http_error_returns_empty_list(self, mock_get):
        """Une erreur HTTP doit retourner une liste vide, pas planter le script."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = ingest.requests.exceptions.HTTPError("404")
        mock_get.return_value = mock_resp

        rows = ingest.download_zones_csv()

        assert rows == []

    @patch("ingest_bronze_zones.requests.get")
    def test_download_network_error_returns_empty_list(self, mock_get):
        """Une erreur réseau doit être interceptée proprement."""
        mock_get.side_effect = ingest.requests.exceptions.ConnectionError("timeout")

        rows = ingest.download_zones_csv()

        assert rows == []


# ── Tests : create_zones_table ─────────────────────────────────

class TestCreateZonesTable:

    def test_create_table_success(self):
        """La création de table réussie doit retourner True et commit."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        result = ingest.create_zones_table(mock_conn)

        assert result is True
        mock_cursor.execute.assert_called_once_with(ingest.CREATE_TABLE_SQL)
        mock_conn.commit.assert_called_once()

    def test_create_table_failure_rolls_back(self):
        """Une erreur SQL doit déclencher un rollback et retourner False."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = ingest.psycopg2.Error("permission denied")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        result = ingest.create_zones_table(mock_conn)

        assert result is False
        mock_conn.rollback.assert_called_once()


# ── Tests : load_zones_to_db ────────────────────────────────────

class TestLoadZonesToDb:

    def test_load_success_counts_all_rows(self):
        """Toutes les lignes valides doivent être comptées et commit appelé."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        rows = [
            {"LocationID": "1", "Borough": "EWR", "Zone": "Newark Airport", "service_zone": "EWR"},
            {"LocationID": "2", "Borough": "Queens", "Zone": "Jamaica Bay", "service_zone": "Boro Zone"},
        ]

        written = ingest.load_zones_to_db(mock_conn, rows)

        assert written == 2
        assert mock_cursor.execute.call_count == 2
        mock_conn.commit.assert_called_once()

    def test_load_converts_location_id_to_int(self):
        """Vérifie que LocationID (string dans le CSV) est bien converti en int pour la requête SQL."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        rows = [{"LocationID": "42", "Borough": "Bronx", "Zone": "Test Zone", "service_zone": "Boro Zone"}]

        ingest.load_zones_to_db(mock_conn, rows)

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert params[0] == 42
        assert isinstance(params[0], int)

    def test_load_sql_error_rolls_back_and_returns_zero(self):
        """Une erreur SQL pendant le chargement doit rollback et retourner 0."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = ingest.psycopg2.Error("constraint violation")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        rows = [{"LocationID": "1", "Borough": "EWR", "Zone": "Test", "service_zone": "EWR"}]

        written = ingest.load_zones_to_db(mock_conn, rows)

        assert written == 0
        mock_conn.rollback.assert_called_once()

    def test_load_missing_field_rolls_back_and_returns_zero(self):
        """Une ligne malformée (champ manquant) doit être interceptée sans planter."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        rows = [{"LocationID": "1", "Borough": "EWR"}]  # Zone et service_zone absents

        written = ingest.load_zones_to_db(mock_conn, rows)

        assert written == 0
        mock_conn.rollback.assert_called_once()


# ── Tests : get_db_connection ────────────────────────────────────

class TestGetDbConnection:

    @patch("ingest_bronze_zones.psycopg2.connect")
    def test_connection_uses_env_variables(self, mock_connect):
        """Vérifie que la connexion utilise bien les paramètres du .env."""
        ingest.get_db_connection()

        mock_connect.assert_called_once_with(
            host="fake-host.eu-west-3.rds.amazonaws.com",
            port="5432",
            dbname="postgres",
            user="fake_user",
            password="fake_password",
            connect_timeout=10,
        )
