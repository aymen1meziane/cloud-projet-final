"""
Tests unitaires — Script d'ingestion Bronze NYC 311 (DynamoDB)
=================================================================
Valide la logique de pagination, de nettoyage des champs et
d'écriture DynamoDB, sans jamais faire de vrai appel réseau ni
de vrai appel AWS.

Usage :
    pip install pytest
    pytest test_ingest_bronze_311.py -v
"""

import sys
import os
from unittest.mock import patch, MagicMock, call
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

os.environ["AWS_ACCESS_KEY_ID"] = "fake_access_key_for_tests"
os.environ["AWS_SECRET_ACCESS_KEY"] = "fake_secret_key_for_tests"
os.environ["AWS_REGION"] = "eu-west-3"
os.environ["DYNAMODB_TABLE_311"] = "fake-table-for-tests"

import ingest_bronze_311 as ingest


# ── Fixtures ────────────────────────────────────────────────────

@pytest.fixture
def fake_311_record_complete():
    """Un enregistrement 311 complet, tel que renvoyé par l'API Socrata."""
    return {
        "unique_key": "60123456",
        "created_date": "2024-01-15T08:30:00.000",
        "closed_date": "2024-01-16T10:00:00.000",
        "agency": "NYPD",
        "agency_name": "New York City Police Department",
        "complaint_type": "Noise - Residential",
        "descriptor": "Loud Music/Party",
        "borough": "BROOKLYN",
        "incident_zip": "11201",
        "latitude": "40.6938",
        "longitude": "-73.9903",
        "status": "Closed",
    }


@pytest.fixture
def fake_311_record_missing_fields():
    """Un enregistrement 311 avec des champs manquants (cas réaliste fréquent)."""
    return {
        "unique_key": "60999999",
        "created_date": "2024-02-10T14:00:00.000",
        "agency": "DOT",
        "complaint_type": "Street Condition",
        # closed_date, descriptor, borough, latitude, longitude absents
    }


@pytest.fixture
def fake_311_record_no_unique_key():
    """Un enregistrement corrompu sans clé unique — doit être ignoré."""
    return {
        "created_date": "2024-01-01T00:00:00.000",
        "agency": "DOB",
    }


# ── Tests : fetch_311_page ──────────────────────────────────────

class TestFetch311Page:

    @patch("ingest_bronze_311.requests.get")
    def test_fetch_page_success(self, mock_get, fake_311_record_complete):
        """Une requête réussie doit retourner la liste de résultats."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = [fake_311_record_complete]
        mock_get.return_value = mock_resp

        result = ingest.fetch_311_page(offset=0)

        assert len(result) == 1
        assert result[0]["unique_key"] == "60123456"

    @patch("ingest_bronze_311.requests.get")
    def test_fetch_page_sends_correct_params(self, mock_get, fake_311_record_complete):
        """Vérifie que le offset, la limite et le filtre de dates sont bien transmis."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = [fake_311_record_complete]
        mock_get.return_value = mock_resp

        ingest.fetch_311_page(offset=2000, limit=500)

        params = mock_get.call_args[1]["params"]
        assert params["$offset"] == 2000
        assert params["$limit"] == 500
        assert "2024-01-01" in params["$where"]
        assert "2024-03-01" in params["$where"]

    @patch("ingest_bronze_311.requests.get")
    def test_fetch_page_http_error_returns_empty_list(self, mock_get):
        """Une erreur HTTP doit retourner une liste vide, pas planter le script."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = ingest.requests.exceptions.HTTPError("500")
        mock_get.return_value = mock_resp

        result = ingest.fetch_311_page(offset=0)

        assert result == []

    @patch("ingest_bronze_311.requests.get")
    def test_fetch_page_network_error_returns_empty_list(self, mock_get):
        """Une erreur réseau doit être interceptée proprement."""
        mock_get.side_effect = ingest.requests.exceptions.ConnectionError("timeout")

        result = ingest.fetch_311_page(offset=0)

        assert result == []


# ── Tests : clean_record ───────────────────────────────────────

class TestCleanRecord:

    def test_clean_complete_record_keeps_all_fields(self, fake_311_record_complete):
        """Un enregistrement complet doit conserver toutes les valeurs réelles."""
        cleaned = ingest.clean_record(fake_311_record_complete)

        assert cleaned["unique_key"] == "60123456"
        assert cleaned["borough"] == "BROOKLYN"
        assert cleaned["complaint_type"] == "Noise - Residential"

    def test_clean_record_fills_missing_fields_with_unknown(self, fake_311_record_missing_fields):
        """Les champs absents doivent être remplacés par 'unknown', pas plantés ni None."""
        cleaned = ingest.clean_record(fake_311_record_missing_fields)

        assert cleaned["unique_key"] == "60999999"
        assert cleaned["closed_date"] == "unknown"
        assert cleaned["borough"] == "unknown"
        assert cleaned["latitude"] == "unknown"

    def test_clean_record_only_keeps_whitelisted_fields(self, fake_311_record_complete):
        """Seuls les champs définis dans FIELDS_TO_KEEP doivent apparaître en sortie."""
        extra_field_record = {**fake_311_record_complete, "champ_inutile": "valeur_parasite"}

        cleaned = ingest.clean_record(extra_field_record)

        assert "champ_inutile" not in cleaned
        assert set(cleaned.keys()) == set(ingest.FIELDS_TO_KEEP)


# ── Tests : write_batch_to_dynamodb ─────────────────────────────

class TestWriteBatchToDynamoDB:

    @patch("ingest_bronze_311.table")
    def test_write_batch_success_counts_correctly(self, mock_table, fake_311_record_complete):
        """Un batch de 2 enregistrements valides doit retourner written=2."""
        mock_batch_writer = MagicMock()
        mock_table.batch_writer.return_value.__enter__.return_value = mock_batch_writer

        records = [fake_311_record_complete, {**fake_311_record_complete, "unique_key": "60123457"}]
        written = ingest.write_batch_to_dynamodb(records)

        assert written == 2
        assert mock_batch_writer.put_item.call_count == 2

    @patch("ingest_bronze_311.table")
    def test_write_batch_skips_records_without_unique_key(
        self, mock_table, fake_311_record_complete, fake_311_record_no_unique_key
    ):
        """Les enregistrements sans unique_key valide doivent être ignorés, pas plantés."""
        mock_batch_writer = MagicMock()
        mock_table.batch_writer.return_value.__enter__.return_value = mock_batch_writer

        records = [fake_311_record_complete, fake_311_record_no_unique_key]
        written = ingest.write_batch_to_dynamodb(records)

        assert written == 1  # seul le record complet est écrit
        assert mock_batch_writer.put_item.call_count == 1

    @patch("ingest_bronze_311.table")
    def test_write_batch_dynamodb_error_returns_partial_count(self, mock_table, fake_311_record_complete):
        """Une erreur DynamoDB pendant le batch doit retourner le compte déjà écrit, sans planter."""
        from botocore.exceptions import ClientError

        mock_table.batch_writer.side_effect = ClientError(
            error_response={"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Throttled"}},
            operation_name="BatchWriteItem",
        )

        written = ingest.write_batch_to_dynamodb([fake_311_record_complete])

        assert written == 0


# ── Tests : ingest_all_pages (logique de pagination) ────────────

class TestIngestAllPages:

    @patch("ingest_bronze_311.write_batch_to_dynamodb")
    @patch("ingest_bronze_311.fetch_311_page")
    @patch("ingest_bronze_311.time.sleep")  # on évite d'attendre réellement pendant les tests
    def test_pagination_stops_on_partial_page(self, mock_sleep, mock_fetch, mock_write):
        """
        Si une page retourne moins de résultats que PAGE_SIZE, c'est la
        dernière page — la pagination doit s'arrêter immédiatement après.
        """
        full_page = [{"unique_key": str(i)} for i in range(ingest.PAGE_SIZE)]
        partial_page = [{"unique_key": "last"}]  # dernière page, incomplète

        mock_fetch.side_effect = [full_page, partial_page]
        mock_write.return_value = 1

        result = ingest.ingest_all_pages()

        assert mock_fetch.call_count == 2
        assert result["pages"] == 2

    @patch("ingest_bronze_311.write_batch_to_dynamodb")
    @patch("ingest_bronze_311.fetch_311_page")
    def test_pagination_stops_on_empty_page(self, mock_fetch, mock_write):
        """Une page vide doit arrêter la pagination sans appeler write_batch dessus."""
        mock_fetch.return_value = []

        result = ingest.ingest_all_pages()

        assert result["total_fetched"] == 0
        mock_write.assert_not_called()

    @patch("ingest_bronze_311.write_batch_to_dynamodb")
    @patch("ingest_bronze_311.fetch_311_page")
    @patch("ingest_bronze_311.time.sleep")
    def test_offset_increments_correctly_between_pages(self, mock_sleep, mock_fetch, mock_write):
        """Vérifie que l'offset progresse bien de PAGE_SIZE entre deux appels."""
        full_page = [{"unique_key": str(i)} for i in range(ingest.PAGE_SIZE)]
        partial_page = [{"unique_key": "last"}]

        mock_fetch.side_effect = [full_page, partial_page]
        mock_write.return_value = 1

        ingest.ingest_all_pages()

        first_call_offset = mock_fetch.call_args_list[0][0][0]
        second_call_offset = mock_fetch.call_args_list[1][0][0]
        assert first_call_offset == 0
        assert second_call_offset == ingest.PAGE_SIZE
