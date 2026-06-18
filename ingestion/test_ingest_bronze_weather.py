"""
Tests unitaires — Script d'ingestion Bronze météo (Open-Meteo)
=================================================================
Valide la logique du script sans jamais faire de vrai appel réseau
ni de vrai appel AWS S3. Tout est mocké (requests, boto3).

Usage :
    pip install pytest
    pytest test_ingest_bronze_weather.py -v
"""

import sys
import os
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Variables d'environnement factices, nécessaires avant l'import du module
# (le module lit os.getenv() au niveau module, à l'import).
os.environ["AWS_ACCESS_KEY_ID"] = "fake_access_key_for_tests"
os.environ["AWS_SECRET_ACCESS_KEY"] = "fake_secret_key_for_tests"
os.environ["AWS_REGION"] = "eu-west-3"
os.environ["S3_BUCKET_BRONZE"] = "fake-bucket-for-tests"

import ingest_bronze_weather as ingest


# ── Fixtures ────────────────────────────────────────────────────

@pytest.fixture
def fake_weather_response_ok():
    """Simule une réponse Open-Meteo valide avec quelques points horaires."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {
        "latitude": 40.71,
        "longitude": -74.01,
        "timezone": "America/New_York",
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00", "2024-01-01T02:00"],
            "temperature_2m": [2.1, 1.8, 1.5],
            "precipitation": [0.0, 0.0, 0.2],
            "windspeed_10m": [12.3, 11.8, 13.1],
            "weathercode": [0, 0, 61],
        },
    }
    return mock_resp


@pytest.fixture
def fake_weather_response_api_error():
    """Simule une réponse Open-Meteo en erreur (paramètre invalide)."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {
        "error": True,
        "reason": "Cannot initialize WeatherVariable from invalid String value",
    }
    return mock_resp


@pytest.fixture
def fake_response_http_error():
    """Simule une erreur HTTP (ex: 400 Bad Request)."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = ingest.requests.exceptions.HTTPError(
        "400 Bad Request"
    )
    return mock_resp


# ── Tests : fetch_weather_data ─────────────────────────────────

class TestFetchWeatherData:

    @patch("ingest_bronze_weather.requests.get")
    def test_fetch_success_returns_data(self, mock_get, fake_weather_response_ok):
        """Une requête réussie doit retourner le dict JSON complet."""
        mock_get.return_value = fake_weather_response_ok

        result = ingest.fetch_weather_data("2024-01-01", "2024-01-31")

        assert result is not None
        assert "hourly" in result
        assert len(result["hourly"]["time"]) == 3
        mock_get.assert_called_once()

    @patch("ingest_bronze_weather.requests.get")
    def test_fetch_calls_correct_url_and_params(self, mock_get, fake_weather_response_ok):
        """Vérifie que les paramètres envoyés à l'API sont corrects (coordonnées NYC, dates)."""
        mock_get.return_value = fake_weather_response_ok

        ingest.fetch_weather_data("2024-01-01", "2024-01-31")

        call_args = mock_get.call_args
        assert call_args[0][0] == ingest.OPEN_METEO_URL
        params = call_args[1]["params"]
        assert params["latitude"] == ingest.NYC_LATITUDE
        assert params["longitude"] == ingest.NYC_LONGITUDE
        assert params["start_date"] == "2024-01-01"
        assert params["end_date"] == "2024-01-31"
        assert "temperature_2m" in params["hourly"]

    @patch("ingest_bronze_weather.requests.get")
    def test_fetch_api_error_returns_none(self, mock_get, fake_weather_response_api_error):
        """Une erreur applicative Open-Meteo (ex: variable invalide) doit retourner None."""
        mock_get.return_value = fake_weather_response_api_error

        result = ingest.fetch_weather_data("2024-01-01", "2024-01-31")

        assert result is None

    @patch("ingest_bronze_weather.requests.get")
    def test_fetch_http_error_returns_none(self, mock_get, fake_response_http_error):
        """Une erreur HTTP doit être interceptée sans planter le script."""
        mock_get.return_value = fake_response_http_error

        result = ingest.fetch_weather_data("2024-01-01", "2024-01-31")

        assert result is None

    @patch("ingest_bronze_weather.requests.get")
    def test_fetch_network_error_returns_none(self, mock_get):
        """Une erreur réseau (timeout, DNS...) doit être interceptée proprement."""
        mock_get.side_effect = ingest.requests.exceptions.ConnectionError("network down")

        result = ingest.fetch_weather_data("2024-01-01", "2024-01-31")

        assert result is None


# ── Tests : upload_to_s3 ───────────────────────────────────────

class TestUploadToS3:

    @patch("ingest_bronze_weather.s3_client")
    def test_upload_success(self, mock_s3, tmp_path):
        """Un upload réussi doit retourner True."""
        local_path = tmp_path / "weather_nyc_2024-01.json"
        local_path.write_text('{"fake": "data"}')

        result = ingest.upload_to_s3(local_path, "bronze/weather/year=2024/month=01/weather_nyc_2024-01.json")

        assert result is True
        mock_s3.upload_file.assert_called_once_with(
            str(local_path),
            ingest.S3_BUCKET,
            "bronze/weather/year=2024/month=01/weather_nyc_2024-01.json",
        )

    @patch("ingest_bronze_weather.s3_client")
    def test_upload_failure_returns_false(self, mock_s3, tmp_path):
        """Une erreur S3 doit être interceptée et retourner False."""
        from botocore.exceptions import ClientError

        mock_s3.upload_file.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Denied"}},
            operation_name="PutObject",
        )
        local_path = tmp_path / "weather_nyc_2024-01.json"
        local_path.write_text('{"fake": "data"}')

        result = ingest.upload_to_s3(local_path, "bronze/weather/fichier.json")

        assert result is False


# ── Tests : ingest_weather_period (logique métier + partitionnement) ──

class TestIngestWeatherPeriod:

    @patch("ingest_bronze_weather.upload_to_s3")
    @patch("ingest_bronze_weather.fetch_weather_data")
    def test_s3_key_partitioning_format(self, mock_fetch, mock_upload, fake_weather_response_ok, tmp_path, monkeypatch):
        """
        Vérifie que la clé S3 générée respecte le partitionnement
        year=YYYY/month=MM attendu par Glue Crawler.
        """
        monkeypatch.setattr(ingest, "LOCAL_TMP_DIR", tmp_path)
        mock_fetch.return_value = fake_weather_response_ok.json.return_value
        mock_upload.return_value = True

        ingest.ingest_weather_period("2024-01-01", "2024-01-31", "2024-01")

        called_s3_key = mock_upload.call_args[0][1]
        assert called_s3_key == "bronze/weather/year=2024/month=01/weather_nyc_2024-01.json"

    @patch("ingest_bronze_weather.upload_to_s3")
    @patch("ingest_bronze_weather.fetch_weather_data")
    def test_writes_valid_json_locally_before_upload(self, mock_fetch, mock_upload, fake_weather_response_ok, tmp_path, monkeypatch):
        """Le fichier JSON local écrit avant l'upload doit être un JSON valide et fidèle aux données reçues."""
        monkeypatch.setattr(ingest, "LOCAL_TMP_DIR", tmp_path)
        mock_fetch.return_value = fake_weather_response_ok.json.return_value
        mock_upload.return_value = True

        # On intercepte le moment de l'upload pour lire le fichier juste avant suppression
        written_content = {}

        def fake_upload(local_path, s3_key):
            written_content["data"] = json.loads(local_path.read_text(encoding="utf-8"))
            return True

        mock_upload.side_effect = fake_upload

        ingest.ingest_weather_period("2024-01-01", "2024-01-31", "2024-01")

        assert written_content["data"]["latitude"] == 40.71
        assert len(written_content["data"]["hourly"]["time"]) == 3

    @patch("ingest_bronze_weather.upload_to_s3")
    @patch("ingest_bronze_weather.fetch_weather_data")
    def test_failed_fetch_skips_upload(self, mock_fetch, mock_upload, tmp_path, monkeypatch):
        """Si la récupération météo échoue, on ne doit JAMAIS tenter l'upload."""
        monkeypatch.setattr(ingest, "LOCAL_TMP_DIR", tmp_path)
        mock_fetch.return_value = None

        status = ingest.ingest_weather_period("2024-01-01", "2024-01-31", "2024-01")

        mock_upload.assert_not_called()
        assert status["success"] is False
