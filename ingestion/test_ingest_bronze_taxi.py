"""
Tests unitaires — Script d'ingestion Bronze
=============================================
Ces tests valident la LOGIQUE du script sans jamais faire de vrai
appel réseau ni de vrai appel AWS S3. Tout est mocké (boto3, requests).

Usage :
    pip install pytest
    pytest test_ingest_bronze_taxi.py -v
"""

import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import pytest

# Permet d'importer le script à tester depuis le même dossier
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# On mocke les variables d'environnement AVANT d'importer le module,
# car le module lit os.getenv() au moment de l'import (niveau module).
os.environ["AWS_ACCESS_KEY_ID"] = "fake_access_key_for_tests"
os.environ["AWS_SECRET_ACCESS_KEY"] = "fake_secret_key_for_tests"
os.environ["AWS_REGION"] = "eu-west-3"
os.environ["S3_BUCKET_BRONZE"] = "fake-bucket-for-tests"

import ingest_bronze_taxi as ingest


# ── Fixtures ────────────────────────────────────────────────────

@pytest.fixture
def fake_response_ok():
    """Simule une réponse HTTP 200 avec un petit contenu Parquet bidon."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-length": "1024"}
    mock_resp.iter_content.return_value = [b"fake_parquet_bytes_chunk"]
    mock_resp.raise_for_status.return_value = None
    return mock_resp


@pytest.fixture
def fake_response_404():
    """Simule une réponse HTTP 404 (fichier pas encore publié par TLC)."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = ingest.requests.exceptions.HTTPError(
        "404 Not Found"
    )
    return mock_resp


# ── Tests : download_file ──────────────────────────────────────

class TestDownloadFile:

    @patch("ingest_bronze_taxi.requests.get")
    @patch("builtins.open", new_callable=mock_open)
    def test_download_success(self, mock_file, mock_get, fake_response_ok, tmp_path):
        """Un téléchargement réussi doit retourner True et écrire le fichier."""
        mock_get.return_value = fake_response_ok
        local_path = tmp_path / "yellow_tripdata_2024-01.parquet"

        result = ingest.download_file("http://fake-url.com/file.parquet", local_path)

        assert result is True
        mock_get.assert_called_once()
        mock_file.assert_called_once_with(local_path, "wb")

    @patch("ingest_bronze_taxi.requests.get")
    def test_download_404_returns_false(self, mock_get, fake_response_404, tmp_path):
        """Un fichier non publié (404) doit retourner False sans planter le script."""
        mock_get.return_value = fake_response_404
        local_path = tmp_path / "yellow_tripdata_2099-01.parquet"

        result = ingest.download_file("http://fake-url.com/file.parquet", local_path)

        assert result is False

    @patch("ingest_bronze_taxi.requests.get")
    def test_download_network_error_returns_false(self, mock_get, tmp_path):
        """Une erreur réseau (timeout, DNS...) doit être interceptée proprement."""
        mock_get.side_effect = ingest.requests.exceptions.ConnectionError("network down")
        local_path = tmp_path / "yellow_tripdata_2024-01.parquet"

        result = ingest.download_file("http://fake-url.com/file.parquet", local_path)

        assert result is False


# ── Tests : upload_to_s3 ───────────────────────────────────────

class TestUploadToS3:

    @patch("ingest_bronze_taxi.s3_client")
    def test_upload_success(self, mock_s3, tmp_path):
        """Un upload réussi doit retourner True et appeler boto3 avec les bons paramètres."""
        local_path = tmp_path / "fichier.parquet"
        local_path.write_text("contenu_bidon")

        result = ingest.upload_to_s3(local_path, "bronze/taxi/year=2024/month=01/fichier.parquet")

        assert result is True
        mock_s3.upload_file.assert_called_once_with(
            str(local_path),
            ingest.S3_BUCKET,
            "bronze/taxi/year=2024/month=01/fichier.parquet",
        )

    @patch("ingest_bronze_taxi.s3_client")
    def test_upload_failure_returns_false(self, mock_s3, tmp_path):
        """Une erreur S3 (permissions, bucket inexistant...) doit être interceptée."""
        from botocore.exceptions import ClientError

        mock_s3.upload_file.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Denied"}},
            operation_name="PutObject",
        )
        local_path = tmp_path / "fichier.parquet"
        local_path.write_text("contenu_bidon")

        result = ingest.upload_to_s3(local_path, "bronze/taxi/fichier.parquet")

        assert result is False


# ── Tests : partitionnement S3 (logique métier critique) ───────

class TestPartitioningLogic:

    @patch("ingest_bronze_taxi.upload_to_s3")
    @patch("ingest_bronze_taxi.download_file")
    def test_s3_key_partitioning_format(self, mock_download, mock_upload, tmp_path, monkeypatch):
        """
        Vérifie que la clé S3 générée respecte bien le partitionnement
        year=YYYY/month=MM/taxi_type=xxx attendu par Glue Crawler.
        """
        monkeypatch.setattr(ingest, "LOCAL_TMP_DIR", tmp_path)
        mock_download.return_value = True
        mock_upload.return_value = True

        ingest.ingest_taxi_month("yellow", "2024-01")

        called_s3_key = mock_upload.call_args[0][1]
        assert called_s3_key == (
            "bronze/taxi/year=2024/month=01/taxi_type=yellow/"
            "yellow_tripdata_2024-01.parquet"
        )

    @patch("ingest_bronze_taxi.upload_to_s3")
    @patch("ingest_bronze_taxi.download_file")
    def test_failed_download_skips_upload(self, mock_download, mock_upload, tmp_path, monkeypatch):
        """Si le téléchargement échoue, on ne doit JAMAIS tenter l'upload."""
        monkeypatch.setattr(ingest, "LOCAL_TMP_DIR", tmp_path)
        mock_download.return_value = False

        status = ingest.ingest_taxi_month("green", "2024-02")

        mock_upload.assert_not_called()
        assert status["success"] is False
