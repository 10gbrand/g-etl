"""Tester för centraliserad downloader."""

from unittest.mock import MagicMock, patch

import pytest

from g_etl.utils.downloader import (
    CHUNK_SIZE,
    PROGRESS_REPORT_INTERVAL,
    download_file_streaming,
    is_url,
)


class TestIsUrl:
    """Tester för is_url-funktionen."""

    def test_http_url(self):
        """HTTP-URL:er identifieras korrekt."""
        assert is_url("http://example.com/data.zip") is True

    def test_https_url(self):
        """HTTPS-URL:er identifieras korrekt."""
        assert is_url("https://example.com/data.zip") is True

    def test_local_absolute_path(self):
        """Absoluta lokala sökvägar är inte URL:er."""
        assert is_url("/Volumes/T9/data.zip") is False

    def test_local_relative_path(self):
        """Relativa lokala sökvägar är inte URL:er."""
        assert is_url("data/file.zip") is False

    def test_windows_path(self):
        """Windows-sökvägar är inte URL:er."""
        assert is_url("C:\\Users\\data.zip") is False

    def test_ftp_is_not_url(self):
        """FTP-protokoll räknas inte som URL (stöds inte)."""
        assert is_url("ftp://example.com/data.zip") is False

    def test_empty_string(self):
        """Tom sträng är inte en URL."""
        assert is_url("") is False

    def test_url_with_query_params(self):
        """URL med query-parametrar identifieras korrekt."""
        assert is_url("https://api.example.com/data?format=zip&v=2") is True


class TestDownloadFileStreaming:
    """Tester för download_file_streaming-funktionen."""

    @patch("g_etl.utils.downloader.requests.get")
    def test_downloads_file_to_temp(self, mock_get):
        """Laddar ner fil till temporär sökväg."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "100"}
        mock_response.iter_content.return_value = [b"test data"]
        mock_get.return_value = mock_response

        result = download_file_streaming("https://example.com/test.zip", suffix=".zip")

        assert result.exists()
        assert result.suffix == ".zip"
        assert result.read_bytes() == b"test data"

        # Cleanup
        result.unlink()

    @patch("g_etl.utils.downloader.requests.get")
    def test_uses_correct_timeout(self, mock_get):
        """Använder specificerad timeout."""
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "0"}
        mock_response.iter_content.return_value = []
        mock_get.return_value = mock_response

        download_file_streaming("https://example.com/test.zip", timeout=600)

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["timeout"] == 600

    @patch("g_etl.utils.downloader.requests.get")
    def test_calls_on_log(self, mock_get):
        """Anropar on_log callback med nedladdningsmeddelande."""
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "0"}
        mock_response.iter_content.return_value = []
        mock_get.return_value = mock_response

        log_messages = []
        download_file_streaming(
            "https://example.com/test.zip",
            on_log=lambda msg: log_messages.append(msg),
        )

        assert len(log_messages) == 1
        assert "https://example.com/test.zip" in log_messages[0]

    @patch("g_etl.utils.downloader.requests.get")
    def test_calls_on_progress_initially(self, mock_get):
        """Anropar on_progress med 0.0 initialt."""
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "0"}
        mock_response.iter_content.return_value = []
        mock_get.return_value = mock_response

        progress_calls = []
        download_file_streaming(
            "https://example.com/test.zip",
            on_progress=lambda p, m: progress_calls.append((p, m)),
        )

        assert len(progress_calls) >= 1
        assert progress_calls[0] == (0.0, "Ansluter...")

    @patch("g_etl.utils.downloader.requests.get")
    def test_progress_with_known_size(self, mock_get):
        """Rapporterar progress korrekt när filstorlek är känd."""
        mock_response = MagicMock()
        total_size = CHUNK_SIZE * PROGRESS_REPORT_INTERVAL * 2  # 2 progress updates
        mock_response.headers = {"content-length": str(total_size)}

        # Generera tillräckligt många chunks för att trigga progress
        chunks = [b"x" * CHUNK_SIZE] * (PROGRESS_REPORT_INTERVAL * 2)
        mock_response.iter_content.return_value = chunks
        mock_get.return_value = mock_response

        progress_calls = []
        download_file_streaming(
            "https://example.com/test.zip",
            on_progress=lambda p, m: progress_calls.append((p, m)),
            progress_weight=0.5,
        )

        # Ska ha initial + minst en progress-rapportering
        assert len(progress_calls) >= 2

        # Cleanup
        # (filen skapas men vi behöver inte ta bort den i detta test)

    @patch("g_etl.utils.downloader.requests.get")
    def test_progress_with_unknown_size(self, mock_get):
        """Rapporterar progress även utan content-length."""
        mock_response = MagicMock()
        mock_response.headers = {}  # Ingen content-length

        # Generera chunks
        chunks = [b"x" * CHUNK_SIZE] * (PROGRESS_REPORT_INTERVAL + 1)
        mock_response.iter_content.return_value = chunks
        mock_get.return_value = mock_response

        progress_calls = []
        download_file_streaming(
            "https://example.com/test.zip",
            on_progress=lambda p, m: progress_calls.append((p, m)),
        )

        # Ska ha fått progress-updates
        assert len(progress_calls) >= 2
        # Vid okänd storlek används progress_weight / 2
        assert any("MB" in msg for _, msg in progress_calls)

    @patch("g_etl.utils.downloader.requests.get")
    def test_raises_on_http_error(self, mock_get):
        """Kastar exception vid HTTP-fel."""
        import requests

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            download_file_streaming("https://example.com/notfound.zip")

    @patch("g_etl.utils.downloader.requests.get")
    def test_default_suffix(self, mock_get):
        """Använder .bin som default suffix."""
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "0"}
        mock_response.iter_content.return_value = []
        mock_get.return_value = mock_response

        result = download_file_streaming("https://example.com/test")

        assert result.suffix == ".bin"

    @patch("g_etl.utils.downloader.requests.get")
    def test_streams_with_correct_chunk_size(self, mock_get):
        """Använder korrekt chunk-storlek för streaming."""
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "0"}
        mock_response.iter_content.return_value = []
        mock_get.return_value = mock_response

        download_file_streaming("https://example.com/test.zip")

        mock_response.iter_content.assert_called_once_with(chunk_size=CHUNK_SIZE)


class TestDownloaderConstants:
    """Tester för modulkonstanter."""

    def test_chunk_size_is_reasonable(self):
        """CHUNK_SIZE är rimlig (mellan 1KB och 1MB)."""
        assert 1024 <= CHUNK_SIZE <= 1024 * 1024

    def test_progress_interval_is_reasonable(self):
        """PROGRESS_REPORT_INTERVAL ger uppdateringar varje ~100KB-1MB."""
        bytes_per_report = CHUNK_SIZE * PROGRESS_REPORT_INTERVAL
        # Ska vara mellan 100KB och 10MB
        assert 100 * 1024 <= bytes_per_report <= 10 * 1024 * 1024


class TestDownloaderImports:
    """Tester för bakåtkompatibilitet och imports."""

    def test_import_from_utils(self):
        """Funktioner kan importeras från g_etl.utils."""
        from g_etl.utils import download_file_streaming, is_url

        assert callable(download_file_streaming)
        assert callable(is_url)

    def test_import_from_downloader(self):
        """Funktioner kan importeras direkt från downloader."""
        from g_etl.utils.downloader import download_file_streaming, is_url

        assert callable(download_file_streaming)
        assert callable(is_url)
