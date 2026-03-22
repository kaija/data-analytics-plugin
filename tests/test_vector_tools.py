"""Unit tests for vector tool scripts.

Validates Requirements 4.2, 4.3, 4.4, 4.5, 4.7.
"""

import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# Add the vector-db scripts directory to the path for direct import.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "skills", "vector-db", "scripts"),
)

from vector_backend import VectorResult
from vector_search import load_config as vs_load_config, vector_search
from metadata_filter import metadata_filter
from retrieve_by_id import retrieve_by_id
from list_collections import list_collections


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(tmp_dir: str, backend: str = "qdrant", **conn_overrides) -> str:
    """Write a temporary vector-config.json and return its path."""
    conn = {"host": "localhost", "port": 6333}
    conn.update(conn_overrides)
    config = {"backend": backend, "connection": conn}
    path = os.path.join(tmp_dir, "vector-config.json")
    with open(path, "w") as fh:
        json.dump(config, fh)
    return path


def _make_mock_adapter(results=None, collections=None, get_by_id_result=None):
    """Create a mock adapter with pre-configured return values."""
    adapter = MagicMock()
    adapter.search.return_value = results or []
    adapter.filter_by_metadata.return_value = results or []
    adapter.get_by_id.return_value = get_by_id_result
    adapter.list_collections.return_value = collections or []
    return adapter


# ---------------------------------------------------------------------------
# load_config tests
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_loads_valid_config(self, tmp_path):
        path = _write_config(str(tmp_path))
        config = vs_load_config(path)
        assert config["backend"] == "qdrant"
        assert "connection" in config

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError, match="Vector config not found"):
            vs_load_config("/nonexistent/path/config.json")

    def test_malformed_json_raises(self, tmp_path):
        path = os.path.join(str(tmp_path), "bad.json")
        with open(path, "w") as fh:
            fh.write("{not valid json")
        with pytest.raises(ValueError, match="Malformed vector config"):
            vs_load_config(path)

    def test_missing_backend_key_raises(self, tmp_path):
        path = os.path.join(str(tmp_path), "no-backend.json")
        with open(path, "w") as fh:
            json.dump({"connection": {}}, fh)
        with pytest.raises(ValueError, match="'backend'"):
            vs_load_config(path)

    def test_missing_connection_key_raises(self, tmp_path):
        path = os.path.join(str(tmp_path), "no-conn.json")
        with open(path, "w") as fh:
            json.dump({"backend": "qdrant"}, fh)
        with pytest.raises(ValueError, match="'connection'"):
            vs_load_config(path)


# ---------------------------------------------------------------------------
# vector_search tests
# ---------------------------------------------------------------------------

class TestVectorSearch:
    @patch("vector_search.create_adapter")
    def test_returns_results(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(results=[
            VectorResult(id="v1", score=0.95, metadata={"tag": "a"}),
            VectorResult(id="v2", score=0.80, metadata={"tag": "b"}),
        ])
        mock_factory.return_value = mock_adapter

        result = vector_search("my_collection", [0.1, 0.2, 0.3], top_k=2, config_path=config_path)

        assert result["collection"] == "my_collection"
        assert result["top_k"] == 2
        assert len(result["results"]) == 2
        assert result["results"][0]["id"] == "v1"
        assert result["results"][0]["score"] == 0.95
        mock_adapter.connect.assert_called_once()
        mock_adapter.search.assert_called_once_with([0.1, 0.2, 0.3], top_k=2)

    @patch("vector_search.create_adapter")
    def test_empty_results(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(results=[])
        mock_factory.return_value = mock_adapter

        result = vector_search("col", [0.0], config_path=config_path)

        assert result["results"] == []

    def test_config_not_found(self):
        with pytest.raises(FileNotFoundError):
            vector_search("col", [0.1], config_path="/no/such/file.json")

    @patch("vector_search.create_adapter")
    def test_connection_error_propagates(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = MagicMock()
        mock_adapter.connect.side_effect = ConnectionError("Qdrant: failed to connect")
        mock_factory.return_value = mock_adapter

        with pytest.raises(ConnectionError, match="failed to connect"):
            vector_search("col", [0.1], config_path=config_path)


# ---------------------------------------------------------------------------
# metadata_filter tests
# ---------------------------------------------------------------------------

class TestMetadataFilter:
    @patch("metadata_filter.create_adapter")
    def test_returns_filtered_results(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(results=[
            VectorResult(id="v3", score=1.0, metadata={"category": "science"}),
        ])
        mock_factory.return_value = mock_adapter

        result = metadata_filter("my_col", {"category": "science"}, config_path=config_path)

        assert result["collection"] == "my_col"
        assert result["filters"] == {"category": "science"}
        assert len(result["results"]) == 1
        assert result["results"][0]["id"] == "v3"
        mock_adapter.filter_by_metadata.assert_called_once_with({"category": "science"})

    @patch("metadata_filter.create_adapter")
    def test_empty_filter_results(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(results=[])
        mock_factory.return_value = mock_adapter

        result = metadata_filter("col", {"key": "val"}, config_path=config_path)

        assert result["results"] == []

    def test_config_not_found(self):
        with pytest.raises(FileNotFoundError):
            metadata_filter("col", {}, config_path="/no/such/file.json")


# ---------------------------------------------------------------------------
# retrieve_by_id tests
# ---------------------------------------------------------------------------

class TestRetrieveById:
    @patch("retrieve_by_id.create_adapter")
    def test_returns_found_vector(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        found = VectorResult(id="vec-42", score=1.0, metadata={"label": "test"})
        mock_adapter = _make_mock_adapter(get_by_id_result=found)
        mock_factory.return_value = mock_adapter

        result = retrieve_by_id("my_col", "vec-42", config_path=config_path)

        assert result["collection"] == "my_col"
        assert result["vector_id"] == "vec-42"
        assert result["result"]["id"] == "vec-42"
        assert result["result"]["metadata"] == {"label": "test"}
        mock_adapter.get_by_id.assert_called_once_with("vec-42")

    @patch("retrieve_by_id.create_adapter")
    def test_returns_none_when_not_found(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(get_by_id_result=None)
        mock_factory.return_value = mock_adapter

        result = retrieve_by_id("col", "missing-id", config_path=config_path)

        assert result["result"] is None

    def test_config_not_found(self):
        with pytest.raises(FileNotFoundError):
            retrieve_by_id("col", "id", config_path="/no/such/file.json")


# ---------------------------------------------------------------------------
# list_collections tests
# ---------------------------------------------------------------------------

class TestListCollections:
    @patch("list_collections.create_adapter")
    def test_returns_collections(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(collections=["col_a", "col_b", "col_c"])
        mock_factory.return_value = mock_adapter

        result = list_collections(config_path=config_path)

        assert result["backend"] == "qdrant"
        assert result["collections"] == ["col_a", "col_b", "col_c"]
        mock_adapter.connect.assert_called_once()
        mock_adapter.list_collections.assert_called_once()

    @patch("list_collections.create_adapter")
    def test_empty_collections(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(collections=[])
        mock_factory.return_value = mock_adapter

        result = list_collections(config_path=config_path)

        assert result["collections"] == []

    def test_config_not_found(self):
        with pytest.raises(FileNotFoundError):
            list_collections(config_path="/no/such/file.json")

    @patch("list_collections.create_adapter")
    def test_connection_error_propagates(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = MagicMock()
        mock_adapter.connect.side_effect = ConnectionError("backend down")
        mock_factory.return_value = mock_adapter

        with pytest.raises(ConnectionError, match="backend down"):
            list_collections(config_path=config_path)
