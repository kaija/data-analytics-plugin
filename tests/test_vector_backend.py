"""Unit tests for the vector backend adapter interface and factory.

Validates Requirements 4.1, 4.6, 4.8, 4.9.
"""

import os
import sys

import pytest

# Add the vector-db scripts directory to the path for direct import.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "skills", "vector-db", "scripts"),
)

from vector_backend import (
    VectorBackendAdapter,
    VectorResult,
    UnsupportedBackendError,
    SUPPORTED_BACKENDS,
    create_adapter,
    PineconeAdapter,
    WeaviateAdapter,
    QdrantAdapter,
    ChromaDBAdapter,
    MilvusAdapter,
    PgvectorAdapter,
    AlloyDBAdapter,
)


# ---------------------------------------------------------------------------
# VectorResult data class
# ---------------------------------------------------------------------------

class TestVectorResult:
    def test_defaults(self):
        r = VectorResult(id="v1", score=0.95)
        assert r.id == "v1"
        assert r.score == 0.95
        assert r.metadata == {}
        assert r.payload is None

    def test_full_construction(self):
        r = VectorResult(
            id="v2",
            score=0.8,
            metadata={"colour": "blue"},
            payload=[0.1, 0.2, 0.3],
        )
        assert r.metadata == {"colour": "blue"}
        assert r.payload == [0.1, 0.2, 0.3]


# ---------------------------------------------------------------------------
# UnsupportedBackendError
# ---------------------------------------------------------------------------

class TestUnsupportedBackendError:
    def test_message_contains_backend_name(self):
        err = UnsupportedBackendError("foobar")
        assert "foobar" in str(err)

    def test_message_lists_supported_backends(self):
        err = UnsupportedBackendError("unknown")
        for backend in SUPPORTED_BACKENDS:
            assert backend in str(err)

    def test_attributes(self):
        err = UnsupportedBackendError("xyz")
        assert err.backend_type == "xyz"
        assert err.supported_backends == SUPPORTED_BACKENDS


# ---------------------------------------------------------------------------
# Factory — create_adapter
# ---------------------------------------------------------------------------

class TestCreateAdapter:
    @pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
    def test_creates_adapter_for_each_supported_backend(self, backend):
        adapter = create_adapter(backend, {})
        assert isinstance(adapter, VectorBackendAdapter)

    def test_pinecone_type(self):
        assert isinstance(create_adapter("pinecone", {}), PineconeAdapter)

    def test_weaviate_type(self):
        assert isinstance(create_adapter("weaviate", {}), WeaviateAdapter)

    def test_qdrant_type(self):
        assert isinstance(create_adapter("qdrant", {}), QdrantAdapter)

    def test_chromadb_type(self):
        assert isinstance(create_adapter("chromadb", {}), ChromaDBAdapter)

    def test_milvus_type(self):
        assert isinstance(create_adapter("milvus", {}), MilvusAdapter)

    def test_pgvector_type(self):
        assert isinstance(create_adapter("pgvector", {}), PgvectorAdapter)

    def test_alloydb_type(self):
        assert isinstance(create_adapter("alloydb", {}), AlloyDBAdapter)

    def test_unsupported_backend_raises(self):
        with pytest.raises(UnsupportedBackendError) as exc_info:
            create_adapter("redis", {})
        assert "redis" in str(exc_info.value)
        assert exc_info.value.supported_backends == SUPPORTED_BACKENDS

    def test_empty_string_raises(self):
        with pytest.raises(UnsupportedBackendError):
            create_adapter("", {})

    def test_case_sensitive(self):
        with pytest.raises(UnsupportedBackendError):
            create_adapter("Pinecone", {})


# ---------------------------------------------------------------------------
# Connection failure — descriptive errors (Req 4.6)
# ---------------------------------------------------------------------------

# Map of backend name -> (adapter class, expected package hint in error message)
_BACKEND_PACKAGE_HINTS = {
    "pinecone": (PineconeAdapter, "pinecone"),
    "weaviate": (WeaviateAdapter, "weaviate-client"),
    "qdrant": (QdrantAdapter, "qdrant-client"),
    "chromadb": (ChromaDBAdapter, "chromadb"),
    "milvus": (MilvusAdapter, "pymilvus"),
    "pgvector": (PgvectorAdapter, "psycopg2"),
    "alloydb": (AlloyDBAdapter, "psycopg2"),
}


class TestConnectionFailureErrors:
    """Adapters raise ConnectionError with backend name and reason when library is missing."""

    @pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
    def test_connect_raises_connection_error_with_backend_name(self, backend):
        adapter = create_adapter(backend, {})
        with pytest.raises(ConnectionError) as exc_info:
            adapter.connect({})
        error_msg = str(exc_info.value)
        # Error message must identify the backend
        adapter_cls, pkg_hint = _BACKEND_PACKAGE_HINTS[backend]
        assert backend.lower() in error_msg.lower() or adapter_cls.__name__.lower().replace("adapter", "") in error_msg.lower()

    @pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
    def test_connect_error_includes_install_hint(self, backend):
        adapter = create_adapter(backend, {})
        with pytest.raises(ConnectionError) as exc_info:
            adapter.connect({})
        error_msg = str(exc_info.value)
        _, pkg_hint = _BACKEND_PACKAGE_HINTS[backend]
        # Error should mention the package to install
        assert pkg_hint in error_msg


# ---------------------------------------------------------------------------
# RuntimeError when not connected (methods called before connect)
# ---------------------------------------------------------------------------

class TestNotConnectedRaisesRuntimeError:
    """All adapter methods raise RuntimeError when called before connect()."""

    @pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
    def test_search_raises_runtime_error(self, backend):
        adapter = create_adapter(backend, {})
        with pytest.raises(RuntimeError, match="not connected"):
            adapter.search([0.1, 0.2], top_k=5)

    @pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
    def test_filter_by_metadata_raises_runtime_error(self, backend):
        adapter = create_adapter(backend, {})
        with pytest.raises(RuntimeError, match="not connected"):
            adapter.filter_by_metadata({"key": "value"})

    @pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
    def test_get_by_id_raises_runtime_error(self, backend):
        adapter = create_adapter(backend, {})
        with pytest.raises(RuntimeError, match="not connected"):
            adapter.get_by_id("some-id")

    @pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
    def test_list_collections_raises_runtime_error(self, backend):
        adapter = create_adapter(backend, {})
        with pytest.raises(RuntimeError, match="not connected"):
            adapter.list_collections()


# ---------------------------------------------------------------------------
# Adapter __init__ sets _client to None
# ---------------------------------------------------------------------------

class TestAdapterInitState:
    """Each adapter starts with _client = None."""

    @pytest.mark.parametrize(
        "cls",
        [PineconeAdapter, WeaviateAdapter, QdrantAdapter, ChromaDBAdapter, MilvusAdapter, PgvectorAdapter, AlloyDBAdapter],
    )
    def test_initial_client_is_none(self, cls):
        adapter = cls()
        assert adapter._client is None
