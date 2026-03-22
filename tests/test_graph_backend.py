"""Unit tests for graph backend adapters.

Tests connection failure error format, _ensure_connected guards,
factory behaviour, and result normalization using mock backends.

Requirements: 5.1, 5.7, 5.9
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers to import the module under test
# ---------------------------------------------------------------------------

sys.path.insert(0, "skills/graph-db/scripts")

from graph_backend import (  # noqa: E402
    GraphBackendAdapter,
    GraphQueryResult,
    GraphTraversalResult,
    NeptuneGremlinAdapter,
    Neo4jAdapter,
    SchemaProperties,
    UnsupportedBackendError,
    create_adapter,
)


# ===========================================================================
# Factory tests
# ===========================================================================

class TestCreateAdapter:
    def test_neo4j_returns_neo4j_adapter(self):
        adapter = create_adapter("neo4j", {})
        assert isinstance(adapter, Neo4jAdapter)

    def test_neptune_gremlin_returns_neptune_adapter(self):
        adapter = create_adapter("neptune-gremlin", {})
        assert isinstance(adapter, NeptuneGremlinAdapter)

    def test_unknown_backend_raises_unsupported_error(self):
        with pytest.raises(UnsupportedBackendError) as exc_info:
            create_adapter("unknown-db", {})
        assert "unknown-db" in str(exc_info.value)
        assert exc_info.value.backend_type == "unknown-db"

    def test_unsupported_error_lists_supported_backends(self):
        with pytest.raises(UnsupportedBackendError) as exc_info:
            create_adapter("baddb", {})
        assert "neo4j" in exc_info.value.supported_backends
        assert "neptune-gremlin" in exc_info.value.supported_backends


# ===========================================================================
# Neo4jAdapter — connection failure
# ===========================================================================

class TestNeo4jAdapterConnect:
    def test_missing_package_raises_connection_error(self):
        """ImportError → ConnectionError with install hint."""
        with patch.dict(sys.modules, {"neo4j": None}):
            adapter = Neo4jAdapter()
            with pytest.raises(ConnectionError) as exc_info:
                adapter.connect({"uri": "bolt://localhost:7687"})
        msg = str(exc_info.value)
        assert "Neo4j" in msg
        assert "neo4j" in msg
        assert "pip install neo4j" in msg

    def test_connection_error_wraps_reason(self):
        """Real connection failure → ConnectionError with backend name and reason."""
        mock_neo4j = types.ModuleType("neo4j")

        def bad_driver(uri, auth):
            raise Exception("Connection refused")

        mock_gdb = MagicMock()
        mock_gdb.driver.side_effect = bad_driver
        mock_neo4j.GraphDatabase = mock_gdb

        with patch.dict(sys.modules, {"neo4j": mock_neo4j}):
            adapter = Neo4jAdapter()
            with pytest.raises(ConnectionError) as exc_info:
                adapter.connect({"uri": "bolt://localhost:7687", "username": "neo4j", "password": "x"})
        msg = str(exc_info.value)
        assert "Neo4j" in msg
        assert "Connection refused" in msg

    def test_successful_connect_sets_client(self):
        mock_neo4j = types.ModuleType("neo4j")
        mock_driver = MagicMock()
        mock_gdb = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_neo4j.GraphDatabase = mock_gdb

        with patch.dict(sys.modules, {"neo4j": mock_neo4j}):
            adapter = Neo4jAdapter()
            adapter.connect({"uri": "bolt://localhost:7687", "username": "neo4j", "password": "pw"})

        assert adapter._client is mock_driver

    def test_default_database_is_neo4j(self):
        mock_neo4j = types.ModuleType("neo4j")
        mock_driver = MagicMock()
        mock_gdb = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_neo4j.GraphDatabase = mock_gdb

        with patch.dict(sys.modules, {"neo4j": mock_neo4j}):
            adapter = Neo4jAdapter()
            adapter.connect({"uri": "bolt://localhost:7687", "username": "neo4j", "password": "pw"})

        assert adapter._database == "neo4j"

    def test_custom_database_is_stored(self):
        mock_neo4j = types.ModuleType("neo4j")
        mock_driver = MagicMock()
        mock_gdb = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_neo4j.GraphDatabase = mock_gdb

        with patch.dict(sys.modules, {"neo4j": mock_neo4j}):
            adapter = Neo4jAdapter()
            adapter.connect({"uri": "bolt://localhost:7687", "username": "neo4j", "password": "pw", "database": "mydb"})

        assert adapter._database == "mydb"


# ===========================================================================
# Neo4jAdapter — _ensure_connected guard
# ===========================================================================

class TestNeo4jEnsureConnected:
    def _fresh(self) -> Neo4jAdapter:
        return Neo4jAdapter()

    def test_execute_query_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="Neo4jAdapter is not connected"):
            self._fresh().execute_query("MATCH (n) RETURN n")

    def test_list_node_labels_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="Neo4jAdapter is not connected"):
            self._fresh().list_node_labels()

    def test_list_relationship_types_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="Neo4jAdapter is not connected"):
            self._fresh().list_relationship_types()

    def test_get_properties_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="Neo4jAdapter is not connected"):
            self._fresh().get_properties("Person")

    def test_traverse_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="Neo4jAdapter is not connected"):
            self._fresh().traverse("1")


# ===========================================================================
# Neo4jAdapter — query execution with mock driver
# ===========================================================================

def _make_neo4j_adapter_with_mock_session(records):
    """Return a Neo4jAdapter whose _client is a mock that yields *records*."""
    adapter = Neo4jAdapter()
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter(records))
    mock_session.run.return_value = mock_result
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)
    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session
    adapter._client = mock_driver
    return adapter, mock_session


class TestNeo4jAdapterQueries:
    def test_list_node_labels_returns_strings(self):
        records = [{"label": "Person"}, {"label": "Movie"}]
        adapter, _ = _make_neo4j_adapter_with_mock_session(records)
        labels = adapter.list_node_labels()
        assert labels == ["Person", "Movie"]

    def test_list_relationship_types_returns_strings(self):
        records = [{"relationshipType": "ACTED_IN"}, {"relationshipType": "DIRECTED"}]
        adapter, _ = _make_neo4j_adapter_with_mock_session(records)
        types_ = adapter.list_relationship_types()
        assert types_ == ["ACTED_IN", "DIRECTED"]

    def test_execute_query_empty_returns_empty_result(self):
        adapter, _ = _make_neo4j_adapter_with_mock_session([])
        result = adapter.execute_query("MATCH (n) RETURN n LIMIT 0")
        assert isinstance(result, GraphQueryResult)
        assert result.nodes == []
        assert result.relationships == []

    def test_get_properties_fallback_returns_schema_properties(self):
        """When APOC raises, fallback MATCH query is used."""
        adapter = Neo4jAdapter()
        mock_driver = MagicMock()
        adapter._client = mock_driver

        call_count = [0]

        def session_factory(**kwargs):
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)

            def run_side_effect(query):
                call_count[0] += 1
                if "apoc" in query:
                    raise Exception("APOC not available")
                # Fallback: return keys
                mock_result = MagicMock()
                mock_result.__iter__ = MagicMock(return_value=iter([{"keys(n)": ["name", "age"]}]))
                return mock_result

            mock_session.run.side_effect = run_side_effect
            return mock_session

        mock_driver.session.side_effect = session_factory
        result = adapter.get_properties("Person")
        assert isinstance(result, SchemaProperties)
        assert result.label_or_type == "Person"
        assert any(p["name"] == "name" for p in result.properties)
        assert any(p["name"] == "age" for p in result.properties)

    def test_traverse_returns_traversal_result(self):
        adapter = Neo4jAdapter()
        adapter._client = MagicMock()

        # Patch execute_query to return a known result
        adapter.execute_query = MagicMock(return_value=GraphQueryResult(
            nodes=[{"id": "1", "labels": ["Person"], "properties": {}}],
            relationships=[],
        ))

        result = adapter.traverse("1", depth=2)
        assert isinstance(result, GraphTraversalResult)
        assert result.start_node_id == "1"
        assert result.depth == 2
        assert len(result.nodes) == 1


# ===========================================================================
# NeptuneGremlinAdapter — connection failure
# ===========================================================================

class TestNeptuneGremlinAdapterConnect:
    def test_missing_package_raises_connection_error(self):
        with patch.dict(sys.modules, {
            "gremlin_python": None,
            "gremlin_python.driver": None,
            "gremlin_python.driver.client": None,
        }):
            adapter = NeptuneGremlinAdapter()
            with pytest.raises(ConnectionError) as exc_info:
                adapter.connect({"uri": "wss://localhost:8182/gremlin"})
        msg = str(exc_info.value)
        assert "Neptune/Gremlin" in msg
        assert "gremlinpython" in msg
        assert "pip install gremlinpython" in msg

    def test_connection_error_wraps_reason(self):
        mock_gremlin = types.ModuleType("gremlin_python")
        mock_driver_pkg = types.ModuleType("gremlin_python.driver")
        mock_client_mod = types.ModuleType("gremlin_python.driver.client")

        class BadClient:
            def __init__(self, *args, **kwargs):
                raise Exception("wss handshake failed")

        mock_client_mod.Client = BadClient
        mock_driver_pkg.client = mock_client_mod
        mock_gremlin.driver = mock_driver_pkg

        with patch.dict(sys.modules, {
            "gremlin_python": mock_gremlin,
            "gremlin_python.driver": mock_driver_pkg,
            "gremlin_python.driver.client": mock_client_mod,
        }):
            adapter = NeptuneGremlinAdapter()
            with pytest.raises(ConnectionError) as exc_info:
                adapter.connect({"uri": "wss://localhost:8182/gremlin"})
        msg = str(exc_info.value)
        assert "Neptune/Gremlin" in msg
        assert "wss handshake failed" in msg

    def test_successful_connect_sets_client(self):
        mock_gremlin = types.ModuleType("gremlin_python")
        mock_driver_pkg = types.ModuleType("gremlin_python.driver")
        mock_client_mod = types.ModuleType("gremlin_python.driver.client")
        mock_instance = MagicMock()
        mock_client_mod.Client = MagicMock(return_value=mock_instance)
        mock_driver_pkg.client = mock_client_mod
        mock_gremlin.driver = mock_driver_pkg

        with patch.dict(sys.modules, {
            "gremlin_python": mock_gremlin,
            "gremlin_python.driver": mock_driver_pkg,
            "gremlin_python.driver.client": mock_client_mod,
        }):
            adapter = NeptuneGremlinAdapter()
            adapter.connect({"uri": "wss://neptune.example.com:8182/gremlin"})

        assert adapter._client is mock_instance


# ===========================================================================
# NeptuneGremlinAdapter — _ensure_connected guard
# ===========================================================================

class TestNeptuneGremlinEnsureConnected:
    def _fresh(self) -> NeptuneGremlinAdapter:
        return NeptuneGremlinAdapter()

    def test_execute_query_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="NeptuneGremlinAdapter is not connected"):
            self._fresh().execute_query("g.V().toList()")

    def test_list_node_labels_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="NeptuneGremlinAdapter is not connected"):
            self._fresh().list_node_labels()

    def test_list_relationship_types_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="NeptuneGremlinAdapter is not connected"):
            self._fresh().list_relationship_types()

    def test_get_properties_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="NeptuneGremlinAdapter is not connected"):
            self._fresh().get_properties("person")

    def test_traverse_raises_if_not_connected(self):
        with pytest.raises(RuntimeError, match="NeptuneGremlinAdapter is not connected"):
            self._fresh().traverse("v1")


# ===========================================================================
# NeptuneGremlinAdapter — query execution with mock client
# ===========================================================================

def _make_gremlin_adapter(submit_return: list) -> NeptuneGremlinAdapter:
    adapter = NeptuneGremlinAdapter()
    mock_client = MagicMock()
    mock_result_set = MagicMock()
    mock_result_set.all.return_value.result.return_value = submit_return
    mock_client.submit.return_value = mock_result_set
    adapter._client = mock_client
    return adapter


class TestNeptuneGremlinAdapterQueries:
    def test_list_node_labels_returns_strings(self):
        adapter = _make_gremlin_adapter(["person", "movie"])
        labels = adapter.list_node_labels()
        assert labels == ["person", "movie"]

    def test_list_relationship_types_returns_strings(self):
        adapter = _make_gremlin_adapter(["knows", "created"])
        types_ = adapter.list_relationship_types()
        assert types_ == ["knows", "created"]

    def test_execute_query_empty_returns_empty_result(self):
        adapter = _make_gremlin_adapter([])
        result = adapter.execute_query("g.V().limit(0).toList()")
        assert isinstance(result, GraphQueryResult)
        assert result.nodes == []
        assert result.relationships == []

    def test_execute_query_normalizes_vertex(self):
        vertex = {"type": "vertex", "id": "v1", "label": "person", "properties": {"name": [{"value": "Alice"}]}}
        adapter = _make_gremlin_adapter([vertex])
        result = adapter.execute_query("g.V().toList()")
        assert len(result.nodes) == 1
        node = result.nodes[0]
        assert node["id"] == "v1"
        assert "person" in node["labels"]
        assert node["properties"]["name"] == "Alice"

    def test_execute_query_normalizes_edge(self):
        edge = {"type": "edge", "id": "e1", "label": "knows", "outV": "v1", "inV": "v2"}
        adapter = _make_gremlin_adapter([edge])
        result = adapter.execute_query("g.E().toList()")
        assert len(result.relationships) == 1
        rel = result.relationships[0]
        assert rel["id"] == "e1"
        assert rel["type"] == "knows"
        assert rel["source"] == "v1"
        assert rel["target"] == "v2"

    def test_get_properties_returns_schema_properties(self):
        value_map = {"name": ["Alice"], "age": [30]}
        adapter = _make_gremlin_adapter([value_map])
        result = adapter.get_properties("person")
        assert isinstance(result, SchemaProperties)
        assert result.label_or_type == "person"
        prop_names = [p["name"] for p in result.properties]
        assert "name" in prop_names
        assert "age" in prop_names

    def test_traverse_returns_traversal_result(self):
        adapter = _make_gremlin_adapter([])
        result = adapter.traverse("v1", depth=2)
        assert isinstance(result, GraphTraversalResult)
        assert result.start_node_id == "v1"
        assert result.depth == 2

    def test_traverse_uses_correct_depth(self):
        adapter = _make_gremlin_adapter([])
        mock_client = adapter._client
        adapter.traverse("v42", depth=5)
        call_args = mock_client.submit.call_args[0][0]
        assert "5" in call_args
        assert "v42" in call_args


# ===========================================================================
# Result data class structure
# ===========================================================================

class TestDataClasses:
    def test_graph_query_result_defaults(self):
        r = GraphQueryResult()
        assert r.nodes == []
        assert r.relationships == []
        assert r.metadata is None

    def test_schema_properties_defaults(self):
        s = SchemaProperties(label_or_type="Foo")
        assert s.label_or_type == "Foo"
        assert s.properties == []

    def test_graph_traversal_result_defaults(self):
        t = GraphTraversalResult(start_node_id="1", depth=3)
        assert t.nodes == []
        assert t.relationships == []
