"""Unit tests for graph tool scripts.

Tests load_config, execute_cypher, execute_gremlin, list_schema,
get_properties, and traverse_graph using mock adapters.

Requirements: 5.1–5.9
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# Add the graph-db scripts directory to the path for direct import.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "skills", "graph-db", "scripts"),
)

from graph_backend import GraphQueryResult, GraphTraversalResult, SchemaProperties


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(tmp_dir: str, backend: str = "neo4j", **conn_overrides) -> str:
    """Write a temporary graph-config.json and return its path."""
    conn = {"uri": "bolt://localhost:7687", "username": "neo4j", "password": "pw"}
    conn.update(conn_overrides)
    config = {"backend": backend, "connection": conn}
    path = os.path.join(tmp_dir, "graph-config.json")
    with open(path, "w") as fh:
        json.dump(config, fh)
    return path


def _make_mock_adapter(
    nodes=None,
    relationships=None,
    node_labels=None,
    relationship_types=None,
    schema_properties=None,
    traversal_result=None,
):
    """Create a mock adapter with pre-configured return values."""
    adapter = MagicMock()
    adapter.execute_query.return_value = GraphQueryResult(
        nodes=nodes or [],
        relationships=relationships or [],
    )
    adapter.list_node_labels.return_value = node_labels or []
    adapter.list_relationship_types.return_value = relationship_types or []
    adapter.get_properties.return_value = schema_properties or SchemaProperties(
        label_or_type="Person", properties=[]
    )
    adapter.traverse.return_value = traversal_result or GraphTraversalResult(
        start_node_id="1", depth=3
    )
    return adapter


# ===========================================================================
# execute_cypher — load_config
# ===========================================================================

class TestExecuteCypherLoadConfig:
    def setup_method(self):
        from execute_cypher import load_config
        self.load_config = load_config

    def test_loads_valid_config(self, tmp_path):
        path = _write_config(str(tmp_path))
        config = self.load_config(path)
        assert config["backend"] == "neo4j"
        assert "connection" in config

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError, match="Graph config not found"):
            self.load_config("/nonexistent/path/graph-config.json")

    def test_malformed_json_raises(self, tmp_path):
        path = os.path.join(str(tmp_path), "bad.json")
        with open(path, "w") as fh:
            fh.write("{not valid json")
        with pytest.raises(ValueError, match="Malformed graph config"):
            self.load_config(path)

    def test_missing_backend_key_raises(self, tmp_path):
        path = os.path.join(str(tmp_path), "no-backend.json")
        with open(path, "w") as fh:
            json.dump({"connection": {}}, fh)
        with pytest.raises(ValueError, match="'backend'"):
            self.load_config(path)

    def test_missing_connection_key_raises(self, tmp_path):
        path = os.path.join(str(tmp_path), "no-conn.json")
        with open(path, "w") as fh:
            json.dump({"backend": "neo4j"}, fh)
        with pytest.raises(ValueError, match="'connection'"):
            self.load_config(path)


# ===========================================================================
# execute_cypher — execute_cypher function
# ===========================================================================

class TestExecuteCypher:
    @patch("execute_cypher.create_adapter")
    def test_returns_nodes_and_relationships(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(
            nodes=[{"id": "1", "labels": ["Person"], "properties": {"name": "Alice"}}],
            relationships=[{"id": "r1", "type": "KNOWS", "source": "1", "target": "2", "properties": {}}],
        )
        mock_factory.return_value = mock_adapter

        from execute_cypher import execute_cypher
        result = execute_cypher("MATCH (n) RETURN n", config_path=config_path)

        assert result["backend"] == "neo4j"
        assert result["query"] == "MATCH (n) RETURN n"
        assert len(result["nodes"]) == 1
        assert result["nodes"][0]["id"] == "1"
        assert len(result["relationships"]) == 1
        assert result["relationships"][0]["type"] == "KNOWS"
        mock_adapter.connect.assert_called_once()
        mock_adapter.execute_query.assert_called_once_with("MATCH (n) RETURN n")

    @patch("execute_cypher.create_adapter")
    def test_empty_result(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter()
        mock_factory.return_value = mock_adapter

        from execute_cypher import execute_cypher
        result = execute_cypher("MATCH (n) RETURN n LIMIT 0", config_path=config_path)

        assert result["nodes"] == []
        assert result["relationships"] == []

    @patch("execute_cypher.create_adapter")
    def test_connection_error_propagates(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = MagicMock()
        mock_adapter.connect.side_effect = ConnectionError("Neo4j: failed to connect")
        mock_factory.return_value = mock_adapter

        from execute_cypher import execute_cypher
        with pytest.raises(ConnectionError, match="failed to connect"):
            execute_cypher("MATCH (n) RETURN n", config_path=config_path)

    def test_config_not_found_raises(self):
        from execute_cypher import execute_cypher
        with pytest.raises(FileNotFoundError):
            execute_cypher("MATCH (n) RETURN n", config_path="/no/such/file.json")

    @patch("execute_cypher.create_adapter")
    def test_metadata_defaults_to_empty_dict(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter()
        mock_adapter.execute_query.return_value = GraphQueryResult(nodes=[], relationships=[], metadata=None)
        mock_factory.return_value = mock_adapter

        from execute_cypher import execute_cypher
        result = execute_cypher("MATCH (n) RETURN n", config_path=config_path)

        assert result["metadata"] == {}


# ===========================================================================
# execute_gremlin — load_config
# ===========================================================================

class TestExecuteGremlinLoadConfig:
    def setup_method(self):
        from execute_gremlin import load_config
        self.load_config = load_config

    def test_loads_valid_config(self, tmp_path):
        path = _write_config(str(tmp_path), backend="neptune-gremlin",
                             uri="wss://neptune.example.com:8182/gremlin")
        config = self.load_config(path)
        assert config["backend"] == "neptune-gremlin"
        assert "connection" in config

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError, match="Graph config not found"):
            self.load_config("/nonexistent/path/graph-config.json")

    def test_malformed_json_raises(self, tmp_path):
        path = os.path.join(str(tmp_path), "bad.json")
        with open(path, "w") as fh:
            fh.write("{not valid json")
        with pytest.raises(ValueError, match="Malformed graph config"):
            self.load_config(path)

    def test_missing_backend_key_raises(self, tmp_path):
        path = os.path.join(str(tmp_path), "no-backend.json")
        with open(path, "w") as fh:
            json.dump({"connection": {}}, fh)
        with pytest.raises(ValueError, match="'backend'"):
            self.load_config(path)

    def test_missing_connection_key_raises(self, tmp_path):
        path = os.path.join(str(tmp_path), "no-conn.json")
        with open(path, "w") as fh:
            json.dump({"backend": "neptune-gremlin"}, fh)
        with pytest.raises(ValueError, match="'connection'"):
            self.load_config(path)


# ===========================================================================
# execute_gremlin — execute_gremlin function
# ===========================================================================

class TestExecuteGremlin:
    @patch("execute_gremlin.create_adapter")
    def test_returns_nodes_and_relationships(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path), backend="neptune-gremlin",
                                    uri="wss://neptune.example.com:8182/gremlin")
        mock_adapter = _make_mock_adapter(
            nodes=[{"id": "v1", "labels": ["person"], "properties": {"name": "Bob"}}],
            relationships=[],
        )
        mock_factory.return_value = mock_adapter

        from execute_gremlin import execute_gremlin
        result = execute_gremlin("g.V().toList()", config_path=config_path)

        assert result["backend"] == "neptune-gremlin"
        assert result["query"] == "g.V().toList()"
        assert len(result["nodes"]) == 1
        assert result["nodes"][0]["id"] == "v1"
        assert result["relationships"] == []
        mock_adapter.connect.assert_called_once()
        mock_adapter.execute_query.assert_called_once_with("g.V().toList()")

    @patch("execute_gremlin.create_adapter")
    def test_empty_result(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path), backend="neptune-gremlin",
                                    uri="wss://neptune.example.com:8182/gremlin")
        mock_adapter = _make_mock_adapter()
        mock_factory.return_value = mock_adapter

        from execute_gremlin import execute_gremlin
        result = execute_gremlin("g.V().limit(0).toList()", config_path=config_path)

        assert result["nodes"] == []
        assert result["relationships"] == []

    @patch("execute_gremlin.create_adapter")
    def test_connection_error_propagates(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path), backend="neptune-gremlin",
                                    uri="wss://neptune.example.com:8182/gremlin")
        mock_adapter = MagicMock()
        mock_adapter.connect.side_effect = ConnectionError("Neptune/Gremlin: failed to connect")
        mock_factory.return_value = mock_adapter

        from execute_gremlin import execute_gremlin
        with pytest.raises(ConnectionError, match="failed to connect"):
            execute_gremlin("g.V().toList()", config_path=config_path)

    def test_config_not_found_raises(self):
        from execute_gremlin import execute_gremlin
        with pytest.raises(FileNotFoundError):
            execute_gremlin("g.V().toList()", config_path="/no/such/file.json")

    @patch("execute_gremlin.create_adapter")
    def test_metadata_defaults_to_empty_dict(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path), backend="neptune-gremlin",
                                    uri="wss://neptune.example.com:8182/gremlin")
        mock_adapter = _make_mock_adapter()
        mock_adapter.execute_query.return_value = GraphQueryResult(nodes=[], relationships=[], metadata=None)
        mock_factory.return_value = mock_adapter

        from execute_gremlin import execute_gremlin
        result = execute_gremlin("g.V().toList()", config_path=config_path)

        assert result["metadata"] == {}


# ===========================================================================
# list_schema
# ===========================================================================

class TestListSchema:
    @patch("list_schema.create_adapter")
    def test_returns_node_labels_and_relationship_types(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(
            node_labels=["Person", "Movie"],
            relationship_types=["ACTED_IN", "DIRECTED"],
        )
        mock_factory.return_value = mock_adapter

        from list_schema import list_schema
        result = list_schema(config_path=config_path)

        assert result["backend"] == "neo4j"
        assert result["node_labels"] == ["Person", "Movie"]
        assert result["relationship_types"] == ["ACTED_IN", "DIRECTED"]
        mock_adapter.connect.assert_called_once()
        mock_adapter.list_node_labels.assert_called_once()
        mock_adapter.list_relationship_types.assert_called_once()

    @patch("list_schema.create_adapter")
    def test_empty_schema(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(node_labels=[], relationship_types=[])
        mock_factory.return_value = mock_adapter

        from list_schema import list_schema
        result = list_schema(config_path=config_path)

        assert result["node_labels"] == []
        assert result["relationship_types"] == []

    def test_config_not_found_raises(self):
        from list_schema import list_schema
        with pytest.raises(FileNotFoundError):
            list_schema(config_path="/no/such/file.json")

    @patch("list_schema.create_adapter")
    def test_connection_error_propagates(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = MagicMock()
        mock_adapter.connect.side_effect = ConnectionError("Neo4j: failed to connect")
        mock_factory.return_value = mock_adapter

        from list_schema import list_schema
        with pytest.raises(ConnectionError, match="failed to connect"):
            list_schema(config_path=config_path)

    @patch("list_schema.create_adapter")
    def test_neptune_gremlin_backend(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path), backend="neptune-gremlin",
                                    uri="wss://neptune.example.com:8182/gremlin")
        mock_adapter = _make_mock_adapter(
            node_labels=["person", "movie"],
            relationship_types=["knows", "created"],
        )
        mock_factory.return_value = mock_adapter

        from list_schema import list_schema
        result = list_schema(config_path=config_path)

        assert result["backend"] == "neptune-gremlin"
        assert result["node_labels"] == ["person", "movie"]
        assert result["relationship_types"] == ["knows", "created"]


# ===========================================================================
# get_properties
# ===========================================================================

class TestGetProperties:
    @patch("get_properties.create_adapter")
    def test_returns_schema_properties(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(
            schema_properties=SchemaProperties(
                label_or_type="Person",
                properties=[
                    {"name": "name", "type": "String"},
                    {"name": "age", "type": "Integer"},
                ],
            )
        )
        mock_factory.return_value = mock_adapter

        from get_properties import get_properties
        result = get_properties("Person", config_path=config_path)

        assert result["backend"] == "neo4j"
        assert result["label_or_type"] == "Person"
        assert len(result["properties"]) == 2
        prop_names = [p["name"] for p in result["properties"]]
        assert "name" in prop_names
        assert "age" in prop_names
        mock_adapter.connect.assert_called_once()
        mock_adapter.get_properties.assert_called_once_with("Person")

    @patch("get_properties.create_adapter")
    def test_empty_properties(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(
            schema_properties=SchemaProperties(label_or_type="EmptyNode", properties=[])
        )
        mock_factory.return_value = mock_adapter

        from get_properties import get_properties
        result = get_properties("EmptyNode", config_path=config_path)

        assert result["properties"] == []

    def test_config_not_found_raises(self):
        from get_properties import get_properties
        with pytest.raises(FileNotFoundError):
            get_properties("Person", config_path="/no/such/file.json")

    @patch("get_properties.create_adapter")
    def test_connection_error_propagates(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = MagicMock()
        mock_adapter.connect.side_effect = ConnectionError("Neo4j: failed to connect")
        mock_factory.return_value = mock_adapter

        from get_properties import get_properties
        with pytest.raises(ConnectionError, match="failed to connect"):
            get_properties("Person", config_path=config_path)

    @patch("get_properties.create_adapter")
    def test_relationship_type_properties(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(
            schema_properties=SchemaProperties(
                label_or_type="ACTED_IN",
                properties=[{"name": "roles", "type": "List"}],
            )
        )
        mock_factory.return_value = mock_adapter

        from get_properties import get_properties
        result = get_properties("ACTED_IN", config_path=config_path)

        assert result["label_or_type"] == "ACTED_IN"
        assert result["properties"][0]["name"] == "roles"


# ===========================================================================
# traverse_graph
# ===========================================================================

class TestTraverseGraph:
    @patch("traverse_graph.create_adapter")
    def test_returns_nodes_and_relationships(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(
            traversal_result=GraphTraversalResult(
                start_node_id="42",
                depth=2,
                nodes=[
                    {"id": "42", "labels": ["Person"], "properties": {}},
                    {"id": "43", "labels": ["Movie"], "properties": {}},
                ],
                relationships=[
                    {"id": "r1", "type": "ACTED_IN", "source": "42", "target": "43", "properties": {}},
                ],
            )
        )
        mock_factory.return_value = mock_adapter

        from traverse_graph import traverse_graph
        result = traverse_graph("42", depth=2, config_path=config_path)

        assert result["backend"] == "neo4j"
        assert result["start_node_id"] == "42"
        assert result["depth"] == 2
        assert len(result["nodes"]) == 2
        assert len(result["relationships"]) == 1
        mock_adapter.connect.assert_called_once()
        mock_adapter.traverse.assert_called_once_with("42", depth=2)

    @patch("traverse_graph.create_adapter")
    def test_depth_enforcement_matches_requested_depth(self, mock_factory, tmp_path):
        """result['depth'] must equal the requested depth value."""
        config_path = _write_config(str(tmp_path))
        for depth in [1, 3, 5, 10]:
            mock_adapter = _make_mock_adapter(
                traversal_result=GraphTraversalResult(
                    start_node_id="1", depth=depth, nodes=[], relationships=[]
                )
            )
            mock_factory.return_value = mock_adapter

            from traverse_graph import traverse_graph
            result = traverse_graph("1", depth=depth, config_path=config_path)

            assert result["depth"] == depth, f"Expected depth={depth}, got {result['depth']}"

    @patch("traverse_graph.create_adapter")
    def test_default_depth_is_three(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(
            traversal_result=GraphTraversalResult(start_node_id="1", depth=3)
        )
        mock_factory.return_value = mock_adapter

        from traverse_graph import traverse_graph
        result = traverse_graph("1", config_path=config_path)

        assert result["depth"] == 3
        mock_adapter.traverse.assert_called_once_with("1", depth=3)

    @patch("traverse_graph.create_adapter")
    def test_empty_traversal(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(
            traversal_result=GraphTraversalResult(start_node_id="99", depth=2)
        )
        mock_factory.return_value = mock_adapter

        from traverse_graph import traverse_graph
        result = traverse_graph("99", depth=2, config_path=config_path)

        assert result["nodes"] == []
        assert result["relationships"] == []

    def test_config_not_found_raises(self):
        from traverse_graph import traverse_graph
        with pytest.raises(FileNotFoundError):
            traverse_graph("1", config_path="/no/such/file.json")

    @patch("traverse_graph.create_adapter")
    def test_connection_error_propagates(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = MagicMock()
        mock_adapter.connect.side_effect = ConnectionError("Neo4j: failed to connect")
        mock_factory.return_value = mock_adapter

        from traverse_graph import traverse_graph
        with pytest.raises(ConnectionError, match="failed to connect"):
            traverse_graph("1", config_path=config_path)

    @patch("traverse_graph.create_adapter")
    def test_start_node_id_preserved_in_result(self, mock_factory, tmp_path):
        config_path = _write_config(str(tmp_path))
        mock_adapter = _make_mock_adapter(
            traversal_result=GraphTraversalResult(start_node_id="node-xyz", depth=1)
        )
        mock_factory.return_value = mock_adapter

        from traverse_graph import traverse_graph
        result = traverse_graph("node-xyz", depth=1, config_path=config_path)

        assert result["start_node_id"] == "node-xyz"
