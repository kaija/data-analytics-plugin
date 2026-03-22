"""Property tests for the Graph DB skill.

Property 19: Graph backend adapter factory creates valid adapters for all supported backends
**Validates: Requirements 5.1**

Property 20: Graph traversal respects depth limit
**Validates: Requirements 5.6**

Property 21: Graph result normalization includes all required fields
**Validates: Requirements 5.7**

Property 22: Graph connection failure error identifies backend and reason
**Validates: Requirements 5.9**
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from typing import Any
from unittest.mock import MagicMock, patch

from hypothesis import given, settings, strategies as st

# Add the graph-db scripts directory to the path for direct import.
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "..", "skills", "graph-db", "scripts"
    ),
)

from graph_backend import (
    SUPPORTED_BACKENDS,
    GraphBackendAdapter,
    GraphQueryResult,
    GraphTraversalResult,
    SchemaProperties,
    UnsupportedBackendError,
    create_adapter,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

supported_backend = st.sampled_from(SUPPORTED_BACKENDS)

depth_strategy = st.integers(min_value=1, max_value=10)

node_id_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=1,
    max_size=50,
)

label_strategy = st.from_regex(r"[A-Za-z][A-Za-z0-9_]{0,19}", fullmatch=True)
rel_type_strategy = st.from_regex(r"[A-Z][A-Z0-9_]{0,19}", fullmatch=True)

prop_key = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
prop_value = st.one_of(
    st.text(min_size=0, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"))),
    st.integers(min_value=0, max_value=9999),
)
properties_dict = st.dictionaries(prop_key, prop_value, min_size=0, max_size=5)


def node_strategy():
    """Strategy that generates a valid node dict."""
    return st.fixed_dictionaries({
        "id": node_id_strategy,
        "labels": st.lists(label_strategy, min_size=0, max_size=3),
        "properties": properties_dict,
    })


def relationship_strategy():
    """Strategy that generates a valid relationship dict."""
    return st.fixed_dictionaries({
        "id": node_id_strategy,
        "type": rel_type_strategy,
        "source": node_id_strategy,
        "target": node_id_strategy,
        "properties": properties_dict,
    })


# ---------------------------------------------------------------------------
# Property 19: Graph backend adapter factory creates valid adapters
#              for all supported backends
# ---------------------------------------------------------------------------

class TestGraphAdapterFactory:
    """Property 19: For any backend name in SUPPORTED_BACKENDS, create_adapter()
    should return a GraphBackendAdapter instance.

    **Validates: Requirements 5.1**
    """

    @given(backend=supported_backend)
    @settings(max_examples=100)
    def test_factory_returns_valid_adapter(self, backend: str):
        adapter = create_adapter(backend, {})
        assert isinstance(adapter, GraphBackendAdapter), (
            f"create_adapter('{backend}', {{}}) did not return a GraphBackendAdapter"
        )


# ---------------------------------------------------------------------------
# Property 20: Graph traversal respects depth limit
# ---------------------------------------------------------------------------

class TestGraphTraversalDepth:
    """Property 20: For any start_node_id and depth value (1-10),
    traverse_graph() should return a result where result["depth"] equals
    the requested depth.

    **Validates: Requirements 5.6**
    """

    @given(start_node_id=node_id_strategy, depth=depth_strategy)
    @settings(max_examples=100)
    @patch("traverse_graph.create_adapter")
    def test_traversal_result_depth_matches_request(
        self, mock_factory, start_node_id: str, depth: int
    ):
        mock_adapter = MagicMock()
        mock_adapter.traverse.return_value = GraphTraversalResult(
            start_node_id=start_node_id,
            depth=depth,
            nodes=[],
            relationships=[],
        )
        mock_factory.return_value = mock_adapter

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = os.path.join(tmp, "graph-config.json")
            with open(cfg_path, "w") as f:
                json.dump(
                    {
                        "backend": "neo4j",
                        "connection": {
                            "uri": "bolt://localhost:7687",
                            "username": "neo4j",
                            "password": "pw",
                        },
                    },
                    f,
                )

            from traverse_graph import traverse_graph

            result = traverse_graph(start_node_id, depth=depth, config_path=cfg_path)

        assert result["depth"] == depth, (
            f"Expected depth={depth}, got {result['depth']}"
        )


# ---------------------------------------------------------------------------
# Property 21: Graph result normalization includes all required fields
# ---------------------------------------------------------------------------

class TestGraphResultNormalization:
    """Property 21: For any GraphQueryResult with arbitrary nodes and
    relationships, the normalized output should contain nodes (list) and
    relationships (list). Each node must have id (str), labels (list),
    properties (dict). Each relationship must have id (str), type (str),
    source (str), target (str), properties (dict).

    **Validates: Requirements 5.7**
    """

    @given(
        nodes=st.lists(node_strategy(), min_size=0, max_size=10),
        relationships=st.lists(relationship_strategy(), min_size=0, max_size=10),
    )
    @settings(max_examples=100)
    @patch("execute_cypher.create_adapter")
    def test_normalized_result_has_required_fields(
        self, mock_factory, nodes: list[dict], relationships: list[dict]
    ):
        mock_adapter = MagicMock()
        mock_adapter.execute_query.return_value = GraphQueryResult(
            nodes=nodes,
            relationships=relationships,
        )
        mock_factory.return_value = mock_adapter

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = os.path.join(tmp, "graph-config.json")
            with open(cfg_path, "w") as f:
                json.dump(
                    {
                        "backend": "neo4j",
                        "connection": {
                            "uri": "bolt://localhost:7687",
                            "username": "neo4j",
                            "password": "pw",
                        },
                    },
                    f,
                )

            from execute_cypher import execute_cypher

            result = execute_cypher("MATCH (n) RETURN n", config_path=cfg_path)

        assert "nodes" in result and isinstance(result["nodes"], list), (
            "Result must contain 'nodes' as a list"
        )
        assert "relationships" in result and isinstance(result["relationships"], list), (
            "Result must contain 'relationships' as a list"
        )

        for node in result["nodes"]:
            assert "id" in node and isinstance(node["id"], str), (
                f"Node missing 'id' (str): {node}"
            )
            assert "labels" in node and isinstance(node["labels"], list), (
                f"Node missing 'labels' (list): {node}"
            )
            assert "properties" in node and isinstance(node["properties"], dict), (
                f"Node missing 'properties' (dict): {node}"
            )

        for rel in result["relationships"]:
            assert "id" in rel and isinstance(rel["id"], str), (
                f"Relationship missing 'id' (str): {rel}"
            )
            assert "type" in rel and isinstance(rel["type"], str), (
                f"Relationship missing 'type' (str): {rel}"
            )
            assert "source" in rel and isinstance(rel["source"], str), (
                f"Relationship missing 'source' (str): {rel}"
            )
            assert "target" in rel and isinstance(rel["target"], str), (
                f"Relationship missing 'target' (str): {rel}"
            )
            assert "properties" in rel and isinstance(rel["properties"], dict), (
                f"Relationship missing 'properties' (dict): {rel}"
            )


# ---------------------------------------------------------------------------
# Property 22: Graph connection failure error identifies backend and reason
# ---------------------------------------------------------------------------

class TestGraphConnectionFailureError:
    """Property 22: For any supported backend, when connect() fails (packages
    not installed), the ConnectionError message must contain the backend name
    and a non-empty reason.

    **Validates: Requirements 5.9**
    """

    # Map backend keys to the display names used in error messages
    _BACKEND_DISPLAY_NAMES: dict[str, list[str]] = {
        "neo4j": ["neo4j"],
        "neptune-gremlin": ["neptune", "gremlin"],
    }

    @given(backend=supported_backend)
    @settings(max_examples=100)
    def test_connection_error_identifies_backend_and_reason(self, backend: str):
        adapter = create_adapter(backend, {})
        try:
            adapter.connect({})
            # If connect() somehow succeeds (e.g. package installed), skip assertion
        except ConnectionError as exc:
            error_msg = str(exc).lower()
            # Must identify the backend — check the key itself or any display name token
            display_tokens = self._BACKEND_DISPLAY_NAMES.get(backend, [backend])
            backend_identified = any(token in error_msg for token in display_tokens)
            assert backend_identified, (
                f"ConnectionError for '{backend}' does not mention the backend "
                f"(checked tokens {display_tokens}): {str(exc)!r}"
            )
            # Must have a non-empty reason
            assert len(str(exc).strip()) > 0, (
                "ConnectionError message must not be empty"
            )
            # Reason should be present (message is longer than just the backend name)
            assert len(str(exc)) > len(backend), (
                f"ConnectionError message too short to contain a reason: {str(exc)!r}"
            )
