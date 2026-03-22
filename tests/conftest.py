"""Shared pytest fixtures for the Data Analytics Skill Suite test suite.

Provides mock AWS clients, mock vector/graph backends, temp catalog files,
and sample data used across unit and property tests.
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Mock AWS clients
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_glue_client():
    """MagicMock configured to return sample Glue responses."""
    client = MagicMock()

    # get_databases
    client.get_databases.return_value = {
        "DatabaseList": [
            {"Name": "analytics_prod", "Description": "Production analytics database"},
            {"Name": "data_lake", "Description": "Data lake database"},
        ]
    }

    # get_tables
    client.get_tables.return_value = {
        "TableList": [
            {
                "Name": "events",
                "DatabaseName": "analytics_prod",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "event_id", "Type": "string"},
                        {"Name": "user_id", "Type": "string"},
                        {"Name": "timestamp", "Type": "bigint"},
                    ]
                },
                "PartitionKeys": [{"Name": "dt", "Type": "string"}],
            },
            {
                "Name": "users",
                "DatabaseName": "analytics_prod",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "user_id", "Type": "string"},
                        {"Name": "email", "Type": "string"},
                    ]
                },
                "PartitionKeys": [],
            },
        ]
    }

    # get_table
    client.get_table.return_value = {
        "Table": {
            "Name": "events",
            "DatabaseName": "analytics_prod",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "event_id", "Type": "string"},
                    {"Name": "user_id", "Type": "string"},
                    {"Name": "timestamp", "Type": "bigint"},
                ]
            },
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        }
    }

    return client


@pytest.fixture
def mock_athena_client():
    """MagicMock configured to return sample Athena query results."""
    client = MagicMock()

    # start_query_execution
    client.start_query_execution.return_value = {
        "QueryExecutionId": "test-query-id-12345"
    }

    # get_query_execution — succeeded state
    client.get_query_execution.return_value = {
        "QueryExecution": {
            "QueryExecutionId": "test-query-id-12345",
            "Status": {"State": "SUCCEEDED"},
            "Statistics": {
                "DataScannedInBytes": 1024,
                "EngineExecutionTimeInMillis": 250,
            },
        }
    }

    # get_query_results
    client.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                {
                    "Data": [
                        {"VarCharValue": "event_id"},
                        {"VarCharValue": "user_id"},
                        {"VarCharValue": "timestamp"},
                    ]
                },
                {
                    "Data": [
                        {"VarCharValue": "evt-001"},
                        {"VarCharValue": "usr-001"},
                        {"VarCharValue": "1700000000"},
                    ]
                },
                {
                    "Data": [
                        {"VarCharValue": "evt-002"},
                        {"VarCharValue": "usr-002"},
                        {"VarCharValue": "1700000001"},
                    ]
                },
                {
                    "Data": [
                        {"VarCharValue": "evt-003"},
                        {"VarCharValue": "usr-003"},
                        {"VarCharValue": "1700000002"},
                    ]
                },
            ],
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {"Name": "event_id", "Type": "varchar"},
                    {"Name": "user_id", "Type": "varchar"},
                    {"Name": "timestamp", "Type": "bigint"},
                ]
            },
        }
    }

    return client


# ---------------------------------------------------------------------------
# Mock vector backend
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_vector_adapter():
    """MagicMock VectorBackendAdapter with sample search results."""
    adapter = MagicMock()

    # search returns 3 VectorResult-like dicts (adapter is mocked, not real)
    adapter.search.return_value = [
        MagicMock(id="vec-001", score=0.95, metadata={"category": "sports"}, payload=None),
        MagicMock(id="vec-002", score=0.87, metadata={"category": "news"}, payload=None),
        MagicMock(id="vec-003", score=0.72, metadata={"category": "tech"}, payload=None),
    ]

    # filter_by_metadata returns 2 results
    adapter.filter_by_metadata.return_value = [
        MagicMock(id="vec-001", score=1.0, metadata={"category": "sports"}, payload=None),
        MagicMock(id="vec-004", score=1.0, metadata={"category": "sports"}, payload=None),
    ]

    # get_by_id returns a single result
    adapter.get_by_id.return_value = MagicMock(
        id="vec-001", score=1.0, metadata={"category": "sports"}, payload=None
    )

    # list_collections returns collection names
    adapter.list_collections.return_value = ["embeddings", "documents", "images"]

    return adapter


# ---------------------------------------------------------------------------
# Mock graph backend
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_graph_adapter():
    """MagicMock GraphBackendAdapter with sample query results."""
    adapter = MagicMock()

    # execute_query returns a GraphQueryResult-like mock
    sample_result = MagicMock()
    sample_result.nodes = [
        {"id": "node-001", "labels": ["Person"], "properties": {"name": "Alice", "age": 30}},
        {"id": "node-002", "labels": ["Person"], "properties": {"name": "Bob", "age": 25}},
    ]
    sample_result.relationships = [
        {
            "id": "rel-001",
            "type": "KNOWS",
            "source": "node-001",
            "target": "node-002",
            "properties": {"since": 2020},
        }
    ]
    sample_result.metadata = {"execution_time_ms": 42}
    adapter.execute_query.return_value = sample_result

    # list_node_labels
    adapter.list_node_labels.return_value = ["Person", "Company", "Product"]

    # list_relationship_types
    adapter.list_relationship_types.return_value = ["KNOWS", "WORKS_AT", "BOUGHT"]

    # get_properties
    schema_props = MagicMock()
    schema_props.label_or_type = "Person"
    schema_props.properties = [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "integer"},
    ]
    adapter.get_properties.return_value = schema_props

    # traverse
    traversal_result = MagicMock()
    traversal_result.start_node_id = "node-001"
    traversal_result.depth = 3
    traversal_result.nodes = sample_result.nodes
    traversal_result.relationships = sample_result.relationships
    adapter.traverse.return_value = traversal_result

    return adapter


# ---------------------------------------------------------------------------
# Temp catalog files
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_catalog_path(tmp_path):
    """Creates a temp catalog-map.json with an empty catalog, returns path."""
    catalog = {"assets": [], "relationships": []}
    path = tmp_path / "catalog-map.json"
    path.write_text(json.dumps(catalog), encoding="utf-8")
    return str(path)


@pytest.fixture
def populated_catalog_path(tmp_path):
    """Creates a temp catalog with 2 assets and 1 relationship, returns path."""
    catalog = {
        "assets": [
            {"uri": "athena://analytics_prod.events", "paradigm": "athena"},
            {"uri": "vector://embeddings", "paradigm": "vector"},
        ],
        "relationships": [
            {
                "id": "rel-fixture-001",
                "source": "athena://analytics_prod.events",
                "target": "vector://embeddings",
                "type": "derived_from",
                "created_at": "2024-01-01T00:00:00Z",
                "unresolved": False,
            }
        ],
    }
    path = tmp_path / "catalog-map.json"
    path.write_text(json.dumps(catalog), encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_athena_rows():
    """Returns list of 3 sample row dicts."""
    return [
        {"event_id": "evt-001", "user_id": "usr-001", "timestamp": 1700000000},
        {"event_id": "evt-002", "user_id": "usr-002", "timestamp": 1700000001},
        {"event_id": "evt-003", "user_id": "usr-003", "timestamp": 1700000002},
    ]


@pytest.fixture
def sample_vector_results():
    """Returns list of 3 VectorResult instances."""
    # Import lazily to avoid import errors if vector-db scripts path not set up
    import sys
    _scripts_path = os.path.join(
        os.path.dirname(__file__), "..", "skills", "vector-db", "scripts"
    )
    if _scripts_path not in sys.path:
        sys.path.insert(0, _scripts_path)

    from vector_backend import VectorResult

    return [
        VectorResult(id="vec-001", score=0.95, metadata={"category": "sports"}),
        VectorResult(id="vec-002", score=0.87, metadata={"category": "news"}),
        VectorResult(id="vec-003", score=0.72, metadata={"category": "tech"}),
    ]


@pytest.fixture
def sample_graph_result():
    """Returns a GraphQueryResult with 2 nodes and 1 relationship."""
    import sys
    _scripts_path = os.path.join(
        os.path.dirname(__file__), "..", "skills", "graph-db", "scripts"
    )
    if _scripts_path not in sys.path:
        sys.path.insert(0, _scripts_path)

    from graph_backend import GraphQueryResult

    return GraphQueryResult(
        nodes=[
            {"id": "node-001", "labels": ["Person"], "properties": {"name": "Alice"}},
            {"id": "node-002", "labels": ["Person"], "properties": {"name": "Bob"}},
        ],
        relationships=[
            {
                "id": "rel-001",
                "type": "KNOWS",
                "source": "node-001",
                "target": "node-002",
                "properties": {},
            }
        ],
    )
