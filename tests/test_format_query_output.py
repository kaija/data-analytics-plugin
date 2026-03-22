"""Unit tests for the Generic Query Script (format-query-output.py).

Validates: Requirements 7.1–7.9
"""

from __future__ import annotations

import importlib.util
import json
import os
import re

import pytest

# ---------------------------------------------------------------------------
# Dynamic import of format-query-output.py (hyphen in filename)
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "format_query_output",
    os.path.join(os.path.dirname(__file__), "..", "scripts", "format-query-output.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

format_query_output = _mod.format_query_output
FormatError = _mod.FormatError
SUPPORTED_SOURCE_TYPES = _mod.SUPPORTED_SOURCE_TYPES

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

ATHENA_DATA = [{"col1": "val1", "col2": 42}, {"col1": "val2", "col2": 99}]
VECTOR_DATA = [{"id": "v1", "score": 0.95, "metadata": {"tag": "a"}, "payload": None}]
GRAPH_DATA = {
    "nodes": [{"id": "n1", "labels": ["Person"], "properties": {"name": "Alice"}}],
    "relationships": [],
}
CATALOG_DATA = [{"uri": "athena://db.t", "paradigm": "athena"}]

ISO8601_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse(source_type, result_set, **kwargs):
    return json.loads(format_query_output(source_type, result_set, **kwargs))


# ---------------------------------------------------------------------------
# Tests: each source type with sample data (Req 7.5–7.8)
# ---------------------------------------------------------------------------

class TestAthenaSourceType:
    """Req 7.5 — athena source type is accepted and formatted correctly."""

    def test_output_structure(self):
        out = _parse("athena", ATHENA_DATA)
        assert "header" in out
        assert "schema" in out
        assert "data" in out

    def test_header_source_type(self):
        out = _parse("athena", ATHENA_DATA)
        assert out["header"]["source_type"] == "athena"

    def test_data_rows(self):
        out = _parse("athena", ATHENA_DATA)
        assert out["data"] == ATHENA_DATA

    def test_schema_fields_inferred(self):
        out = _parse("athena", ATHENA_DATA)
        fields = {f["name"]: f["type"] for f in out["schema"]["fields"]}
        assert fields["col1"] == "string"
        assert fields["col2"] == "integer"


class TestVectorSourceType:
    """Req 7.6 — vector source type is accepted and formatted correctly."""

    def test_output_structure(self):
        out = _parse("vector", VECTOR_DATA)
        assert "header" in out
        assert "schema" in out
        assert "data" in out

    def test_header_source_type(self):
        out = _parse("vector", VECTOR_DATA)
        assert out["header"]["source_type"] == "vector"

    def test_data_row_fields(self):
        out = _parse("vector", VECTOR_DATA)
        row = out["data"][0]
        assert row["id"] == "v1"
        assert row["score"] == 0.95
        assert row["metadata"] == {"tag": "a"}
        assert row["payload"] is None

    def test_schema_is_fixed(self):
        out = _parse("vector", VECTOR_DATA)
        names = [f["name"] for f in out["schema"]["fields"]]
        assert names == ["id", "score", "metadata", "payload"]


class TestGraphSourceType:
    """Req 7.7 — graph source type is accepted and formatted correctly."""

    def test_output_structure(self):
        out = _parse("graph", GRAPH_DATA)
        assert "header" in out
        assert "schema" in out
        assert "data" in out

    def test_header_source_type(self):
        out = _parse("graph", GRAPH_DATA)
        assert out["header"]["source_type"] == "graph"

    def test_node_mapped_to_row(self):
        out = _parse("graph", GRAPH_DATA)
        row = out["data"][0]
        assert row["id"] == "n1"
        assert row["labels"] == "Person"
        assert row["properties"] == {"name": "Alice"}

    def test_schema_fields(self):
        out = _parse("graph", GRAPH_DATA)
        names = [f["name"] for f in out["schema"]["fields"]]
        assert "id" in names
        assert "labels" in names
        assert "properties" in names


class TestCatalogSourceType:
    """Req 7.8 — catalog source type is accepted and formatted correctly."""

    def test_output_structure(self):
        out = _parse("catalog", CATALOG_DATA)
        assert "header" in out
        assert "schema" in out
        assert "data" in out

    def test_header_source_type(self):
        out = _parse("catalog", CATALOG_DATA)
        assert out["header"]["source_type"] == "catalog"

    def test_data_rows(self):
        out = _parse("catalog", CATALOG_DATA)
        assert out["data"] == CATALOG_DATA

    def test_schema_fields_inferred(self):
        out = _parse("catalog", CATALOG_DATA)
        names = [f["name"] for f in out["schema"]["fields"]]
        assert "uri" in names
        assert "paradigm" in names


# ---------------------------------------------------------------------------
# Tests: empty result sets (Req 7.3)
# ---------------------------------------------------------------------------

class TestEmptyResultSets:
    """Req 7.3 — empty result sets return header + schema + empty data array."""

    def test_athena_empty(self):
        out = _parse("athena", [])
        assert out["data"] == []
        assert out["schema"]["fields"] == []
        assert out["header"]["result_count"] == 0

    def test_vector_empty(self):
        out = _parse("vector", [])
        assert out["data"] == []
        assert isinstance(out["schema"]["fields"], list)
        assert out["header"]["result_count"] == 0

    def test_graph_empty_nodes(self):
        out = _parse("graph", {"nodes": [], "relationships": []})
        assert out["data"] == []
        assert isinstance(out["schema"]["fields"], list)
        assert out["header"]["result_count"] == 0

    def test_catalog_empty(self):
        out = _parse("catalog", [])
        assert out["data"] == []
        assert out["schema"]["fields"] == []
        assert out["header"]["result_count"] == 0


# ---------------------------------------------------------------------------
# Tests: unparseable input raises FormatError (Req 7.9)
# ---------------------------------------------------------------------------

class TestUnparseableInput:
    """Req 7.9 — unparseable input raises FormatError with source and details."""

    def test_athena_non_list_raises(self):
        with pytest.raises(FormatError) as exc_info:
            format_query_output("athena", "not a list")
        err = exc_info.value
        assert err.source_type == "athena"
        assert "list" in err.reason.lower()

    def test_athena_row_not_dict_raises(self):
        with pytest.raises(FormatError) as exc_info:
            format_query_output("athena", ["not", "dicts"])
        err = exc_info.value
        assert err.source_type == "athena"

    def test_vector_non_list_raises(self):
        with pytest.raises(FormatError) as exc_info:
            format_query_output("vector", 42)
        err = exc_info.value
        assert err.source_type == "vector"
        assert "list" in err.reason.lower()

    def test_graph_non_dict_raises(self):
        with pytest.raises(FormatError) as exc_info:
            format_query_output("graph", [1, 2, 3])
        err = exc_info.value
        assert err.source_type == "graph"
        assert "dict" in err.reason.lower()

    def test_graph_missing_nodes_key_raises(self):
        with pytest.raises(FormatError) as exc_info:
            format_query_output("graph", {"relationships": []})
        err = exc_info.value
        assert err.source_type == "graph"
        assert "nodes" in err.reason.lower()

    def test_format_error_str_contains_source_and_reason(self):
        with pytest.raises(FormatError) as exc_info:
            format_query_output("athena", "bad")
        err = exc_info.value
        msg = str(err)
        assert "athena" in msg
        assert err.reason in msg


# ---------------------------------------------------------------------------
# Tests: unknown source type raises FormatError (Req 7.9)
# ---------------------------------------------------------------------------

class TestUnknownSourceType:
    """Req 7.9 — unknown source type raises FormatError."""

    def test_unknown_source_raises(self):
        with pytest.raises(FormatError) as exc_info:
            format_query_output("mysql", [])
        err = exc_info.value
        assert err.source_type == "mysql"

    def test_empty_source_raises(self):
        with pytest.raises(FormatError) as exc_info:
            format_query_output("", [])
        assert exc_info.value.source_type == ""

    def test_supported_source_types_constant(self):
        assert SUPPORTED_SOURCE_TYPES == {"athena", "vector", "graph", "catalog"}


# ---------------------------------------------------------------------------
# Tests: query_timestamp auto-generation (Req 7.2)
# ---------------------------------------------------------------------------

class TestQueryTimestamp:
    """Req 7.2 — query_timestamp is ISO 8601 with Z suffix when auto-generated."""

    def test_auto_timestamp_iso8601_z(self):
        out = _parse("athena", ATHENA_DATA)
        ts = out["header"]["query_timestamp"]
        assert ISO8601_Z_RE.match(ts), f"timestamp '{ts}' is not ISO 8601 with Z suffix"

    def test_provided_timestamp_is_used(self):
        ts = "2024-06-15T12:00:00Z"
        out = _parse("athena", ATHENA_DATA, query_timestamp=ts)
        assert out["header"]["query_timestamp"] == ts

    def test_provided_timestamp_not_overridden(self):
        ts = "2000-01-01T00:00:00Z"
        out = _parse("vector", VECTOR_DATA, query_timestamp=ts)
        assert out["header"]["query_timestamp"] == ts


# ---------------------------------------------------------------------------
# Tests: result_count matches len(data) (Req 7.3)
# ---------------------------------------------------------------------------

class TestResultCount:
    """Req 7.3 — result_count in header equals len(data)."""

    def test_athena_result_count(self):
        out = _parse("athena", ATHENA_DATA)
        assert out["header"]["result_count"] == len(out["data"])
        assert out["header"]["result_count"] == 2

    def test_vector_result_count(self):
        out = _parse("vector", VECTOR_DATA)
        assert out["header"]["result_count"] == len(out["data"])
        assert out["header"]["result_count"] == 1

    def test_graph_result_count(self):
        out = _parse("graph", GRAPH_DATA)
        assert out["header"]["result_count"] == len(out["data"])
        assert out["header"]["result_count"] == 1

    def test_catalog_result_count(self):
        out = _parse("catalog", CATALOG_DATA)
        assert out["header"]["result_count"] == len(out["data"])
        assert out["header"]["result_count"] == 1

    def test_result_count_zero_for_empty(self):
        out = _parse("athena", [])
        assert out["header"]["result_count"] == 0
        assert out["data"] == []
