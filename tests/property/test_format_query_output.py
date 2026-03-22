"""Property tests for the Generic Query Script (format-query-output.py).

Property 28: LLM-Friendly Format validation
**Validates: Requirements 7.1, 7.2, 7.3**

Property 29: Format output is valid JSON
**Validates: Requirements 7.4**

Property 30: Formatter accepts all four source types
**Validates: Requirements 7.5, 7.6, 7.7, 7.8**
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys

from hypothesis import given, settings, strategies as st

# ---------------------------------------------------------------------------
# Dynamic import of format-query-output.py (hyphen in filename)
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "format_query_output",
    os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "format-query-output.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

format_query_output = _mod.format_query_output
FormatError = _mod.FormatError
SUPPORTED_SOURCE_TYPES = _mod.SUPPORTED_SOURCE_TYPES

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Simple string keys / values for row dicts
_str_key = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
_scalar_value = st.one_of(
    st.text(min_size=0, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N", "P"))),
    st.integers(min_value=-10_000, max_value=10_000),
    st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
)

# Athena / Catalog: list of dicts with string keys and scalar values
_row_dict = st.dictionaries(_str_key, _scalar_value, min_size=1, max_size=8)
athena_result_set = st.lists(_row_dict, min_size=0, max_size=30)
catalog_result_set = athena_result_set

# Vector: list of dicts with id, score, metadata, payload
_vector_id = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")), min_size=1, max_size=40
)
_score = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
_metadata = st.dictionaries(_str_key, _scalar_value, min_size=0, max_size=5)
_payload = st.one_of(
    st.none(),
    st.lists(st.floats(allow_nan=False, allow_infinity=False), min_size=1, max_size=8),
)
_vector_item = st.fixed_dictionaries(
    {"id": _vector_id, "score": _score, "metadata": _metadata, "payload": _payload}
)
vector_result_set = st.lists(_vector_item, min_size=0, max_size=30)

# Graph: dict with 'nodes' list and 'relationships' list
_node_id = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")), min_size=1, max_size=20
)
_label = st.text(
    alphabet=st.characters(whitelist_categories=("L",)), min_size=1, max_size=15
)
_node = st.fixed_dictionaries(
    {
        "id": _node_id,
        "labels": st.lists(_label, min_size=0, max_size=4),
        "properties": st.dictionaries(_str_key, _scalar_value, min_size=0, max_size=5),
    }
)
graph_result_set = st.fixed_dictionaries(
    {
        "nodes": st.lists(_node, min_size=0, max_size=20),
        "relationships": st.lists(st.dictionaries(_str_key, _scalar_value), min_size=0, max_size=10),
    }
)

# Strategy that picks a source type and a matching result set
_source_and_result = st.one_of(
    st.tuples(st.just("athena"), athena_result_set),
    st.tuples(st.just("vector"), vector_result_set),
    st.tuples(st.just("graph"), graph_result_set),
    st.tuples(st.just("catalog"), catalog_result_set),
)


# ---------------------------------------------------------------------------
# Property 28: LLM-Friendly Format validation
# ---------------------------------------------------------------------------

class TestLLMFriendlyFormat:
    """Property 28: For any valid result set and source type, the output must
    have a 'header' with source_type, query_timestamp, result_count; a
    'schema' with 'fields' list; and a 'data' list.  result_count in header
    must equal len(data).

    **Validates: Requirements 7.1, 7.2, 7.3**
    """

    @given(source_and_result=_source_and_result)
    @settings(max_examples=100)
    def test_output_structure_and_result_count(self, source_and_result):
        source_type, result_set = source_and_result
        output_str = format_query_output(source_type, result_set)
        output = json.loads(output_str)

        # Top-level keys
        assert "header" in output, "output missing 'header'"
        assert "schema" in output, "output missing 'schema'"
        assert "data" in output, "output missing 'data'"

        # Header fields
        header = output["header"]
        assert "source_type" in header, "header missing 'source_type'"
        assert "query_timestamp" in header, "header missing 'query_timestamp'"
        assert "result_count" in header, "header missing 'result_count'"
        assert header["source_type"] == source_type

        # Schema has 'fields' list
        schema = output["schema"]
        assert "fields" in schema, "schema missing 'fields'"
        assert isinstance(schema["fields"], list), "'fields' must be a list"

        # Data is a list
        assert isinstance(output["data"], list), "'data' must be a list"

        # result_count == len(data)
        assert header["result_count"] == len(output["data"]), (
            f"result_count={header['result_count']} != len(data)={len(output['data'])}"
        )


# ---------------------------------------------------------------------------
# Property 29: Format output is valid JSON
# ---------------------------------------------------------------------------

class TestOutputIsValidJSON:
    """Property 29: For any valid result set and source type,
    format_query_output() returns a string that is valid JSON
    (json.loads() succeeds).

    **Validates: Requirements 7.4**
    """

    @given(source_and_result=_source_and_result)
    @settings(max_examples=100)
    def test_output_is_valid_json_string(self, source_and_result):
        source_type, result_set = source_and_result
        output_str = format_query_output(source_type, result_set)

        assert isinstance(output_str, str), "format_query_output must return a string"

        try:
            parsed = json.loads(output_str)
        except json.JSONDecodeError as exc:
            raise AssertionError(
                f"format_query_output returned invalid JSON for source_type='{source_type}': {exc}"
            ) from exc

        assert isinstance(parsed, dict), "parsed JSON must be a dict"


# ---------------------------------------------------------------------------
# Property 30: Formatter accepts all four source types
# ---------------------------------------------------------------------------

class TestAllSourceTypesAccepted:
    """Property 30: For each of the four source types (athena, vector, graph,
    catalog), format_query_output() succeeds without raising FormatError.

    **Validates: Requirements 7.5, 7.6, 7.7, 7.8**
    """

    @given(result_set=athena_result_set)
    @settings(max_examples=100)
    def test_athena_source_type_accepted(self, result_set):
        try:
            output_str = format_query_output("athena", result_set)
        except FormatError as exc:
            raise AssertionError(
                f"format_query_output raised FormatError for source_type='athena': {exc}"
            ) from exc
        assert isinstance(output_str, str)

    @given(result_set=vector_result_set)
    @settings(max_examples=100)
    def test_vector_source_type_accepted(self, result_set):
        try:
            output_str = format_query_output("vector", result_set)
        except FormatError as exc:
            raise AssertionError(
                f"format_query_output raised FormatError for source_type='vector': {exc}"
            ) from exc
        assert isinstance(output_str, str)

    @given(result_set=graph_result_set)
    @settings(max_examples=100)
    def test_graph_source_type_accepted(self, result_set):
        try:
            output_str = format_query_output("graph", result_set)
        except FormatError as exc:
            raise AssertionError(
                f"format_query_output raised FormatError for source_type='graph': {exc}"
            ) from exc
        assert isinstance(output_str, str)

    @given(result_set=catalog_result_set)
    @settings(max_examples=100)
    def test_catalog_source_type_accepted(self, result_set):
        try:
            output_str = format_query_output("catalog", result_set)
        except FormatError as exc:
            raise AssertionError(
                f"format_query_output raised FormatError for source_type='catalog': {exc}"
            ) from exc
        assert isinstance(output_str, str)
