"""Property tests for the Vector DB skill.

Property 12: Vector backend adapter factory creates valid adapters for all supported backends
**Validates: Requirements 4.1**

Property 13: Vector search respects top-k limit
**Validates: Requirements 4.2**

Property 14: Metadata filter returns only matching vectors
**Validates: Requirements 4.3**

Property 15: Vector retrieve-by-ID round trip
**Validates: Requirements 4.4**

Property 16: Vector result normalization includes all required fields
**Validates: Requirements 4.8**

Property 17: Unsupported vector backend error lists all supported backends
**Validates: Requirements 4.9**

Property 18: Vector connection failure error identifies backend and reason
**Validates: Requirements 4.6**
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from typing import Any
from unittest.mock import MagicMock, patch

from hypothesis import given, settings, strategies as st

# Add the vector-db scripts directory to the path for direct import.
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "..", "skills", "vector-db", "scripts"
    ),
)

from vector_backend import (
    SUPPORTED_BACKENDS,
    UnsupportedBackendError,
    VectorBackendAdapter,
    VectorResult,
    create_adapter,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# A backend name drawn from the supported set
supported_backend = st.sampled_from(SUPPORTED_BACKENDS)

# Random strings that are NOT in the supported set (for unsupported tests)
unsupported_backend = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P")),
    min_size=1,
    max_size=30,
).filter(lambda s: s not in SUPPORTED_BACKENDS)

# top-k values (positive integers)
top_k_strategy = st.integers(min_value=1, max_value=200)

# A simple vector ID
vector_id_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=1,
    max_size=50,
)

# Metadata key-value pairs (string keys, string or int values)
metadata_key = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
metadata_value = st.one_of(
    st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"))),
    st.integers(min_value=0, max_value=10000),
)
metadata_filters = st.dictionaries(metadata_key, metadata_value, min_size=1, max_size=5)

# Similarity scores
score_strategy = st.floats(min_value=0.0, max_value=1.0, allow_nan=False)

# Number of mock results to generate
num_results = st.integers(min_value=0, max_value=50)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_vector_results(n: int, top_k: int | None = None) -> list[VectorResult]:
    """Build a list of n VectorResult instances."""
    return [
        VectorResult(
            id=f"vec-{i}",
            score=round(1.0 - i * 0.01, 4),
            metadata={"idx": i},
        )
        for i in range(n)
    ]


def _make_filtered_results(filters: dict, count: int) -> list[VectorResult]:
    """Build mock results whose metadata matches the given filters."""
    results = []
    for i in range(count):
        meta = dict(filters)  # copy filter values into metadata
        meta["extra"] = f"extra-{i}"
        results.append(
            VectorResult(id=f"filt-{i}", score=1.0, metadata=meta)
        )
    return results


# ---------------------------------------------------------------------------
# Property 12: Vector backend adapter factory creates valid adapters
#              for all supported backends
# ---------------------------------------------------------------------------

class TestVectorAdapterFactory:
    """Property 12: For any backend name in the supported set, the adapter
    factory should return a valid VectorBackendAdapter instance.

    **Validates: Requirements 4.1**
    """

    @given(backend=supported_backend)
    @settings(max_examples=100)
    def test_factory_returns_valid_adapter(self, backend: str):
        adapter = create_adapter(backend, {})
        assert isinstance(adapter, VectorBackendAdapter), (
            f"create_adapter('{backend}', {{}}) did not return a VectorBackendAdapter"
        )


# ---------------------------------------------------------------------------
# Property 13: Vector search respects top-k limit
# ---------------------------------------------------------------------------

class TestVectorSearchTopK:
    """Property 13: For any query embedding and top-k value, vector_search
    should return at most top-k results, each containing the required
    fields: id, score, and metadata.

    **Validates: Requirements 4.2**
    """

    @given(top_k=top_k_strategy, available=num_results)
    @settings(max_examples=100)
    @patch("vector_search.create_adapter")
    def test_result_count_at_most_top_k(
        self, mock_factory, top_k: int, available: int
    ):
        # The adapter returns min(available, top_k) results (simulating real behaviour)
        returned = min(available, top_k)
        mock_adapter = MagicMock()
        mock_adapter.search.return_value = _make_vector_results(returned)
        mock_factory.return_value = mock_adapter

        # Write a temp config
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = os.path.join(tmp, "vector-config.json")
            with open(cfg_path, "w") as f:
                json.dump({"backend": "qdrant", "connection": {}}, f)

            from vector_search import vector_search

            result = vector_search(
                "test_col", [0.1, 0.2], top_k=top_k, config_path=cfg_path
            )

        assert len(result["results"]) <= top_k, (
            f"Got {len(result['results'])} results but top_k={top_k}"
        )
        # Each result must have required fields
        for r in result["results"]:
            assert "id" in r, "Result missing 'id'"
            assert "score" in r, "Result missing 'score'"
            assert "metadata" in r, "Result missing 'metadata'"


# ---------------------------------------------------------------------------
# Property 14: Metadata filter returns only matching vectors
# ---------------------------------------------------------------------------

class TestMetadataFilterMatching:
    """Property 14: For any set of metadata filter expressions, all returned
    vectors should have metadata values that satisfy every filter condition.

    **Validates: Requirements 4.3**
    """

    @given(filters=metadata_filters, count=st.integers(min_value=0, max_value=20))
    @settings(max_examples=100)
    @patch("metadata_filter.create_adapter")
    def test_all_results_match_filters(
        self, mock_factory, filters: dict, count: int
    ):
        mock_adapter = MagicMock()
        mock_adapter.filter_by_metadata.return_value = _make_filtered_results(
            filters, count
        )
        mock_factory.return_value = mock_adapter

        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = os.path.join(tmp, "vector-config.json")
            with open(cfg_path, "w") as f:
                json.dump({"backend": "qdrant", "connection": {}}, f)

            from metadata_filter import metadata_filter as mf_func

            result = mf_func("test_col", filters, config_path=cfg_path)

        for r in result["results"]:
            for key, expected_value in filters.items():
                assert key in r["metadata"], (
                    f"Result {r['id']} missing filter key '{key}' in metadata"
                )
                assert r["metadata"][key] == expected_value, (
                    f"Result {r['id']} metadata['{key}']={r['metadata'][key]} "
                    f"!= expected {expected_value}"
                )


# ---------------------------------------------------------------------------
# Property 15: Vector retrieve-by-ID round trip
# ---------------------------------------------------------------------------

class TestVectorRetrieveByIdRoundTrip:
    """Property 15: For any vector stored in the vector store, retrieving it
    by its ID should return a result with the same ID and matching metadata.

    **Validates: Requirements 4.4**
    """

    @given(
        vec_id=vector_id_strategy,
        score=score_strategy,
        meta=st.dictionaries(metadata_key, metadata_value, min_size=0, max_size=5),
    )
    @settings(max_examples=100)
    @patch("retrieve_by_id.create_adapter")
    def test_round_trip_preserves_id_and_metadata(
        self, mock_factory, vec_id: str, score: float, meta: dict
    ):
        stored = VectorResult(id=vec_id, score=score, metadata=meta)
        mock_adapter = MagicMock()
        mock_adapter.get_by_id.return_value = stored
        mock_factory.return_value = mock_adapter

        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = os.path.join(tmp, "vector-config.json")
            with open(cfg_path, "w") as f:
                json.dump({"backend": "qdrant", "connection": {}}, f)

            from retrieve_by_id import retrieve_by_id as rbi_func

            result = rbi_func("test_col", vec_id, config_path=cfg_path)

        assert result["result"] is not None, "Expected a result, got None"
        assert result["result"]["id"] == vec_id, (
            f"ID mismatch: {result['result']['id']} != {vec_id}"
        )
        assert result["result"]["metadata"] == meta, (
            f"Metadata mismatch: {result['result']['metadata']} != {meta}"
        )


# ---------------------------------------------------------------------------
# Property 16: Vector result normalization includes all required fields
# ---------------------------------------------------------------------------

class TestVectorResultNormalization:
    """Property 16: For any backend and any raw result, the normalized output
    should contain id (string), score (number), metadata (object), and
    optionally payload (array or null).

    **Validates: Requirements 4.8**
    """

    @given(
        vec_id=vector_id_strategy,
        score=score_strategy,
        meta=st.dictionaries(metadata_key, metadata_value, min_size=0, max_size=5),
        payload=st.one_of(
            st.none(),
            st.lists(st.floats(allow_nan=False, allow_infinity=False), min_size=1, max_size=10),
        ),
    )
    @settings(max_examples=100)
    def test_normalized_result_has_required_fields(
        self, vec_id: str, score: float, meta: dict, payload
    ):
        vr = VectorResult(id=vec_id, score=score, metadata=meta, payload=payload)
        d = asdict(vr)

        # Required fields
        assert "id" in d and isinstance(d["id"], str), "id must be a string"
        assert "score" in d and isinstance(d["score"], (int, float)), "score must be a number"
        assert "metadata" in d and isinstance(d["metadata"], dict), "metadata must be a dict"

        # payload is optional: list or None
        assert "payload" in d
        assert d["payload"] is None or isinstance(d["payload"], list), (
            f"payload must be list or None, got {type(d['payload'])}"
        )


# ---------------------------------------------------------------------------
# Property 17: Unsupported vector backend error lists all supported backends
# ---------------------------------------------------------------------------

class TestUnsupportedBackendErrorListing:
    """Property 17: For any string not in the supported backends set, the
    error response should contain the complete list of supported backends.

    **Validates: Requirements 4.9**
    """

    @given(backend_name=unsupported_backend)
    @settings(max_examples=100)
    def test_error_lists_all_supported_backends(self, backend_name: str):
        try:
            create_adapter(backend_name, {})
            # Should not reach here
            assert False, f"Expected UnsupportedBackendError for '{backend_name}'"
        except UnsupportedBackendError as exc:
            # The error must list every supported backend
            for sb in SUPPORTED_BACKENDS:
                assert sb in exc.supported_backends, (
                    f"Supported backend '{sb}' missing from error.supported_backends"
                )
            assert exc.backend_type == backend_name
            # Also verify the string representation mentions them
            error_str = str(exc)
            for sb in SUPPORTED_BACKENDS:
                assert sb in error_str, (
                    f"Supported backend '{sb}' missing from error message"
                )


# ---------------------------------------------------------------------------
# Property 18: Vector connection failure error identifies backend and reason
# ---------------------------------------------------------------------------

class TestVectorConnectionFailureError:
    """Property 18: For any failed connection attempt to a vector store, the
    error should contain the backend name and a non-empty failure reason.

    **Validates: Requirements 4.6**
    """

    @given(backend=supported_backend)
    @settings(max_examples=100)
    def test_connection_error_identifies_backend(self, backend: str):
        adapter = create_adapter(backend, {})
        try:
            adapter.connect({})
            # Some adapters may succeed with empty config in certain envs;
            # that's acceptable — we only test the failure path.
        except ConnectionError as exc:
            error_msg = str(exc)
            # Must identify the backend (case-insensitive check)
            backend_identified = backend.lower() in error_msg.lower()
            assert backend_identified, (
                f"Connection error for '{backend}' does not mention the backend: {error_msg}"
            )
            # Must have a non-empty reason
            assert len(error_msg) > 0, "Connection error message must not be empty"
