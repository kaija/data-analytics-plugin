"""Perform vector similarity search against the configured backend.

Loads backend configuration from vector-config.json, creates the
appropriate adapter via the factory, connects, and executes a
similarity search with the given query embedding and top-k parameter.

Requirements: 4.2, 4.7
"""

from __future__ import annotations

import importlib.util as _ilu
import json
import os as _os
import sys
from dataclasses import asdict

from vector_backend import (
    UnsupportedBackendError,
    VectorResult,
    create_adapter,
)

try:
    _fqo_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "..", "..", "scripts", "format-query-output.py")
    _fqo_spec = _ilu.spec_from_file_location("format_query_output", _fqo_path)
    _fqo_mod = _ilu.module_from_spec(_fqo_spec)
    _fqo_spec.loader.exec_module(_fqo_mod)
    format_query_output = _fqo_mod.format_query_output
except Exception:
    def format_query_output(source_type, result_set, query_timestamp=None):
        return json.dumps(result_set, indent=2)

DEFAULT_CONFIG_PATH = _os.path.join(
    _os.path.dirname(_os.path.abspath(__file__)),
    "..",
    "assets",
    "vector-config.json",
)

DEFAULT_TOP_K = 10


def load_config(config_path: str | None = None) -> dict:
    """Load and validate the vector backend configuration.

    Args:
        config_path: Path to vector-config.json. Defaults to the
            skill's assets directory.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the config file is malformed or missing required keys.
    """
    path = config_path or DEFAULT_CONFIG_PATH
    if not _os.path.isfile(path):
        raise FileNotFoundError(f"Vector config not found: {path}")

    with open(path, "r") as fh:
        try:
            config = json.load(fh)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Malformed vector config: {exc}") from exc

    if "backend" not in config:
        raise ValueError("Vector config missing required key: 'backend'")
    if "connection" not in config:
        raise ValueError("Vector config missing required key: 'connection'")

    return config


def vector_search(
    collection: str,
    query_embedding: list[float] | str,
    top_k: int = DEFAULT_TOP_K,
    config_path: str | None = None,
) -> dict:
    """Perform vector similarity search.

    Args:
        collection: Name of the collection / index to search.
        query_embedding: The query vector (list of floats) or a plain text
            query string (for backends with built-in embedding like AlloyDB).
        top_k: Number of nearest neighbours to return.
        config_path: Optional path to vector-config.json.

    Returns:
        Dictionary with 'collection', 'top_k', and 'results' keys.
    """
    config = load_config(config_path)
    # Inject collection into connection config so the adapter knows which
    # collection to target.
    conn = dict(config["connection"])
    conn.setdefault("collection", collection)
    conn.setdefault("index", collection)

    adapter = create_adapter(config["backend"], conn)
    adapter.connect(conn)

    results: list[VectorResult] = adapter.search(query_embedding, top_k=top_k)

    return {
        "collection": collection,
        "top_k": top_k,
        "results": [asdict(r) for r in results],
    }


def main() -> None:
    """Entry point for standalone invocation.

    Usage:
        vector_search.py <collection> '<query_embedding_json_or_text>' [top_k]

    The query can be either a JSON array of floats (raw embedding) or a
    plain text string (for backends with built-in embedding like AlloyDB).
    """
    if len(sys.argv) < 3:
        error = {
            "error": "missing_argument",
            "message": (
                "Usage: vector_search.py <collection> "
                "'<query_embedding_json_or_text>' [top_k]"
            ),
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)

    collection = sys.argv[1]

    # Try to parse as JSON embedding array; if that fails, treat as plain
    # text query (for backends with built-in embedding like AlloyDB).
    raw_query = sys.argv[2]
    try:
        query_embedding = json.loads(raw_query)
        if not isinstance(query_embedding, list):
            # Parsed JSON but not a list — treat as text query
            query_embedding = raw_query
    except json.JSONDecodeError:
        # Not valid JSON — treat as a plain text query string
        query_embedding = raw_query

    top_k = DEFAULT_TOP_K
    if len(sys.argv) >= 4:
        try:
            top_k = int(sys.argv[3])
        except ValueError:
            error = {
                "error": "invalid_argument",
                "message": f"top_k must be an integer, got: {sys.argv[3]}",
            }
            print(json.dumps(error), file=sys.stderr)
            sys.exit(1)

    try:
        result = vector_search(collection, query_embedding, top_k=top_k)
        print(format_query_output("vector", result["results"]))
    except FileNotFoundError as exc:
        error = {"error": "config_not_found", "message": str(exc)}
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)
    except ValueError as exc:
        error = {"error": "config_error", "message": str(exc)}
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)
    except UnsupportedBackendError as exc:
        error = {
            "error": "unsupported_backend",
            "message": str(exc),
            "supported_backends": exc.supported_backends,
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)
    except ConnectionError as exc:
        error = {"error": "connection_error", "message": str(exc)}
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        error = {"error": "vector_search_error", "message": str(exc)}
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
