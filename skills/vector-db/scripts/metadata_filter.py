"""Filter vectors by metadata key-value expressions.

Loads backend configuration from vector-config.json, creates the
appropriate adapter via the factory, connects, and filters vectors
by the supplied metadata key-value pairs.

Requirements: 4.3, 4.7
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


def metadata_filter(
    collection: str,
    filters: dict,
    config_path: str | None = None,
) -> dict:
    """Filter vectors by metadata key-value expressions.

    Args:
        collection: Name of the collection / index to filter.
        filters: Dictionary of metadata key-value filter expressions.
        config_path: Optional path to vector-config.json.

    Returns:
        Dictionary with 'collection', 'filters', and 'results' keys.
    """
    config = load_config(config_path)
    conn = dict(config["connection"])
    conn.setdefault("collection", collection)
    conn.setdefault("index", collection)

    adapter = create_adapter(config["backend"], conn)
    adapter.connect(conn)

    results: list[VectorResult] = adapter.filter_by_metadata(filters)

    return {
        "collection": collection,
        "filters": filters,
        "results": [asdict(r) for r in results],
    }


def main() -> None:
    """Entry point for standalone invocation.

    Usage:
        metadata_filter.py <collection> '<filters_json>'
    """
    if len(sys.argv) < 3:
        error = {
            "error": "missing_argument",
            "message": "Usage: metadata_filter.py <collection> '<filters_json>'",
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)

    collection = sys.argv[1]

    try:
        filters = json.loads(sys.argv[2])
    except json.JSONDecodeError:
        error = {
            "error": "invalid_argument",
            "message": "filters must be a valid JSON object of key-value pairs",
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)

    if not isinstance(filters, dict):
        error = {
            "error": "invalid_argument",
            "message": "filters must be a JSON object (dictionary)",
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)

    try:
        result = metadata_filter(collection, filters)
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
        error = {"error": "metadata_filter_error", "message": str(exc)}
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
