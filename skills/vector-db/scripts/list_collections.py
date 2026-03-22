"""List available collections or indices in the connected vector store.

Loads backend configuration from vector-config.json, creates the
appropriate adapter via the factory, connects, and lists all
available collections / indices.

Requirements: 4.5, 4.7
"""

from __future__ import annotations

import importlib.util as _ilu
import json
import os as _os
import sys

from vector_backend import (
    UnsupportedBackendError,
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


def list_collections(config_path: str | None = None) -> dict:
    """List available collections or indices in the vector store.

    Args:
        config_path: Optional path to vector-config.json.

    Returns:
        Dictionary with 'backend' and 'collections' keys.
    """
    config = load_config(config_path)
    conn = dict(config["connection"])

    adapter = create_adapter(config["backend"], conn)
    adapter.connect(conn)

    collections: list[str] = adapter.list_collections()

    return {
        "backend": config["backend"],
        "collections": collections,
    }


def main() -> None:
    """Entry point for standalone invocation.

    Usage:
        list_collections.py
    """
    try:
        result = list_collections()
        result_set = [{"collection": c} for c in result["collections"]]
        print(format_query_output("vector", result_set))
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
        error = {"error": "list_collections_error", "message": str(exc)}
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
