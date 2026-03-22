"""List node labels and relationship types in the connected graph database.

Loads backend configuration from graph-config.json, creates the
appropriate adapter via the factory, connects, and returns all
node labels and relationship types in the graph.

Requirements: 5.4
"""

from __future__ import annotations

import argparse
import importlib.util as _ilu
import json
import os as _os
import sys

sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from graph_backend import UnsupportedBackendError, create_adapter

try:
    _fqo_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "..", "..", "scripts", "format-query-output.py")
    _fqo_spec = _ilu.spec_from_file_location("format_query_output", _fqo_path)
    _fqo_mod = _ilu.module_from_spec(_fqo_spec)
    _fqo_spec.loader.exec_module(_fqo_mod)
    format_query_output = _fqo_mod.format_query_output
except Exception:
    def format_query_output(source_type, result_set, query_timestamp=None):
        return json.dumps(result_set, indent=2)

DEFAULT_CONFIG = _os.path.join(
    _os.path.dirname(_os.path.abspath(__file__)),
    "..",
    "assets",
    "graph-config.json",
)


def load_config(config_path: str = DEFAULT_CONFIG) -> dict:
    """Load and validate the graph backend configuration.

    Args:
        config_path: Path to graph-config.json.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the config file is malformed or missing required keys.
    """
    if not _os.path.isfile(config_path):
        raise FileNotFoundError(f"Graph config not found: {config_path}")

    with open(config_path, "r") as fh:
        try:
            config = json.load(fh)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Malformed graph config: {exc}") from exc

    if "backend" not in config:
        raise ValueError("Graph config missing required key: 'backend'")
    if "connection" not in config:
        raise ValueError("Graph config missing required key: 'connection'")

    return config


def list_schema(config_path: str = DEFAULT_CONFIG) -> dict:
    """List all node labels and relationship types in the graph.

    Args:
        config_path: Path to graph-config.json.

    Returns:
        Dictionary with backend, node_labels, and relationship_types.
    """
    config = load_config(config_path)
    conn = dict(config["connection"])

    adapter = create_adapter(config["backend"], conn)
    adapter.connect(conn)

    node_labels = adapter.list_node_labels()
    relationship_types = adapter.list_relationship_types()

    return {
        "backend": config["backend"],
        "node_labels": node_labels,
        "relationship_types": relationship_types,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List node labels and relationship types in the graph database."
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help="Path to graph-config.json (default: assets/graph-config.json)",
    )
    args = parser.parse_args()

    try:
        result = list_schema(config_path=args.config)
        result_set = [{"node_labels": result["node_labels"], "relationship_types": result["relationship_types"]}]
        print(format_query_output("catalog", result_set))
    except FileNotFoundError as exc:
        print(json.dumps({"error": "config_not_found", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)
    except ValueError as exc:
        print(json.dumps({"error": "config_error", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)
    except UnsupportedBackendError as exc:
        print(
            json.dumps({
                "error": "unsupported_backend",
                "message": str(exc),
                "supported_backends": exc.supported_backends,
            }),
            file=sys.stderr,
        )
        sys.exit(1)
    except ConnectionError as exc:
        print(json.dumps({"error": "connection_error", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(json.dumps({"error": "list_schema_error", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
