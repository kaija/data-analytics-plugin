"""Get properties for a node label or relationship type in the graph database.

Loads backend configuration from graph-config.json, creates the
appropriate adapter via the factory, connects, and returns the
property schema for the given label or relationship type.

Requirements: 5.5
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


def get_properties(label_or_type: str, config_path: str = DEFAULT_CONFIG) -> dict:
    """Get properties for a node label or relationship type.

    Args:
        label_or_type: The node label or relationship type to inspect.
        config_path: Path to graph-config.json.

    Returns:
        Dictionary with backend, label_or_type, and properties list.
    """
    config = load_config(config_path)
    conn = dict(config["connection"])

    adapter = create_adapter(config["backend"], conn)
    adapter.connect(conn)

    schema = adapter.get_properties(label_or_type)

    return {
        "backend": config["backend"],
        "label_or_type": label_or_type,
        "properties": schema.properties,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Get properties for a node label or relationship type."
    )
    parser.add_argument("label_or_type", help="Node label or relationship type to inspect")
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help="Path to graph-config.json (default: assets/graph-config.json)",
    )
    args = parser.parse_args()

    try:
        result = get_properties(args.label_or_type, config_path=args.config)
        print(format_query_output("catalog", result["properties"]))
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
        print(json.dumps({"error": "get_properties_error", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
