"""Register a relationship between two data assets in the catalog.

Loads the catalog, auto-registers source and target assets, then
registers the directed relationship between them.

Requirements: 6.1, 6.6, 6.7
"""

import importlib.util as _ilu
import json
import os
import os as _os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from catalog_persistence import (
    CatalogError,
    get_paradigm,
    load_catalog,
    register_asset,
    register_relationship,
    save_catalog,
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

DEFAULT_CATALOG = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "assets",
    "catalog-map.json",
)


def register_relationship_cmd(
    source_uri: str,
    target_uri: str,
    rel_type: str,
    description: str = "",
    catalog_path: str = DEFAULT_CATALOG,
) -> dict:
    """Register a relationship between two assets, auto-registering each asset.

    Args:
        source_uri: URI of the source asset, e.g. "athena://db.table".
        target_uri: URI of the target asset, e.g. "vector://my-collection".
        rel_type: Relationship type, e.g. "derived_from".
        description: Optional human-readable description.
        catalog_path: Path to the catalog JSON file.

    Returns:
        Dict with 'registered', 'relationship', 'source_asset', 'target_asset'.

    Raises:
        ValueError: If either URI is malformed.
        CatalogError: If the catalog file is malformed.
    """
    source_paradigm = get_paradigm(source_uri)
    target_paradigm = get_paradigm(target_uri)

    catalog = load_catalog(catalog_path)

    source_asset = register_asset(catalog, source_uri, source_paradigm)
    target_asset = register_asset(catalog, target_uri, target_paradigm)

    relationship = register_relationship(
        catalog, source_uri, target_uri, rel_type, description
    )

    save_catalog(catalog, catalog_path)

    return {
        "registered": True,
        "relationship": relationship,
        "source_asset": source_asset,
        "target_asset": target_asset,
    }


def main() -> None:
    """Entry point for standalone invocation."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Register a relationship between two data assets."
    )
    parser.add_argument("source_uri", help="Source asset URI, e.g. athena://db.table")
    parser.add_argument("target_uri", help="Target asset URI, e.g. vector://collection")
    parser.add_argument("rel_type", help="Relationship type, e.g. derived_from")
    parser.add_argument("description", nargs="?", default="", help="Optional description")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help="Path to catalog JSON")
    args = parser.parse_args()

    try:
        result = register_relationship_cmd(
            args.source_uri,
            args.target_uri,
            args.rel_type,
            args.description,
            args.catalog,
        )
        result_set = [result["relationship"]]
        print(format_query_output("catalog", result_set))
    except ValueError as exc:
        print(json.dumps({"error": "invalid_uri", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)
    except CatalogError as exc:
        print(json.dumps(exc.to_dict()), file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as exc:
        print(json.dumps({"error": "file_not_found", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
