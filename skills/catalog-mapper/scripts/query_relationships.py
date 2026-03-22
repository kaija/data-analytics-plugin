"""Query all relationships for a given asset URI.

Returns relationships where the asset appears as source or target.

Requirements: 6.2
"""

import importlib.util as _ilu
import json
import os
import os as _os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from catalog_persistence import CatalogError, load_catalog

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


def query_relationships(
    asset_uri: str,
    catalog_path: str = DEFAULT_CATALOG,
) -> dict:
    """Find all relationships where the given asset is source or target.

    Args:
        asset_uri: The asset URI to query.
        catalog_path: Path to the catalog JSON file.

    Returns:
        Dict with 'asset_uri', 'as_source' (list), 'as_target' (list),
        and 'total' (int).

    Raises:
        CatalogError: If the catalog file is malformed.
    """
    catalog = load_catalog(catalog_path)

    as_source = [
        rel for rel in catalog["relationships"] if rel.get("source") == asset_uri
    ]
    as_target = [
        rel for rel in catalog["relationships"] if rel.get("target") == asset_uri
    ]

    return {
        "asset_uri": asset_uri,
        "as_source": as_source,
        "as_target": as_target,
        "total": len(as_source) + len(as_target),
    }


def main() -> None:
    """Entry point for standalone invocation."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Query all relationships for a given asset URI."
    )
    parser.add_argument("asset_uri", help="Asset URI to query, e.g. athena://db.table")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help="Path to catalog JSON")
    args = parser.parse_args()

    try:
        result = query_relationships(args.asset_uri, args.catalog)
        result_set = result["as_source"] + result["as_target"]
        print(format_query_output("catalog", result_set))
    except CatalogError as exc:
        print(json.dumps(exc.to_dict()), file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as exc:
        print(json.dumps({"error": "file_not_found", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
