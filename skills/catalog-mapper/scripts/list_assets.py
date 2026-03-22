"""List all registered assets in the catalog.

Returns every asset with its URI and paradigm type.

Requirements: 6.3
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


def list_assets(catalog_path: str = DEFAULT_CATALOG) -> dict:
    """Return all assets registered in the catalog.

    Args:
        catalog_path: Path to the catalog JSON file.

    Returns:
        Dict with 'assets' (list of asset dicts) and 'total' (int).

    Raises:
        CatalogError: If the catalog file is malformed.
    """
    catalog = load_catalog(catalog_path)
    assets = catalog["assets"]
    return {
        "assets": assets,
        "total": len(assets),
    }


def main() -> None:
    """Entry point for standalone invocation."""
    import argparse

    parser = argparse.ArgumentParser(description="List all registered catalog assets.")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help="Path to catalog JSON")
    args = parser.parse_args()

    try:
        result = list_assets(args.catalog)
        print(format_query_output("catalog", result["assets"]))
    except CatalogError as exc:
        print(json.dumps(exc.to_dict()), file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as exc:
        print(json.dumps({"error": "file_not_found", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
