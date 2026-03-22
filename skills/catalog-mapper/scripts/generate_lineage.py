"""Generate transitive lineage for a given asset URI.

Traverses relationships in both directions (upstream and downstream)
using BFS to collect all transitively connected assets.

Requirements: 6.4
"""

import importlib.util as _ilu
import json
import os
import os as _os
import sys
from collections import deque

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


def generate_lineage(
    asset_uri: str,
    catalog_path: str = DEFAULT_CATALOG,
) -> dict:
    """Traverse relationships transitively to find upstream and downstream assets.

    Upstream: assets that feed into the given asset (following target → source).
    Downstream: assets this asset feeds into (following source → target).

    Args:
        asset_uri: The asset URI to generate lineage for.
        catalog_path: Path to the catalog JSON file.

    Returns:
        Dict with 'asset_uri', 'upstream' (list of URIs), 'downstream'
        (list of URIs), and 'all_relationships' (list of relationship dicts
        traversed).

    Raises:
        CatalogError: If the catalog file is malformed.
    """
    catalog = load_catalog(catalog_path)
    relationships = catalog["relationships"]

    # BFS upstream: find all assets that (transitively) feed into asset_uri
    upstream: list[str] = []
    upstream_rels: list[dict] = []
    visited_up: set[str] = {asset_uri}
    queue: deque[str] = deque([asset_uri])
    while queue:
        current = queue.popleft()
        for rel in relationships:
            if rel.get("target") == current and rel.get("source") not in visited_up:
                source = rel["source"]
                visited_up.add(source)
                upstream.append(source)
                upstream_rels.append(rel)
                queue.append(source)

    # BFS downstream: find all assets that asset_uri (transitively) feeds into
    downstream: list[str] = []
    downstream_rels: list[dict] = []
    visited_down: set[str] = {asset_uri}
    queue = deque([asset_uri])
    while queue:
        current = queue.popleft()
        for rel in relationships:
            if rel.get("source") == current and rel.get("target") not in visited_down:
                target = rel["target"]
                visited_down.add(target)
                downstream.append(target)
                downstream_rels.append(rel)
                queue.append(target)

    # Deduplicate all_relationships by id while preserving order
    seen_ids: set[str] = set()
    all_relationships: list[dict] = []
    for rel in upstream_rels + downstream_rels:
        rel_id = rel.get("id")
        if rel_id not in seen_ids:
            seen_ids.add(rel_id)
            all_relationships.append(rel)

    return {
        "asset_uri": asset_uri,
        "upstream": upstream,
        "downstream": downstream,
        "all_relationships": all_relationships,
    }


def main() -> None:
    """Entry point for standalone invocation."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate transitive lineage for a data asset."
    )
    parser.add_argument("asset_uri", help="Asset URI, e.g. vector://event-embeddings")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help="Path to catalog JSON")
    args = parser.parse_args()

    try:
        result = generate_lineage(args.asset_uri, args.catalog)
        if result["all_relationships"]:
            result_set = result["all_relationships"]
        else:
            result_set = [{"asset_uri": result["asset_uri"], "upstream": str(result["upstream"]), "downstream": str(result["downstream"])}]
        print(format_query_output("catalog", result_set))
    except CatalogError as exc:
        print(json.dumps(exc.to_dict()), file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as exc:
        print(json.dumps({"error": "file_not_found", "message": str(exc)}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
