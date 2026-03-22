"""Catalog Persistence Layer for Catalog Mapper Skill.

Provides load/save functions for catalog-map.json, asset registration,
relationship registration with duplicate detection, and URI paradigm extraction.

Requirements: 6.5, 6.8, 6.9
"""

import json
import os
import re
import uuid
from datetime import datetime, timezone


class CatalogError(Exception):
    """Raised when the catalog file is malformed or an operation is invalid.

    Attributes:
        message: Human-readable description of the error.
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict:
        return {
            "error": "catalog_error",
            "message": self.message,
        }


_REQUIRED_KEYS = ("assets", "relationships")


def _validate_catalog_structure(data: object, source: str = "catalog") -> None:
    """Raise CatalogError if data is not a valid catalog dict."""
    if not isinstance(data, dict):
        raise CatalogError(f"{source} must be a JSON object, got {type(data).__name__}")
    for key in _REQUIRED_KEYS:
        if key not in data:
            raise CatalogError(f"{source} is missing required key: '{key}'")
    if not isinstance(data["assets"], list):
        raise CatalogError(f"{source} 'assets' must be an array")
    if not isinstance(data["relationships"], list):
        raise CatalogError(f"{source} 'relationships' must be an array")


def load_catalog(catalog_path: str) -> dict:
    """Load catalog from file, auto-creating an empty one if missing.

    Args:
        catalog_path: Path to the catalog-map.json file.

    Returns:
        Catalog dict with 'assets' and 'relationships' lists.

    Raises:
        CatalogError: If the file exists but contains invalid JSON or is
            missing required keys ('assets', 'relationships').
    """
    if not os.path.isfile(catalog_path) or os.path.getsize(catalog_path) == 0:
        empty: dict = {"assets": [], "relationships": []}
        save_catalog(empty, catalog_path)
        return empty

    try:
        with open(catalog_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise CatalogError(
            f"Catalog file at '{catalog_path}' contains invalid JSON: {exc}"
        ) from exc

    _validate_catalog_structure(data, source=f"catalog file at '{catalog_path}'")
    return data


def save_catalog(catalog: dict, catalog_path: str) -> None:
    """Save catalog to file as pretty-printed JSON.

    Args:
        catalog: Catalog dict with 'assets' and 'relationships' lists.
        catalog_path: Destination file path.

    Raises:
        CatalogError: If catalog dict is malformed (missing required keys).
    """
    _validate_catalog_structure(catalog, source="catalog")

    os.makedirs(os.path.dirname(os.path.abspath(catalog_path)), exist_ok=True)
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2)
        f.write("\n")


def register_asset(catalog: dict, uri: str, paradigm: str) -> dict:
    """Add an asset to the catalog if not already present (idempotent by URI).

    Args:
        catalog: Catalog dict (mutated in place).
        uri: Asset URI, e.g. "athena://mydb.mytable".
        paradigm: Paradigm string, e.g. "athena".

    Returns:
        The asset dict (existing or newly created).
    """
    for asset in catalog["assets"]:
        if asset.get("uri") == uri:
            return asset

    asset = {
        "uri": uri,
        "paradigm": paradigm,
        "registered_at": _utc_now(),
    }
    catalog["assets"].append(asset)
    return asset


def register_relationship(
    catalog: dict,
    source: str,
    target: str,
    rel_type: str,
    description: str = "",
) -> dict:
    """Register a relationship between two assets (idempotent).

    If a relationship with the same source, target, and type already exists,
    the existing one is returned without creating a duplicate.

    Args:
        catalog: Catalog dict (mutated in place).
        source: Source asset URI.
        target: Target asset URI.
        rel_type: Relationship type string, e.g. "derived_from".
        description: Optional human-readable description.

    Returns:
        The relationship dict (existing or newly created).
    """
    for rel in catalog["relationships"]:
        if (
            rel.get("source") == source
            and rel.get("target") == target
            and rel.get("type") == rel_type
        ):
            return rel

    registered_uris = {asset.get("uri") for asset in catalog["assets"]}
    unresolved = source not in registered_uris or target not in registered_uris

    relationship: dict = {
        "id": str(uuid.uuid4()),
        "source": source,
        "target": target,
        "type": rel_type,
        "description": description,
        "created_at": _utc_now(),
        "unresolved": unresolved,
    }
    catalog["relationships"].append(relationship)
    return relationship


def get_paradigm(uri: str) -> str:
    """Extract the paradigm from a URI string.

    Args:
        uri: URI in the format ``{paradigm}://{identifier}``.

    Returns:
        The paradigm string (e.g. "athena", "vector", "graph").

    Raises:
        ValueError: If the URI does not match the expected format.
    """
    match = re.match(r"^([A-Za-z][A-Za-z0-9+\-.]*):\/\/.+$", uri)
    if not match:
        raise ValueError(
            f"URI '{uri}' does not match expected format '{{paradigm}}://{{identifier}}'"
        )
    return match.group(1)


def _utc_now() -> str:
    """Return current UTC time as ISO 8601 string with Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
