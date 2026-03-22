"""Unit tests for catalog mapper scripts.

Tests catalog_persistence, register_relationship_cmd, query_relationships,
list_assets, and generate_lineage using tmp_path fixtures.

Requirements: 6.1–6.9
"""

from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "skills", "catalog-mapper", "scripts"),
)

from catalog_persistence import (
    CatalogError,
    get_paradigm,
    load_catalog,
    register_asset,
    register_relationship,
    save_catalog,
)
from generate_lineage import generate_lineage
from list_assets import list_assets
from query_relationships import query_relationships
from register_relationship import register_relationship_cmd


# ===========================================================================
# Helpers
# ===========================================================================

def _catalog_path(tmp_path, name: str = "catalog.json") -> str:
    return str(tmp_path / name)


def _write_catalog(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


# ===========================================================================
# catalog_persistence — load_catalog
# ===========================================================================

class TestLoadCatalog:
    def test_missing_file_auto_creates_empty_catalog(self, tmp_path):
        """Req 6.8: missing file → auto-create empty catalog."""
        path = _catalog_path(tmp_path, "nonexistent.json")
        catalog = load_catalog(path)
        assert catalog == {"assets": [], "relationships": []}
        assert os.path.isfile(path)

    def test_empty_file_auto_creates_empty_catalog(self, tmp_path):
        """Req 6.8: 0-byte file → auto-create empty catalog (bug fix)."""
        path = _catalog_path(tmp_path)
        open(path, "w").close()  # create 0-byte file
        catalog = load_catalog(path)
        assert catalog == {"assets": [], "relationships": []}

    def test_malformed_json_raises_catalog_error(self, tmp_path):
        """Req 6.9: malformed JSON → CatalogError."""
        path = _catalog_path(tmp_path)
        with open(path, "w") as f:
            f.write("{not valid json")
        with pytest.raises(CatalogError, match="invalid JSON"):
            load_catalog(path)

    def test_valid_file_returns_correct_data(self, tmp_path):
        """Req 6.8: valid file → returns its data."""
        path = _catalog_path(tmp_path)
        data = {
            "assets": [{"uri": "athena://db.t", "paradigm": "athena"}],
            "relationships": [],
        }
        _write_catalog(path, data)
        catalog = load_catalog(path)
        assert len(catalog["assets"]) == 1
        assert catalog["assets"][0]["uri"] == "athena://db.t"

    def test_missing_assets_key_raises_catalog_error(self, tmp_path):
        """Req 6.9: missing 'assets' key → CatalogError."""
        path = _catalog_path(tmp_path)
        _write_catalog(path, {"relationships": []})
        with pytest.raises(CatalogError, match="assets"):
            load_catalog(path)

    def test_missing_relationships_key_raises_catalog_error(self, tmp_path):
        """Req 6.9: missing 'relationships' key → CatalogError."""
        path = _catalog_path(tmp_path)
        _write_catalog(path, {"assets": []})
        with pytest.raises(CatalogError, match="relationships"):
            load_catalog(path)


# ===========================================================================
# catalog_persistence — save_catalog
# ===========================================================================

class TestSaveCatalog:
    def test_writes_valid_json_that_can_be_reloaded(self, tmp_path):
        """Req 6.8: save then load round-trips correctly."""
        path = _catalog_path(tmp_path)
        catalog = {
            "assets": [{"uri": "vector://col", "paradigm": "vector"}],
            "relationships": [],
        }
        save_catalog(catalog, path)
        reloaded = load_catalog(path)
        assert reloaded["assets"][0]["uri"] == "vector://col"
        assert reloaded["relationships"] == []


# ===========================================================================
# catalog_persistence — register_asset
# ===========================================================================

class TestRegisterAsset:
    def test_adds_asset_with_uri_and_paradigm(self, tmp_path):
        """Req 6.1: asset registered with uri and paradigm."""
        catalog = {"assets": [], "relationships": []}
        asset = register_asset(catalog, "athena://db.table", "athena")
        assert asset["uri"] == "athena://db.table"
        assert asset["paradigm"] == "athena"
        assert len(catalog["assets"]) == 1

    def test_register_asset_is_idempotent(self, tmp_path):
        """Req 6.7: calling twice with same URI returns same asset, no duplicate."""
        catalog = {"assets": [], "relationships": []}
        a1 = register_asset(catalog, "athena://db.table", "athena")
        a2 = register_asset(catalog, "athena://db.table", "athena")
        assert a1 is a2
        assert len(catalog["assets"]) == 1


# ===========================================================================
# catalog_persistence — register_relationship
# ===========================================================================

class TestRegisterRelationship:
    def test_adds_relationship_with_expected_fields(self):
        """Req 6.1: relationship has id, source, target, type, created_at, unresolved."""
        catalog = {"assets": [], "relationships": []}
        rel = register_relationship(catalog, "athena://a", "vector://b", "derived_from")
        assert rel["source"] == "athena://a"
        assert rel["target"] == "vector://b"
        assert rel["type"] == "derived_from"
        assert "id" in rel
        assert "created_at" in rel
        assert "unresolved" in rel

    def test_register_relationship_is_idempotent(self):
        """Req 6.7: same source+target+type returns existing, no duplicate."""
        catalog = {"assets": [], "relationships": []}
        r1 = register_relationship(catalog, "athena://a", "vector://b", "derived_from")
        r2 = register_relationship(catalog, "athena://a", "vector://b", "derived_from")
        assert r1["id"] == r2["id"]
        assert len(catalog["relationships"]) == 1

    def test_unresolved_true_when_assets_not_registered(self):
        """Req 6.6: unresolved=True when assets not pre-registered."""
        catalog = {"assets": [], "relationships": []}
        rel = register_relationship(catalog, "athena://a", "vector://b", "derived_from")
        assert rel["unresolved"] is True

    def test_unresolved_false_when_both_assets_registered(self):
        """Req 6.6: unresolved=False when both assets pre-registered."""
        catalog = {"assets": [], "relationships": []}
        register_asset(catalog, "athena://a", "athena")
        register_asset(catalog, "vector://b", "vector")
        rel = register_relationship(catalog, "athena://a", "vector://b", "derived_from")
        assert rel["unresolved"] is False


# ===========================================================================
# catalog_persistence — get_paradigm
# ===========================================================================

class TestGetParadigm:
    def test_extracts_paradigm_from_athena_uri(self):
        """Req 6.5: athena://db.table → 'athena'."""
        assert get_paradigm("athena://db.table") == "athena"

    def test_extracts_paradigm_from_vector_uri(self):
        assert get_paradigm("vector://my-collection") == "vector"

    def test_extracts_paradigm_from_graph_uri(self):
        assert get_paradigm("graph://my-graph") == "graph"

    def test_raises_value_error_for_invalid_uri(self):
        """Req 6.5: URI without '://' → ValueError."""
        with pytest.raises(ValueError):
            get_paradigm("no-scheme-here")

    def test_raises_value_error_for_empty_string(self):
        with pytest.raises(ValueError):
            get_paradigm("")


# ===========================================================================
# register_relationship_cmd
# ===========================================================================

class TestRegisterRelationshipCmd:
    def test_registers_source_and_target_assets_automatically(self, tmp_path):
        """Req 6.1, 6.6: auto-registers both assets."""
        path = _catalog_path(tmp_path)
        result = register_relationship_cmd(
            "athena://db.t", "vector://col", "derived_from", catalog_path=path
        )
        assert result["source_asset"]["uri"] == "athena://db.t"
        assert result["target_asset"]["uri"] == "vector://col"

    def test_returns_expected_keys(self, tmp_path):
        """Req 6.1: result has 'registered', 'relationship', 'source_asset', 'target_asset'."""
        path = _catalog_path(tmp_path)
        result = register_relationship_cmd(
            "athena://db.t", "vector://col", "derived_from", catalog_path=path
        )
        assert result["registered"] is True
        assert "relationship" in result
        assert "source_asset" in result
        assert "target_asset" in result

    def test_persists_to_catalog_file(self, tmp_path):
        """Req 6.8: relationship is persisted to disk."""
        path = _catalog_path(tmp_path)
        register_relationship_cmd(
            "athena://db.t", "vector://col", "derived_from", catalog_path=path
        )
        catalog = load_catalog(path)
        assert len(catalog["relationships"]) == 1
        assert catalog["relationships"][0]["source"] == "athena://db.t"

    def test_duplicate_call_returns_existing_relationship(self, tmp_path):
        """Req 6.7: idempotent — second call returns same relationship."""
        path = _catalog_path(tmp_path)
        r1 = register_relationship_cmd(
            "athena://db.t", "vector://col", "derived_from", catalog_path=path
        )
        r2 = register_relationship_cmd(
            "athena://db.t", "vector://col", "derived_from", catalog_path=path
        )
        assert r1["relationship"]["id"] == r2["relationship"]["id"]
        catalog = load_catalog(path)
        assert len(catalog["relationships"]) == 1

    def test_invalid_uri_raises_value_error(self, tmp_path):
        """Req 6.5: malformed URI → ValueError."""
        path = _catalog_path(tmp_path)
        with pytest.raises(ValueError):
            register_relationship_cmd("bad-uri", "vector://col", "derived_from", catalog_path=path)


# ===========================================================================
# query_relationships
# ===========================================================================

class TestQueryRelationships:
    def _setup_catalog(self, tmp_path) -> str:
        path = _catalog_path(tmp_path)
        register_relationship_cmd("athena://a", "vector://b", "derived_from", catalog_path=path)
        register_relationship_cmd("vector://b", "graph://c", "feeds_into", catalog_path=path)
        return path

    def test_returns_relationships_where_asset_is_source(self, tmp_path):
        """Req 6.2: as_source contains relationships where asset is source."""
        path = self._setup_catalog(tmp_path)
        result = query_relationships("athena://a", catalog_path=path)
        assert len(result["as_source"]) == 1
        assert result["as_source"][0]["source"] == "athena://a"

    def test_returns_relationships_where_asset_is_target(self, tmp_path):
        """Req 6.2: as_target contains relationships where asset is target."""
        path = self._setup_catalog(tmp_path)
        result = query_relationships("vector://b", catalog_path=path)
        assert len(result["as_target"]) == 1
        assert result["as_target"][0]["target"] == "vector://b"

    def test_returns_total_count(self, tmp_path):
        """Req 6.2: total = len(as_source) + len(as_target)."""
        path = self._setup_catalog(tmp_path)
        result = query_relationships("vector://b", catalog_path=path)
        assert result["total"] == len(result["as_source"]) + len(result["as_target"])

    def test_asset_with_no_relationships_returns_empty_lists(self, tmp_path):
        """Req 6.2: unknown asset → empty as_source and as_target."""
        path = self._setup_catalog(tmp_path)
        result = query_relationships("athena://unknown", catalog_path=path)
        assert result["as_source"] == []
        assert result["as_target"] == []
        assert result["total"] == 0


# ===========================================================================
# list_assets
# ===========================================================================

class TestListAssets:
    def test_returns_all_registered_assets(self, tmp_path):
        """Req 6.3: all assets returned with uri and paradigm."""
        path = _catalog_path(tmp_path)
        register_relationship_cmd("athena://db.t", "vector://col", "derived_from", catalog_path=path)
        result = list_assets(catalog_path=path)
        uris = [a["uri"] for a in result["assets"]]
        assert "athena://db.t" in uris
        assert "vector://col" in uris
        for asset in result["assets"]:
            assert "uri" in asset
            assert "paradigm" in asset

    def test_returns_total_count(self, tmp_path):
        """Req 6.3: total matches number of assets."""
        path = _catalog_path(tmp_path)
        register_relationship_cmd("athena://db.t", "vector://col", "derived_from", catalog_path=path)
        result = list_assets(catalog_path=path)
        assert result["total"] == len(result["assets"])

    def test_empty_catalog_returns_empty_list(self, tmp_path):
        """Req 6.3: empty catalog → empty assets list."""
        path = _catalog_path(tmp_path)
        result = list_assets(catalog_path=path)
        assert result["assets"] == []
        assert result["total"] == 0


# ===========================================================================
# generate_lineage
# ===========================================================================

class TestGenerateLineage:
    def _setup_chain(self, tmp_path) -> str:
        """Build A→B→C chain and return catalog path."""
        path = _catalog_path(tmp_path)
        register_relationship_cmd("athena://A", "vector://B", "derived_from", catalog_path=path)
        register_relationship_cmd("vector://B", "graph://C", "feeds_into", catalog_path=path)
        return path

    def test_middle_node_has_upstream_and_downstream(self, tmp_path):
        """Req 6.4: B has A upstream and C downstream."""
        path = self._setup_chain(tmp_path)
        result = generate_lineage("vector://B", catalog_path=path)
        assert "athena://A" in result["upstream"]
        assert "graph://C" in result["downstream"]

    def test_source_node_has_empty_upstream(self, tmp_path):
        """Req 6.4: A has no upstream."""
        path = self._setup_chain(tmp_path)
        result = generate_lineage("athena://A", catalog_path=path)
        assert result["upstream"] == []
        assert "vector://B" in result["downstream"]
        assert "graph://C" in result["downstream"]

    def test_sink_node_has_empty_downstream(self, tmp_path):
        """Req 6.4: C has no downstream."""
        path = self._setup_chain(tmp_path)
        result = generate_lineage("graph://C", catalog_path=path)
        assert result["downstream"] == []
        assert "vector://B" in result["upstream"]
        assert "athena://A" in result["upstream"]

    def test_isolated_asset_has_empty_upstream_and_downstream(self, tmp_path):
        """Req 6.4: isolated asset → empty upstream and downstream."""
        path = self._setup_chain(tmp_path)
        # Register an isolated asset with no relationships
        catalog = load_catalog(path)
        register_asset(catalog, "graph://isolated", "graph")
        save_catalog(catalog, path)

        result = generate_lineage("graph://isolated", catalog_path=path)
        assert result["upstream"] == []
        assert result["downstream"] == []

    def test_all_relationships_contains_traversed_relationships(self, tmp_path):
        """Req 6.4: all_relationships contains the traversed relationship dicts."""
        path = self._setup_chain(tmp_path)
        result = generate_lineage("vector://B", catalog_path=path)
        assert len(result["all_relationships"]) >= 1
        for rel in result["all_relationships"]:
            assert "id" in rel
            assert "source" in rel
            assert "target" in rel
