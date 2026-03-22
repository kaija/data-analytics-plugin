"""Property tests for the Catalog Mapper skill.

Property 23: Catalog register/query round trip
**Validates: Requirements 6.2**

Property 24: Catalog list assets returns all registered assets with paradigm
**Validates: Requirements 6.3**

Property 25: Catalog persistence round trip
**Validates: Requirements 6.5**

Property 26: Unresolved assets are flagged
**Validates: Requirements 6.7**

Property 27: Lineage report includes all transitive relationships
**Validates: Requirements 6.4**
"""

from __future__ import annotations

import os
import sys
import tempfile

from hypothesis import given, settings, strategies as st

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "..", "skills", "catalog-mapper", "scripts"
    ),
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

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

PARADIGMS = ["athena", "vector", "graph", "s3", "kafka"]

paradigm_st = st.sampled_from(PARADIGMS)

identifier_st = st.from_regex(r"[a-z][a-z0-9_]{1,15}\.[a-z][a-z0-9_]{1,15}", fullmatch=True)

uri_st = st.builds(lambda p, i: f"{p}://{i}", paradigm_st, identifier_st)

rel_type_st = st.sampled_from(
    ["derived_from", "feeds_into", "copies_from", "aggregates", "joins"]
)

# A set of distinct URIs (2–6 items) for multi-asset tests
uri_set_st = st.lists(uri_st, min_size=2, max_size=6, unique=True)


# ---------------------------------------------------------------------------
# Property 23: Catalog register/query round trip
# ---------------------------------------------------------------------------


class TestCatalogRegisterQueryRoundTrip:
    """Property 23: For any source_uri, target_uri, and rel_type, after
    registering a relationship, querying either asset should return that
    relationship.

    **Validates: Requirements 6.2**
    """

    @given(source_uri=uri_st, target_uri=uri_st, rel_type=rel_type_st)
    @settings(max_examples=100)
    def test_query_source_finds_relationship(
        self, source_uri: str, target_uri: str, rel_type: str
    ):
        with tempfile.TemporaryDirectory() as tmp:
            catalog_path = os.path.join(tmp, "catalog.json")
            register_relationship_cmd(
                source_uri, target_uri, rel_type, catalog_path=catalog_path
            )
            result = query_relationships(source_uri, catalog_path=catalog_path)

        # The registered relationship must appear in as_source
        sources = [r["source"] for r in result["as_source"]]
        assert source_uri in sources, (
            f"source_uri '{source_uri}' not found in as_source after registration"
        )
        # Verify the specific relationship is present
        matching = [
            r for r in result["as_source"]
            if r["source"] == source_uri and r["target"] == target_uri and r["type"] == rel_type
        ]
        assert len(matching) >= 1, (
            f"Registered relationship ({source_uri} -> {target_uri}, {rel_type}) "
            f"not found when querying source"
        )

    @given(source_uri=uri_st, target_uri=uri_st, rel_type=rel_type_st)
    @settings(max_examples=100)
    def test_query_target_finds_relationship(
        self, source_uri: str, target_uri: str, rel_type: str
    ):
        with tempfile.TemporaryDirectory() as tmp:
            catalog_path = os.path.join(tmp, "catalog.json")
            register_relationship_cmd(
                source_uri, target_uri, rel_type, catalog_path=catalog_path
            )
            result = query_relationships(target_uri, catalog_path=catalog_path)

        # The registered relationship must appear in as_target
        targets = [r["target"] for r in result["as_target"]]
        assert target_uri in targets, (
            f"target_uri '{target_uri}' not found in as_target after registration"
        )
        matching = [
            r for r in result["as_target"]
            if r["source"] == source_uri and r["target"] == target_uri and r["type"] == rel_type
        ]
        assert len(matching) >= 1, (
            f"Registered relationship ({source_uri} -> {target_uri}, {rel_type}) "
            f"not found when querying target"
        )


# ---------------------------------------------------------------------------
# Property 24: Catalog list assets returns all registered assets with paradigm
# ---------------------------------------------------------------------------


class TestCatalogListAssets:
    """Property 24: For any set of registered assets, list_assets() returns
    all of them with the correct paradigm extracted from the URI.

    **Validates: Requirements 6.3**
    """

    @given(uris=uri_set_st)
    @settings(max_examples=100)
    def test_all_registered_assets_appear_in_list(self, uris: list[str]):
        with tempfile.TemporaryDirectory() as tmp:
            catalog_path = os.path.join(tmp, "catalog.json")
            catalog = load_catalog(catalog_path)
            for uri in uris:
                paradigm = get_paradigm(uri)
                register_asset(catalog, uri, paradigm)
            save_catalog(catalog, catalog_path)

            result = list_assets(catalog_path=catalog_path)

        listed_uris = {a["uri"] for a in result["assets"]}
        for uri in uris:
            assert uri in listed_uris, (
                f"Registered URI '{uri}' not found in list_assets() output"
            )
        assert result["total"] == len(result["assets"])

    @given(uris=uri_set_st)
    @settings(max_examples=100)
    def test_listed_assets_have_correct_paradigm(self, uris: list[str]):
        with tempfile.TemporaryDirectory() as tmp:
            catalog_path = os.path.join(tmp, "catalog.json")
            catalog = load_catalog(catalog_path)
            for uri in uris:
                paradigm = get_paradigm(uri)
                register_asset(catalog, uri, paradigm)
            save_catalog(catalog, catalog_path)

            result = list_assets(catalog_path=catalog_path)

        asset_map = {a["uri"]: a for a in result["assets"]}
        for uri in uris:
            assert uri in asset_map, f"URI '{uri}' missing from listed assets"
            expected_paradigm = get_paradigm(uri)
            actual_paradigm = asset_map[uri]["paradigm"]
            assert actual_paradigm == expected_paradigm, (
                f"Paradigm mismatch for '{uri}': expected '{expected_paradigm}', "
                f"got '{actual_paradigm}'"
            )


# ---------------------------------------------------------------------------
# Property 25: Catalog persistence round trip
# ---------------------------------------------------------------------------


class TestCatalogPersistenceRoundTrip:
    """Property 25: For any catalog state, save_catalog followed by
    load_catalog returns an identical catalog.

    **Validates: Requirements 6.5**
    """

    @given(uris=uri_set_st, rel_type=rel_type_st)
    @settings(max_examples=100)
    def test_save_load_round_trip(self, uris: list[str], rel_type: str):
        with tempfile.TemporaryDirectory() as tmp:
            catalog_path = os.path.join(tmp, "catalog.json")
            catalog = load_catalog(catalog_path)

            # Register all URIs as assets
            for uri in uris:
                register_asset(catalog, uri, get_paradigm(uri))

            # Register a relationship between first two URIs
            if len(uris) >= 2:
                register_relationship(catalog, uris[0], uris[1], rel_type)

            save_catalog(catalog, catalog_path)
            reloaded = load_catalog(catalog_path)

        # Assets must be identical
        assert reloaded["assets"] == catalog["assets"], (
            "Assets differ after save/load round trip"
        )
        # Relationships must be identical
        assert reloaded["relationships"] == catalog["relationships"], (
            "Relationships differ after save/load round trip"
        )

    @given(uris=uri_set_st)
    @settings(max_examples=100)
    def test_multiple_save_load_cycles_are_stable(self, uris: list[str]):
        """Repeated save/load cycles should not alter the catalog."""
        with tempfile.TemporaryDirectory() as tmp:
            catalog_path = os.path.join(tmp, "catalog.json")
            catalog = load_catalog(catalog_path)
            for uri in uris:
                register_asset(catalog, uri, get_paradigm(uri))
            save_catalog(catalog, catalog_path)

            # Three round trips
            for _ in range(3):
                loaded = load_catalog(catalog_path)
                save_catalog(loaded, catalog_path)

            final = load_catalog(catalog_path)

        assert final["assets"] == catalog["assets"], (
            "Assets changed across multiple save/load cycles"
        )


# ---------------------------------------------------------------------------
# Property 26: Unresolved assets are flagged
# ---------------------------------------------------------------------------


class TestUnresolvedAssetsFlagged:
    """Property 26: When registering a relationship where source or target
    has not been pre-registered as an asset, the relationship should have
    unresolved=True. When both assets ARE pre-registered, unresolved=False.

    **Validates: Requirements 6.7**
    """

    @given(source_uri=uri_st, target_uri=uri_st, rel_type=rel_type_st)
    @settings(max_examples=100)
    def test_unregistered_assets_yield_unresolved_true(
        self, source_uri: str, target_uri: str, rel_type: str
    ):
        """Neither asset pre-registered → unresolved=True."""
        catalog: dict = {"assets": [], "relationships": []}
        rel = register_relationship(catalog, source_uri, target_uri, rel_type)
        assert rel["unresolved"] is True, (
            f"Expected unresolved=True when neither asset is pre-registered, "
            f"got unresolved={rel['unresolved']}"
        )

    @given(source_uri=uri_st, target_uri=uri_st, rel_type=rel_type_st)
    @settings(max_examples=100)
    def test_only_source_registered_yields_unresolved_true(
        self, source_uri: str, target_uri: str, rel_type: str
    ):
        """Only source pre-registered → unresolved=True (source != target)."""
        # When source == target, registering source also covers target → skip
        if source_uri == target_uri:
            return
        catalog: dict = {"assets": [], "relationships": []}
        register_asset(catalog, source_uri, get_paradigm(source_uri))
        rel = register_relationship(catalog, source_uri, target_uri, rel_type)
        assert rel["unresolved"] is True, (
            f"Expected unresolved=True when only source is registered"
        )

    @given(
        source_uri=uri_st,
        target_uri=uri_st,
        rel_type=rel_type_st,
    )
    @settings(max_examples=100)
    def test_only_target_registered_yields_unresolved_true(
        self, source_uri: str, target_uri: str, rel_type: str
    ):
        """Only target pre-registered → unresolved=True (source != target)."""
        # When source == target, registering target also covers source → skip
        if source_uri == target_uri:
            return
        catalog: dict = {"assets": [], "relationships": []}
        register_asset(catalog, target_uri, get_paradigm(target_uri))
        rel = register_relationship(catalog, source_uri, target_uri, rel_type)
        assert rel["unresolved"] is True, (
            f"Expected unresolved=True when only target is registered"
        )

    @given(source_uri=uri_st, target_uri=uri_st, rel_type=rel_type_st)
    @settings(max_examples=100)
    def test_both_registered_yields_unresolved_false(
        self, source_uri: str, target_uri: str, rel_type: str
    ):
        """Both assets pre-registered → unresolved=False."""
        catalog: dict = {"assets": [], "relationships": []}
        register_asset(catalog, source_uri, get_paradigm(source_uri))
        register_asset(catalog, target_uri, get_paradigm(target_uri))
        rel = register_relationship(catalog, source_uri, target_uri, rel_type)
        assert rel["unresolved"] is False, (
            f"Expected unresolved=False when both assets are pre-registered, "
            f"got unresolved={rel['unresolved']}"
        )


# ---------------------------------------------------------------------------
# Property 27: Lineage report includes all transitive relationships
# ---------------------------------------------------------------------------


class TestLineageTransitivity:
    """Property 27: For a chain A→B→C, generate_lineage(B) should include A
    in upstream and C in downstream. For arbitrary chains, all transitively
    connected assets appear in the lineage.

    **Validates: Requirements 6.4**
    """

    @given(
        a_uri=uri_st,
        b_uri=uri_st,
        c_uri=uri_st,
        rel_type=rel_type_st,
    )
    @settings(max_examples=100)
    def test_chain_abc_lineage_of_b(
        self, a_uri: str, b_uri: str, c_uri: str, rel_type: str
    ):
        """A→B→C: lineage of B must have A upstream and C downstream."""
        # Ensure all three URIs are distinct
        if len({a_uri, b_uri, c_uri}) < 3:
            return  # skip degenerate case

        with tempfile.TemporaryDirectory() as tmp:
            catalog_path = os.path.join(tmp, "catalog.json")
            # Register A→B and B→C
            register_relationship_cmd(a_uri, b_uri, rel_type, catalog_path=catalog_path)
            register_relationship_cmd(b_uri, c_uri, rel_type, catalog_path=catalog_path)

            lineage = generate_lineage(b_uri, catalog_path=catalog_path)

        assert a_uri in lineage["upstream"], (
            f"Expected '{a_uri}' in upstream of '{b_uri}', got {lineage['upstream']}"
        )
        assert c_uri in lineage["downstream"], (
            f"Expected '{c_uri}' in downstream of '{b_uri}', got {lineage['downstream']}"
        )

    @given(
        uris=st.lists(uri_st, min_size=3, max_size=6, unique=True),
        rel_type=rel_type_st,
    )
    @settings(max_examples=80)
    def test_chain_all_transitive_assets_in_lineage(
        self, uris: list[str], rel_type: str
    ):
        """For a linear chain u0→u1→…→uN, lineage of the middle node must
        include all upstream and downstream nodes transitively."""
        if len(uris) < 3:
            return

        with tempfile.TemporaryDirectory() as tmp:
            catalog_path = os.path.join(tmp, "catalog.json")
            # Build a linear chain
            for i in range(len(uris) - 1):
                register_relationship_cmd(
                    uris[i], uris[i + 1], rel_type, catalog_path=catalog_path
                )

            # Query lineage of the middle node
            mid = len(uris) // 2
            lineage = generate_lineage(uris[mid], catalog_path=catalog_path)

        # All nodes before mid should be upstream
        for i in range(mid):
            assert uris[i] in lineage["upstream"], (
                f"Expected '{uris[i]}' in upstream of '{uris[mid]}', "
                f"got {lineage['upstream']}"
            )
        # All nodes after mid should be downstream
        for i in range(mid + 1, len(uris)):
            assert uris[i] in lineage["downstream"], (
                f"Expected '{uris[i]}' in downstream of '{uris[mid]}', "
                f"got {lineage['downstream']}"
            )
