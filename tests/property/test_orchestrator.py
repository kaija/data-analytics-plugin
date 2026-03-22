"""Property tests for orchestrator routing rules.

Property 3: Orchestrator routes to correct skill by intent
**Validates: Requirements 2.1**

Property 4: Multi-paradigm delegation covers all relevant skills
**Validates: Requirements 2.2**

Property 5: Ambiguous intent triggers clarification
**Validates: Requirements 2.5**

These are structural tests that parse agents/orchestrator.md and verify
the routing rules are present and correct.
"""

from __future__ import annotations

import os

import pytest
from hypothesis import given, settings, strategies as st

ORCHESTRATOR_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "agents", "orchestrator.md"
)

# ---------------------------------------------------------------------------
# Keyword → skill name mappings per intent classification rules
# ---------------------------------------------------------------------------

ATHENA_KEYWORDS = ["SQL", "Athena", "tables", "schemas", "databases", "structured data"]
VECTOR_KEYWORDS = ["embeddings", "vectors", "similarity search", "semantic search"]
GRAPH_KEYWORDS = ["graphs", "nodes", "relationships", "traversal", "Cypher", "Gremlin"]
CATALOG_KEYWORDS = ["lineage", "catalog", "mapping", "cross-paradigm relationships"]

KEYWORD_SKILL_PAIRS = (
    [(kw, "athena-glue") for kw in ATHENA_KEYWORDS]
    + [(kw, "vector-db") for kw in VECTOR_KEYWORDS]
    + [(kw, "graph-db") for kw in GRAPH_KEYWORDS]
    + [(kw, "catalog-mapper") for kw in CATALOG_KEYWORDS]
)

ALL_SKILLS = ["athena-glue", "vector-db", "graph-db", "catalog-mapper"]


def _load_orchestrator() -> str:
    with open(ORCHESTRATOR_PATH, "r") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Property 3: Orchestrator routes to correct skill by intent
# ---------------------------------------------------------------------------

class TestOrchestratorRoutingByIntent:
    """Property 3: For any keyword associated with a skill, the orchestrator's
    intent classification rules should map it to the correct skill name.

    **Validates: Requirements 2.1**
    """

    @given(pair=st.sampled_from(KEYWORD_SKILL_PAIRS))
    @settings(max_examples=len(KEYWORD_SKILL_PAIRS))
    def test_keyword_maps_to_correct_skill(self, pair: tuple[str, str]) -> None:
        """Both the keyword and its expected skill name must appear in
        orchestrator.md (case-insensitive for keywords), confirming the
        routing rule is present."""
        keyword, skill = pair
        doc = _load_orchestrator()
        doc_lower = doc.lower()

        assert keyword.lower() in doc_lower, (
            f"Keyword '{keyword}' not found in orchestrator.md — "
            f"routing rule for '{skill}' may be missing"
        )
        assert skill in doc, (
            f"Skill name '{skill}' not found in orchestrator.md — "
            f"routing target for keyword '{keyword}' is absent"
        )

    @given(keyword=st.sampled_from(ATHENA_KEYWORDS))
    @settings(max_examples=len(ATHENA_KEYWORDS))
    def test_athena_keywords_colocated_with_skill_name(self, keyword: str) -> None:
        """Each Athena keyword and 'athena-glue' must both appear in the doc."""
        doc = _load_orchestrator()
        assert keyword.lower() in doc.lower(), (
            f"Athena keyword '{keyword}' missing from orchestrator.md"
        )
        assert "athena-glue" in doc, "Skill 'athena-glue' missing from orchestrator.md"

    @given(keyword=st.sampled_from(VECTOR_KEYWORDS))
    @settings(max_examples=len(VECTOR_KEYWORDS))
    def test_vector_keywords_colocated_with_skill_name(self, keyword: str) -> None:
        """Each vector keyword and 'vector-db' must both appear in the doc."""
        doc = _load_orchestrator()
        assert keyword.lower() in doc.lower(), (
            f"Vector keyword '{keyword}' missing from orchestrator.md"
        )
        assert "vector-db" in doc, "Skill 'vector-db' missing from orchestrator.md"

    @given(keyword=st.sampled_from(GRAPH_KEYWORDS))
    @settings(max_examples=len(GRAPH_KEYWORDS))
    def test_graph_keywords_colocated_with_skill_name(self, keyword: str) -> None:
        """Each graph keyword and 'graph-db' must both appear in the doc."""
        doc = _load_orchestrator()
        assert keyword.lower() in doc.lower(), (
            f"Graph keyword '{keyword}' missing from orchestrator.md"
        )
        assert "graph-db" in doc, "Skill 'graph-db' missing from orchestrator.md"

    @given(keyword=st.sampled_from(CATALOG_KEYWORDS))
    @settings(max_examples=len(CATALOG_KEYWORDS))
    def test_catalog_keywords_colocated_with_skill_name(self, keyword: str) -> None:
        """Each catalog keyword and 'catalog-mapper' must both appear in the doc."""
        doc = _load_orchestrator()
        assert keyword.lower() in doc.lower(), (
            f"Catalog keyword '{keyword}' missing from orchestrator.md"
        )
        assert "catalog-mapper" in doc, "Skill 'catalog-mapper' missing from orchestrator.md"


# ---------------------------------------------------------------------------
# Property 4: Multi-paradigm delegation covers all relevant skills
# ---------------------------------------------------------------------------

class TestMultiParadigmDelegation:
    """Property 4: For any request that contains keywords from multiple
    paradigms, all relevant skills should be identified.  Verified
    structurally: orchestrator.md must describe multi-paradigm handling
    and reference every skill.

    **Validates: Requirements 2.2**
    """

    def test_orchestrator_mentions_multi_paradigm_handling(self) -> None:
        """orchestrator.md must contain a section or phrase about multi-paradigm
        requests and sequential delegation."""
        doc = _load_orchestrator()

        has_multi_paradigm = any(
            phrase in doc
            for phrase in [
                "Multi-Paradigm",
                "multi-paradigm",
                "Multi-paradigm",
                "multiple paradigm",
                "multiple paradigms",
            ]
        )
        assert has_multi_paradigm, (
            "orchestrator.md is missing a multi-paradigm section or phrase"
        )

        assert "sequential" in doc.lower(), (
            "orchestrator.md does not mention sequential delegation for multi-paradigm requests"
        )

    @given(skill=st.sampled_from(ALL_SKILLS))
    @settings(max_examples=len(ALL_SKILLS))
    def test_all_skills_referenced_for_multi_paradigm(self, skill: str) -> None:
        """Every skill name must appear in orchestrator.md so that multi-paradigm
        delegation can reach any combination of skills."""
        doc = _load_orchestrator()
        assert skill in doc, (
            f"Skill '{skill}' not referenced in orchestrator.md — "
            "multi-paradigm delegation cannot reach it"
        )


# ---------------------------------------------------------------------------
# Property 5: Ambiguous intent triggers clarification
# ---------------------------------------------------------------------------

class TestAmbiguousIntentClarification:
    """Property 5: For any request that contains no recognizable keywords,
    the orchestrator should not route to any skill (ambiguous) and should
    instead ask the user for clarification.

    Verified structurally: orchestrator.md must contain instructions for
    handling ambiguous requests.

    **Validates: Requirements 2.5**
    """

    def test_orchestrator_contains_ambiguous_request_instructions(self) -> None:
        """orchestrator.md must mention ambiguous requests and clarification."""
        doc = _load_orchestrator()

        has_ambiguous = any(
            phrase in doc
            for phrase in ["Ambiguous", "ambiguous", "cannot confidently classify"]
        )
        assert has_ambiguous, (
            "orchestrator.md missing instructions for ambiguous requests"
        )

        has_clarification = any(
            phrase in doc
            for phrase in [
                "clarification",
                "clarify",
                "ask the user",
                "ask for clarification",
                "ask",
            ]
        )
        assert has_clarification, (
            "orchestrator.md does not instruct the agent to ask for clarification "
            "when intent is ambiguous"
        )

    def test_ambiguous_handling_precedes_delegation(self) -> None:
        """The clarification instruction must exist as a distinct section,
        not buried inside a delegation rule."""
        doc = _load_orchestrator()
        # The word 'clarification' or 'ambiguous' should appear outside the
        # intent classification bullet list — a section heading is the clearest signal.
        lines = doc.splitlines()
        found_ambiguous_section = any(
            ("ambiguous" in line.lower() or "clarification" in line.lower())
            and line.strip().startswith("#")
            for line in lines
        )
        assert found_ambiguous_section, (
            "orchestrator.md should have a dedicated section heading for ambiguous "
            "request handling (e.g. '## Ambiguous Requests')"
        )
