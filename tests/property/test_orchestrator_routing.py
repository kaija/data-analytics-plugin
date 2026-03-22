"""Property tests for orchestrator routing rules.

Feature: data-analytics-skill-suite
**Validates: Requirements 2.1, 2.2, 2.5**
"""

import os

from hypothesis import given, settings, strategies as st

ORCHESTRATOR_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "agents", "orchestrator.md"
)

# (keyword, expected skill name) pairs per intent classification rules
KEYWORD_SKILL_PAIRS = [
    # Athena/Glue (primary analysis)
    ("SQL", "athena-glue"),
    ("Athena", "athena-glue"),
    ("tables", "athena-glue"),
    ("schemas", "athena-glue"),
    ("databases", "athena-glue"),
    ("structured data", "athena-glue"),
    # Vector DB (assistive — semantic clue discovery)
    ("Embeddings", "vector-db"),
    ("vectors", "vector-db"),
    ("similarity search", "vector-db"),
    ("semantic search", "vector-db"),
    # Graph DB (assistive — relationship discovery)
    ("Graphs", "graph-db"),
    ("nodes", "graph-db"),
    ("relationships", "graph-db"),
    ("traversal", "graph-db"),
    ("Cypher", "graph-db"),
    ("Gremlin", "graph-db"),
    # Catalog Mapper (assistive — lineage tracking)
    ("Lineage", "catalog-mapper"),
    ("catalog", "catalog-mapper"),
    ("mapping", "catalog-mapper"),
    ("cross-paradigm", "catalog-mapper"),
]


def _load_orchestrator() -> str:
    with open(ORCHESTRATOR_PATH, "r") as f:
        return f.read()


class TestOrchestratorRouting:
    """Property 3: Orchestrator routes to correct skill by intent.

    **Validates: Requirements 2.1**
    """

    @given(pair=st.sampled_from(KEYWORD_SKILL_PAIRS))
    @settings(max_examples=len(KEYWORD_SKILL_PAIRS))
    def test_keyword_and_skill_both_appear_in_orchestrator(self, pair):
        """For any keyword→skill pair, both the keyword and the skill name must
        appear in orchestrator.md, confirming a routing rule exists."""
        keyword, skill = pair
        doc = _load_orchestrator()
        assert keyword in doc, (
            f"Keyword '{keyword}' not found in orchestrator.md"
        )
        assert skill in doc, (
            f"Skill name '{skill}' not found in orchestrator.md"
        )


class TestMultiParadigmDelegation:
    """Property 4: Multi-paradigm delegation covers all relevant skills.

    **Validates: Requirements 2.2**
    """

    def test_orchestrator_contains_multi_paradigm_instructions(self):
        """orchestrator.md must contain instructions for handling multi-paradigm
        requests via sequential delegation."""
        doc = _load_orchestrator()
        # Check for multi-paradigm section or concept
        assert any(
            phrase in doc
            for phrase in [
                "multi-paradigm",
                "Multi-Paradigm",
                "Multi-paradigm",
                "multiple paradigm",
            ]
        ), "orchestrator.md missing multi-paradigm section"

        # Check for sequential delegation instruction
        assert "sequential" in doc.lower(), (
            "orchestrator.md missing sequential delegation instruction"
        )

    def test_orchestrator_covers_all_four_skills(self):
        """orchestrator.md must reference all four skill names."""
        doc = _load_orchestrator()
        for skill in ("athena-glue", "vector-db", "graph-db", "catalog-mapper"):
            assert skill in doc, (
                f"orchestrator.md does not reference skill '{skill}'"
            )


class TestAmbiguousIntentClarification:
    """Property 5: Ambiguous intent triggers clarification.

    **Validates: Requirements 2.5**
    """

    def test_orchestrator_contains_clarification_instructions(self):
        """orchestrator.md must contain instructions to ask for clarification
        when intent is ambiguous."""
        doc = _load_orchestrator()
        assert any(
            phrase in doc
            for phrase in [
                "ambiguous",
                "Ambiguous",
                "clarification",
                "clarify",
            ]
        ), "orchestrator.md missing ambiguous-request / clarification instructions"

        assert any(
            phrase in doc
            for phrase in [
                "ask",
                "Ask",
                "request clarification",
                "ask the user",
                "ask for clarification",
            ]
        ), "orchestrator.md does not instruct agent to ask for clarification"
