"""Property tests for plugin manifest required fields.

Feature: data-analytics-skill-suite, Property 1: Plugin manifest contains all required fields
**Validates: Requirements 1.1**
"""

import json
import os

from hypothesis import given, settings, strategies as st

MANIFEST_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", ".claude-plugin", "plugin.json"
)


# --- Strategies for generating manifest-like objects ---

valid_non_empty_string = st.text(min_size=1, max_size=100).filter(lambda s: s.strip())

valid_author = st.fixed_dictionaries({"name": valid_non_empty_string})

valid_manifest = st.fixed_dictionaries(
    {
        "name": valid_non_empty_string,
        "description": valid_non_empty_string,
        "version": valid_non_empty_string,
        "author": valid_author,
    }
)


def _load_manifest() -> dict:
    with open(MANIFEST_PATH, "r") as f:
        return json.load(f)


def _validate_manifest(manifest: dict) -> None:
    """Assert that a manifest dict contains all required fields with correct types."""
    assert "name" in manifest, "Manifest missing 'name' field"
    assert isinstance(manifest["name"], str), "'name' must be a string"
    assert len(manifest["name"].strip()) > 0, "'name' must be non-empty"

    assert "description" in manifest, "Manifest missing 'description' field"
    assert isinstance(manifest["description"], str), "'description' must be a string"
    assert len(manifest["description"].strip()) > 0, "'description' must be non-empty"

    assert "version" in manifest, "Manifest missing 'version' field"
    assert isinstance(manifest["version"], str), "'version' must be a string"
    assert len(manifest["version"].strip()) > 0, "'version' must be non-empty"

    assert "author" in manifest, "Manifest missing 'author' field"
    assert isinstance(manifest["author"], dict), "'author' must be an object"
    assert "name" in manifest["author"], "'author' missing 'name' field"
    assert isinstance(manifest["author"]["name"], str), "'author.name' must be a string"


class TestPluginManifestRequiredFields:
    """Property 1: Plugin manifest contains all required fields."""

    def test_actual_manifest_has_required_fields(self):
        """The actual plugin.json on disk must contain all required fields."""
        manifest = _load_manifest()
        _validate_manifest(manifest)

    @given(manifest=valid_manifest)
    @settings(max_examples=100)
    def test_generated_valid_manifests_pass_validation(self, manifest):
        """Any manifest with non-empty name, description, version, and author.name
        should pass the required-fields validation."""
        _validate_manifest(manifest)

    @given(
        manifest=valid_manifest,
        field=st.sampled_from(["name", "description", "version", "author"]),
    )
    @settings(max_examples=100)
    def test_removing_required_field_fails_validation(self, manifest, field):
        """Removing any single required field from a valid manifest must fail validation."""
        incomplete = {k: v for k, v in manifest.items() if k != field}
        try:
            _validate_manifest(incomplete)
            raise AssertionError(
                f"Validation should have failed for missing '{field}'"
            )
        except AssertionError as e:
            if f"should have failed" in str(e):
                raise
            # Expected: validation raised an AssertionError about the missing field

    @given(
        manifest=valid_manifest,
        field=st.sampled_from(["name", "description", "version"]),
    )
    @settings(max_examples=100)
    def test_empty_string_field_fails_validation(self, manifest, field):
        """Setting a required string field to empty must fail validation."""
        invalid = dict(manifest)
        invalid[field] = "   "
        try:
            _validate_manifest(invalid)
            raise AssertionError(
                f"Validation should have failed for empty '{field}'"
            )
        except AssertionError as e:
            if f"should have failed" in str(e):
                raise

    @given(manifest=valid_manifest)
    @settings(max_examples=100)
    def test_author_without_name_fails_validation(self, manifest):
        """An author object missing the 'name' key must fail validation."""
        invalid = dict(manifest)
        invalid["author"] = {}
        try:
            _validate_manifest(invalid)
            raise AssertionError(
                "Validation should have failed for author missing 'name'"
            )
        except AssertionError as e:
            if "should have failed" in str(e):
                raise


# ---------------------------------------------------------------------------
# Property 2: Skill auto-discovery without manifest changes
# **Validates: Requirements 1.8**
# ---------------------------------------------------------------------------

SKILLS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "skills")


def _find_skill_md_files() -> list[str]:
    """Return a sorted list of absolute paths to all SKILL.md files under skills/."""
    result = []
    for root, _dirs, files in os.walk(SKILLS_DIR):
        if "SKILL.md" in files:
            result.append(os.path.join(root, "SKILL.md"))
    return sorted(result)


def _parse_frontmatter(skill_md_path: str) -> dict:
    """Parse YAML frontmatter from a SKILL.md file.

    Frontmatter is the block between the first and second '---' delimiters.
    Returns a dict of key: value pairs (values are raw strings).
    """
    with open(skill_md_path, "r") as f:
        content = f.read()

    parts = content.split("---")
    # parts[0] is empty (before first ---), parts[1] is frontmatter, parts[2+] is body
    if len(parts) < 3:
        return {}

    frontmatter_block = parts[1]
    result = {}
    for line in frontmatter_block.splitlines():
        line = line.strip()
        if not line:
            continue
        if ": " in line:
            key, _, value = line.partition(": ")
            result[key.strip()] = value.strip()
    return result


def _load_plugin_json() -> dict:
    with open(MANIFEST_PATH, "r") as f:
        return json.load(f)


class TestSkillAutoDiscovery:
    """Property 2: Skill auto-discovery without manifest changes."""

    def test_skills_directory_contains_skill_md_files(self):
        """There must be at least one SKILL.md file in the skills/ directory tree."""
        skill_files = _find_skill_md_files()
        assert len(skill_files) > 0, "No SKILL.md files found under skills/"

    def test_plugin_json_does_not_hardcode_skill_names(self):
        """plugin.json must NOT contain a hardcoded list of skill names.

        The auto-discovery contract means skills are found by filesystem scan,
        not by an explicit list in the manifest.
        """
        plugin = _load_plugin_json()
        skill_files = _find_skill_md_files()
        skill_names = []
        for path in skill_files:
            fm = _parse_frontmatter(path)
            name = fm.get("name", "").strip()
            if name:
                skill_names.append(name)

        plugin_str = json.dumps(plugin)
        # None of the skill names should appear as a dedicated skills list key
        assert "skills" not in plugin, (
            "plugin.json must not have a 'skills' key that hardcodes skill names"
        )
        # Verify skill names are not enumerated as a list value anywhere
        for name in skill_names:
            # It's acceptable for the plugin name or keywords to mention a skill
            # technology (e.g. "athena"), but there must be no dedicated skills array.
            # We check that no array in the manifest contains the exact skill name.
            for key, value in plugin.items():
                if isinstance(value, list):
                    assert name not in value, (
                        f"plugin.json key '{key}' hardcodes skill name '{name}'; "
                        "skills must be discoverable by filesystem scan only"
                    )

    @given(skill_md_path=st.sampled_from(_find_skill_md_files()))
    @settings(max_examples=50)
    def test_each_skill_md_has_valid_name_frontmatter(self, skill_md_path: str):
        """For any SKILL.md file discovered in skills/, its YAML frontmatter must
        contain a non-empty 'name' field — this is the auto-discovery contract.

        **Validates: Requirements 1.8**
        """
        fm = _parse_frontmatter(skill_md_path)
        assert "name" in fm, (
            f"{skill_md_path}: SKILL.md frontmatter is missing the 'name' field"
        )
        assert fm["name"].strip(), (
            f"{skill_md_path}: SKILL.md 'name' frontmatter field must not be empty"
        )

    @given(skill_md_path=st.sampled_from(_find_skill_md_files()))
    @settings(max_examples=50)
    def test_skill_discoverable_by_file_presence_alone(self, skill_md_path: str):
        """A skill is discoverable purely by the presence of a SKILL.md file with a
        valid 'name' frontmatter field — no changes to plugin.json are required.

        Verifies that the skill name from SKILL.md is NOT required to be listed
        in plugin.json for discovery to work.

        **Validates: Requirements 1.8**
        """
        fm = _parse_frontmatter(skill_md_path)
        skill_name = fm.get("name", "").strip()
        assert skill_name, f"{skill_md_path}: SKILL.md must have a non-empty 'name'"

        plugin = _load_plugin_json()
        # The skill name must not be required in plugin.json — confirm it's absent
        # from any list-type field (which would imply a hardcoded registry).
        for key, value in plugin.items():
            if isinstance(value, list):
                assert skill_name not in value, (
                    f"Skill '{skill_name}' is hardcoded in plugin.json['{key}']; "
                    "auto-discovery must work without manifest changes"
                )
