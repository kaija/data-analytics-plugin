"""Property tests for credential resolver priority order and failure reporting.

Feature: data-analytics-skill-suite, Property 9: Credential resolver follows priority order
**Validates: Requirements 3.7**

Feature: data-analytics-skill-suite, Property 10: Credential resolution failure lists all attempted methods
**Validates: Requirements 3.8**
"""

from unittest.mock import patch

from hypothesis import given, settings, strategies as st

import sys
import os

# Ensure the skills module is importable
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "skills", "athena-glue", "scripts")
)

from credential_resolver import AWSCredentials, CredentialResolver, CredentialResolutionError


# --- The canonical priority order ---

DISCOVERY_ORDER = [
    "iam_role",
    "environment_variables",
    "shared_credential_file",
    "aws_config_file",
    "named_profile",
]

# --- Strategies ---

# Generate a non-empty subset of method indices (at least one source available)
available_indices = st.lists(
    st.integers(min_value=0, max_value=len(DISCOVERY_ORDER) - 1),
    min_size=1,
    max_size=len(DISCOVERY_ORDER),
    unique=True,
)


def _make_creds(method_name: str) -> AWSCredentials:
    """Create a distinguishable AWSCredentials for a given method."""
    return AWSCredentials(
        access_key=f"AK_{method_name}",
        secret_key=f"SK_{method_name}",
        token=f"TK_{method_name}",
        method=method_name,
    )


def _patch_resolver(resolver: CredentialResolver, available: set[int]):
    """Patch _try_* methods: available indices return creds, others return None."""
    patchers = []
    for idx, method_name in enumerate(DISCOVERY_ORDER):
        fn_name = f"_try_{method_name}"
        if idx in available:
            p = patch.object(resolver, fn_name, return_value=_make_creds(method_name))
        else:
            p = patch.object(resolver, fn_name, return_value=None)
        patchers.append(p)
    return patchers


class TestCredentialResolverPriorityOrder:
    """Property 9: Credential resolver follows priority order."""

    @given(indices=available_indices)
    @settings(max_examples=100)
    def test_resolver_returns_highest_priority_source(self, indices):
        """For any non-empty subset of available credential sources,
        resolve() must return credentials from the source with the
        smallest index (highest priority) in DISCOVERY_ORDER."""
        available = set(indices)
        expected_idx = min(available)
        expected_method = DISCOVERY_ORDER[expected_idx]

        resolver = CredentialResolver()
        patchers = _patch_resolver(resolver, available)

        for p in patchers:
            p.start()
        try:
            creds = resolver.resolve()
            assert creds.method == expected_method, (
                f"Expected method '{expected_method}' (priority {expected_idx}) "
                f"but got '{creds.method}'. Available sources: "
                f"{[DISCOVERY_ORDER[i] for i in sorted(available)]}"
            )
            assert creds.access_key == f"AK_{expected_method}"
            assert creds.secret_key == f"SK_{expected_method}"
        finally:
            for p in patchers:
                p.stop()

    @given(indices=available_indices)
    @settings(max_examples=100)
    def test_resolver_does_not_call_lower_priority_after_success(self, indices):
        """Once a higher-priority source succeeds, lower-priority sources
        should not be attempted."""
        available = set(indices)
        highest_idx = min(available)

        resolver = CredentialResolver()
        patchers = _patch_resolver(resolver, available)

        for p in patchers:
            p.start()
        try:
            creds = resolver.resolve()
            # Verify methods after the winning one were not called
            for idx in range(highest_idx + 1, len(DISCOVERY_ORDER)):
                fn_name = f"_try_{DISCOVERY_ORDER[idx]}"
                mock_fn = getattr(resolver, fn_name)
                assert not mock_fn.called, (
                    f"Lower-priority method '{DISCOVERY_ORDER[idx]}' "
                    f"(index {idx}) was called even though "
                    f"'{DISCOVERY_ORDER[highest_idx]}' (index {highest_idx}) succeeded"
                )
        finally:
            for p in patchers:
                p.stop()

    def test_discovery_order_matches_design(self):
        """The resolver's DISCOVERY_ORDER must match the documented priority."""
        assert CredentialResolver.DISCOVERY_ORDER == DISCOVERY_ORDER, (
            f"DISCOVERY_ORDER mismatch.\n"
            f"Expected: {DISCOVERY_ORDER}\n"
            f"Actual:   {CredentialResolver.DISCOVERY_ORDER}"
        )


# --- Strategies for Property 10 ---

# Generate a failure reason string for each discovery method
failure_reason = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P", "Z")),
    min_size=1,
    max_size=100,
)

# Generate a list of exactly 5 failure reasons (one per method)
failure_reasons_list = st.lists(failure_reason, min_size=5, max_size=5)


def _patch_resolver_all_fail(resolver: CredentialResolver, reasons: list[str]):
    """Patch all _try_* methods to raise exceptions with the given reasons."""
    patchers = []
    for idx, method_name in enumerate(DISCOVERY_ORDER):
        fn_name = f"_try_{method_name}"
        p = patch.object(resolver, fn_name, side_effect=Exception(reasons[idx]))
        patchers.append(p)
    return patchers


def _patch_resolver_all_return_none(resolver: CredentialResolver):
    """Patch all _try_* methods to return None (no credentials found)."""
    patchers = []
    for method_name in DISCOVERY_ORDER:
        fn_name = f"_try_{method_name}"
        p = patch.object(resolver, fn_name, return_value=None)
        patchers.append(p)
    return patchers


class TestCredentialResolutionFailure:
    """Property 10: Credential resolution failure lists all attempted methods.

    **Validates: Requirements 3.8**
    """

    @given(reasons=failure_reasons_list)
    @settings(max_examples=100)
    def test_failure_error_contains_all_attempted_methods_with_exceptions(self, reasons):
        """When all credential discovery methods raise exceptions,
        the CredentialResolutionError should list all 5 attempted methods
        with their failure reasons."""
        resolver = CredentialResolver()
        patchers = _patch_resolver_all_fail(resolver, reasons)

        for p in patchers:
            p.start()
        try:
            try:
                resolver.resolve()
                assert False, "resolve() should have raised CredentialResolutionError"
            except CredentialResolutionError as exc:
                attempted = exc.attempted_methods

                # Must have exactly 5 entries (one per method)
                assert len(attempted) == len(DISCOVERY_ORDER), (
                    f"Expected {len(DISCOVERY_ORDER)} attempted methods, "
                    f"got {len(attempted)}"
                )

                # Each entry must have 'method' and 'result' keys
                for entry in attempted:
                    assert "method" in entry, f"Entry missing 'method' key: {entry}"
                    assert "result" in entry, f"Entry missing 'result' key: {entry}"

                # Methods must match DISCOVERY_ORDER in order
                actual_methods = [e["method"] for e in attempted]
                assert actual_methods == DISCOVERY_ORDER, (
                    f"Method order mismatch.\n"
                    f"Expected: {DISCOVERY_ORDER}\n"
                    f"Actual:   {actual_methods}"
                )

                # Each result should contain the corresponding failure reason
                for idx, entry in enumerate(attempted):
                    assert entry["result"] == reasons[idx], (
                        f"Method '{entry['method']}' result mismatch.\n"
                        f"Expected: {reasons[idx]!r}\n"
                        f"Actual:   {entry['result']!r}"
                    )
        finally:
            for p in patchers:
                p.stop()

    def test_failure_error_contains_all_methods_when_returning_none(self):
        """When all credential discovery methods return None (no credentials),
        the CredentialResolutionError should still list all 5 attempted methods."""
        resolver = CredentialResolver()
        patchers = _patch_resolver_all_return_none(resolver)

        for p in patchers:
            p.start()
        try:
            try:
                resolver.resolve()
                assert False, "resolve() should have raised CredentialResolutionError"
            except CredentialResolutionError as exc:
                attempted = exc.attempted_methods

                assert len(attempted) == len(DISCOVERY_ORDER), (
                    f"Expected {len(DISCOVERY_ORDER)} attempted methods, "
                    f"got {len(attempted)}"
                )

                for entry in attempted:
                    assert "method" in entry, f"Entry missing 'method' key: {entry}"
                    assert "result" in entry, f"Entry missing 'result' key: {entry}"

                actual_methods = [e["method"] for e in attempted]
                assert actual_methods == DISCOVERY_ORDER, (
                    f"Method order mismatch.\n"
                    f"Expected: {DISCOVERY_ORDER}\n"
                    f"Actual:   {actual_methods}"
                )
        finally:
            for p in patchers:
                p.stop()
