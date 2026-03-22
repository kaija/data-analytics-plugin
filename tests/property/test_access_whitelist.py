"""Property tests for whitelist enforcement.

Feature: data-analytics-skill-suite, Property 11: Whitelist enforcement rejects unauthorized resources and identifies them
**Validates: Requirements 3.9, 3.10**
"""

import json
import os
import sys
import tempfile

from hypothesis import given, settings, assume, strategies as st

# Ensure the skills module is importable
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "skills", "athena-glue", "scripts"),
)

from access_whitelist import AccessWhitelist, WhitelistValidationResult


# --- Strategies ---

# Valid SQL identifier: starts with a letter, followed by letters/digits/underscores
sql_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)

# A whitelist entry: a database name with a list of table names
whitelist_entry = st.fixed_dictionaries({
    "database": sql_identifier,
    "tables": st.lists(sql_identifier, min_size=1, max_size=5, unique=True),
})

# A whitelist config with at least one allowed entry (unique databases)
whitelist_config = st.lists(
    whitelist_entry, min_size=1, max_size=5
).filter(
    lambda entries: len({e["database"] for e in entries}) == len(entries)
)


def _create_whitelist(allowed_entries):
    """Write a whitelist config to a temp file and return an AccessWhitelist."""
    config = {"version": "1.0", "allowed": allowed_entries}
    fd, path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(config, f)
        return AccessWhitelist(config_path=path)
    finally:
        os.unlink(path)


class TestWhitelistEnforcementRejectsUnauthorized:
    """Property 11: Whitelist enforcement rejects unauthorized resources and identifies them.

    **Validates: Requirements 3.9, 3.10**
    """

    @given(
        config_entries=whitelist_config,
        unauth_db=sql_identifier,
        unauth_table=sql_identifier,
    )
    @settings(max_examples=100)
    def test_unauthorized_database_is_rejected(self, config_entries, unauth_db, unauth_table):
        """For any whitelist config, a query referencing a database NOT in the
        whitelist must be rejected, and the unauthorized resource must be
        identified in the result."""
        allowed_dbs = {e["database"] for e in config_entries}
        assume(unauth_db not in allowed_dbs)

        wl = _create_whitelist(config_entries)

        sql = f"SELECT * FROM {unauth_db}.{unauth_table}"
        result = wl.validate_query(sql)

        assert result.authorized is False, (
            f"Query referencing unauthorized database '{unauth_db}' should be rejected"
        )
        unauth_dbs = {r.database for r in result.unauthorized_resources}
        assert unauth_db in unauth_dbs, (
            f"Unauthorized database '{unauth_db}' not listed in unauthorized_resources: "
            f"{[r.database for r in result.unauthorized_resources]}"
        )

    @given(
        config_entries=whitelist_config,
        unauth_table=sql_identifier,
    )
    @settings(max_examples=100)
    def test_unauthorized_table_is_rejected(self, config_entries, unauth_table):
        """For any whitelist config with specific tables (no wildcard), a query
        referencing a table NOT in the allowed list must be rejected, and the
        unauthorized table must be identified."""
        non_wildcard = [e for e in config_entries if "*" not in e["tables"]]
        assume(len(non_wildcard) > 0)

        target_entry = non_wildcard[0]
        db = target_entry["database"]
        allowed_tables = set(target_entry["tables"])
        assume(unauth_table not in allowed_tables)

        wl = _create_whitelist(config_entries)

        sql = f"SELECT * FROM {db}.{unauth_table}"
        result = wl.validate_query(sql)

        assert result.authorized is False, (
            f"Query referencing unauthorized table '{db}.{unauth_table}' should be rejected"
        )
        unauth_pairs = [(r.database, r.table) for r in result.unauthorized_resources]
        assert (db, unauth_table) in unauth_pairs, (
            f"Unauthorized resource '{db}.{unauth_table}' not listed in "
            f"unauthorized_resources: {unauth_pairs}"
        )

    @given(
        config_entries=whitelist_config,
        unauth_db=sql_identifier,
        unauth_table=sql_identifier,
    )
    @settings(max_examples=100)
    def test_sql_with_unauthorized_join_identifies_all_unauthorized(
        self, config_entries, unauth_db, unauth_table
    ):
        """For any whitelist config, a SQL query with JOINs referencing
        unauthorized tables must identify ALL unauthorized resources."""
        allowed_dbs = {e["database"] for e in config_entries}
        assume(unauth_db not in allowed_dbs)

        auth_entry = config_entries[0]
        auth_db = auth_entry["database"]
        auth_table = auth_entry["tables"][0]

        wl = _create_whitelist(config_entries)

        sql = (
            f"SELECT * FROM {auth_db}.{auth_table} a "
            f"JOIN {unauth_db}.{unauth_table} b ON a.id = b.id"
        )
        result = wl.validate_query(sql)

        assert result.authorized is False, (
            f"Query with unauthorized JOIN target '{unauth_db}.{unauth_table}' "
            f"should be rejected"
        )
        unauth_dbs = {r.database for r in result.unauthorized_resources}
        assert unauth_db in unauth_dbs, (
            f"Unauthorized database '{unauth_db}' not found in unauthorized_resources"
        )


class TestWhitelistEnforcementAllowsAuthorized:
    """Property 11 (converse): Whitelisted resources are always authorized.

    **Validates: Requirements 3.9, 3.10**
    """

    @given(config_entries=whitelist_config)
    @settings(max_examples=100)
    def test_whitelisted_database_table_is_authorized(self, config_entries):
        """For any whitelist config, is_authorized must return True for every
        database/table pair explicitly listed in the whitelist."""
        wl = _create_whitelist(config_entries)

        for entry in config_entries:
            db = entry["database"]
            assert wl.is_authorized(db) is True, (
                f"Database '{db}' should be authorized"
            )
            for table in entry["tables"]:
                if table != "*":
                    assert wl.is_authorized(db, table) is True, (
                        f"Table '{db}.{table}' should be authorized"
                    )

    @given(config_entries=whitelist_config)
    @settings(max_examples=100)
    def test_whitelisted_query_is_authorized(self, config_entries):
        """For any whitelist config, a SQL query referencing ONLY whitelisted
        database.table pairs must be authorized."""
        wl = _create_whitelist(config_entries)

        entry = config_entries[0]
        db = entry["database"]
        table = entry["tables"][0]

        sql = f"SELECT * FROM {db}.{table}"
        result = wl.validate_query(sql)

        assert result.authorized is True, (
            f"Query referencing whitelisted '{db}.{table}' should be authorized, "
            f"but got unauthorized_resources: "
            f"{[(r.database, r.table) for r in result.unauthorized_resources]}"
        )
        assert result.unauthorized_resources == []

    @given(
        config_entries=whitelist_config,
        extra_table=sql_identifier,
    )
    @settings(max_examples=100)
    def test_wildcard_database_authorizes_any_table(self, config_entries, extra_table):
        """For any whitelist config with a wildcard ('*') database entry,
        any table in that database must be authorized."""
        wildcard_db = "wildcard_db"
        assume(wildcard_db not in {e["database"] for e in config_entries})
        entries_with_wildcard = config_entries + [
            {"database": wildcard_db, "tables": ["*"]}
        ]

        wl = _create_whitelist(entries_with_wildcard)

        assert wl.is_authorized(wildcard_db, extra_table) is True, (
            f"Wildcard database '{wildcard_db}' should authorize any table, "
            f"but '{extra_table}' was rejected"
        )

        sql = f"SELECT * FROM {wildcard_db}.{extra_table}"
        result = wl.validate_query(sql)
        assert result.authorized is True
