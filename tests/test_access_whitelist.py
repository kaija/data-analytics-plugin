"""Unit tests for AccessWhitelist enforcement.

Requirements: 3.9, 3.10, 3.11, 3.12
"""

import json
import os
import sys

import pytest

# Add the scripts directory to the path so we can import the module directly
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "skills", "athena-glue", "scripts"),
)

from access_whitelist import (
    AccessWhitelist,
    WhitelistConfigError,
    WhitelistValidationResult,
)


@pytest.fixture
def whitelist_dir(tmp_path):
    """Create a temp directory with a valid whitelist config."""
    config = {
        "version": "1.0",
        "allowed": [
            {"database": "analytics_prod", "tables": ["events", "users", "sessions"]},
            {"database": "data_lake", "tables": ["*"]},
        ],
    }
    config_path = tmp_path / "access-whitelist.json"
    config_path.write_text(json.dumps(config))
    return str(config_path)


class TestAccessWhitelistInit:
    """Tests for whitelist loading and validation."""

    def test_load_valid_config(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        assert wl.is_authorized("analytics_prod", "events")

    def test_missing_file_raises_error(self, tmp_path):
        missing = str(tmp_path / "nonexistent.json")
        with pytest.raises(WhitelistConfigError, match="not found"):
            AccessWhitelist(config_path=missing)

    def test_malformed_json_raises_error(self, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("{not valid json")
        with pytest.raises(WhitelistConfigError, match="invalid JSON"):
            AccessWhitelist(config_path=str(bad_file))

    def test_missing_version_key(self, tmp_path):
        config_path = tmp_path / "wl.json"
        config_path.write_text(json.dumps({"allowed": []}))
        with pytest.raises(WhitelistConfigError, match="'version'"):
            AccessWhitelist(config_path=str(config_path))

    def test_missing_allowed_key(self, tmp_path):
        config_path = tmp_path / "wl.json"
        config_path.write_text(json.dumps({"version": "1.0"}))
        with pytest.raises(WhitelistConfigError, match="'allowed'"):
            AccessWhitelist(config_path=str(config_path))

    def test_allowed_not_array(self, tmp_path):
        config_path = tmp_path / "wl.json"
        config_path.write_text(json.dumps({"version": "1.0", "allowed": "bad"}))
        with pytest.raises(WhitelistConfigError, match="must be an array"):
            AccessWhitelist(config_path=str(config_path))

    def test_entry_missing_database(self, tmp_path):
        config_path = tmp_path / "wl.json"
        config_path.write_text(json.dumps({
            "version": "1.0",
            "allowed": [{"tables": ["t1"]}],
        }))
        with pytest.raises(WhitelistConfigError, match="'database'"):
            AccessWhitelist(config_path=str(config_path))

    def test_entry_missing_tables(self, tmp_path):
        config_path = tmp_path / "wl.json"
        config_path.write_text(json.dumps({
            "version": "1.0",
            "allowed": [{"database": "db1"}],
        }))
        with pytest.raises(WhitelistConfigError, match="'tables'"):
            AccessWhitelist(config_path=str(config_path))


class TestIsAuthorized:
    """Tests for is_authorized method."""

    def test_authorized_database_and_table(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        assert wl.is_authorized("analytics_prod", "events") is True

    def test_unauthorized_table(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        assert wl.is_authorized("analytics_prod", "secret_table") is False

    def test_unauthorized_database(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        assert wl.is_authorized("unknown_db", "any_table") is False

    def test_wildcard_allows_any_table(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        assert wl.is_authorized("data_lake", "anything") is True
        assert wl.is_authorized("data_lake", "another_table") is True

    def test_database_only_check(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        assert wl.is_authorized("analytics_prod") is True
        assert wl.is_authorized("unknown_db") is False


class TestValidateQuery:
    """Tests for validate_query method."""

    def test_authorized_query(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        result = wl.validate_query("SELECT * FROM analytics_prod.events")
        assert result.authorized is True
        assert result.unauthorized_resources == []

    def test_unauthorized_query(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        result = wl.validate_query("SELECT * FROM secret_db.passwords")
        assert result.authorized is False
        assert len(result.unauthorized_resources) == 1
        assert result.unauthorized_resources[0].database == "secret_db"
        assert result.unauthorized_resources[0].table == "passwords"

    def test_mixed_authorized_and_unauthorized(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        sql = (
            "SELECT * FROM analytics_prod.events e "
            "JOIN secret_db.passwords p ON e.id = p.id"
        )
        result = wl.validate_query(sql)
        assert result.authorized is False
        assert len(result.unauthorized_resources) == 1
        assert result.unauthorized_resources[0].database == "secret_db"

    def test_join_multiple_tables(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        sql = (
            "SELECT * FROM analytics_prod.events e "
            "JOIN analytics_prod.users u ON e.user_id = u.id "
            "JOIN analytics_prod.sessions s ON u.id = s.user_id"
        )
        result = wl.validate_query(sql)
        assert result.authorized is True

    def test_wildcard_database_query(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        result = wl.validate_query("SELECT * FROM data_lake.any_table_name")
        assert result.authorized is True

    def test_to_dict(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        result = wl.validate_query("SELECT * FROM secret_db.passwords")
        d = result.to_dict()
        assert d["authorized"] is False
        assert d["unauthorized_resources"] == [
            {"database": "secret_db", "table": "passwords"}
        ]

    def test_bare_table_name(self, whitelist_dir):
        """Bare table names (no database prefix) are extracted with table=None."""
        wl = AccessWhitelist(config_path=whitelist_dir)
        result = wl.validate_query("SELECT * FROM some_table")
        # bare name -> database="some_table", table=None
        # "some_table" is not a whitelisted database, so unauthorized
        assert result.authorized is False

    def test_case_insensitive_keywords(self, whitelist_dir):
        wl = AccessWhitelist(config_path=whitelist_dir)
        result = wl.validate_query("select * from analytics_prod.events")
        assert result.authorized is True


class TestExtractTableReferences:
    """Tests for the SQL table reference extraction."""

    def test_simple_from(self):
        refs = AccessWhitelist._extract_table_references(
            "SELECT * FROM db.tbl"
        )
        assert refs == [("db", "tbl")]

    def test_multiple_joins(self):
        sql = "SELECT * FROM db.t1 JOIN db.t2 ON t1.id = t2.id LEFT JOIN db.t3 ON t2.id = t3.id"
        refs = AccessWhitelist._extract_table_references(sql)
        assert ("db", "t1") in refs
        assert ("db", "t2") in refs
        assert ("db", "t3") in refs

    def test_bare_table(self):
        refs = AccessWhitelist._extract_table_references("SELECT * FROM mytable")
        assert refs == [("mytable", None)]

    def test_deduplication(self):
        sql = "SELECT * FROM db.tbl UNION SELECT * FROM db.tbl"
        refs = AccessWhitelist._extract_table_references(sql)
        assert refs.count(("db", "tbl")) == 1

    def test_backtick_quoted(self):
        refs = AccessWhitelist._extract_table_references(
            "SELECT * FROM `db`.`tbl`"
        )
        assert refs == [("db", "tbl")]
