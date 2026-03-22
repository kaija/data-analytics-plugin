"""Unit tests for execute_query.py."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, "skills/athena-glue/scripts")

from execute_query import execute_query, main


def _make_athena_mock(state="SUCCEEDED", columns=None, rows=None, statistics=None):
    """Helper to build a mock Athena client with standard responses."""
    athena = MagicMock()
    athena.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}

    stats = statistics or {
        "DataScannedInBytes": 1024,
        "TotalExecutionTimeInMillis": 250,
    }
    status_resp = {
        "QueryExecution": {
            "Status": {"State": state},
            "Statistics": stats,
        }
    }
    if state == "FAILED":
        status_resp["QueryExecution"]["Status"]["StateChangeReason"] = "Syntax error"

    athena.get_query_execution.return_value = status_resp

    col_info = [{"Name": c} for c in (columns or [])]
    result_rows = rows or []
    athena.get_query_results.return_value = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": col_info},
            "Rows": result_rows,
        }
    }
    return athena


class TestExecuteQuery:
    """Tests for the execute_query function."""

    @patch("execute_query.boto3.Session")
    @patch("execute_query.CredentialResolver")
    @patch("execute_query.AccessWhitelist")
    def test_returns_columns_rows_and_metadata(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(authorized=True, unauthorized_resources=[])
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = _make_athena_mock(
            columns=["id", "name"],
            rows=[
                {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]},
                {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "Alice"}]},
            ],
        )
        mock_session_cls.return_value.client.return_value = athena

        result = execute_query("SELECT * FROM db.tbl", whitelist_path="dummy")

        assert result["columns"] == ["id", "name"]
        assert result["rows"] == [{"id": "1", "name": "Alice"}]
        assert result["data_scanned_bytes"] == 1024
        assert result["execution_time_ms"] == 250

    @patch("execute_query.boto3.Session")
    @patch("execute_query.CredentialResolver")
    @patch("execute_query.AccessWhitelist")
    def test_metadata_always_present(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(authorized=True, unauthorized_resources=[])
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = _make_athena_mock(columns=[], rows=[], statistics={})
        mock_session_cls.return_value.client.return_value = athena

        result = execute_query("SELECT 1", whitelist_path="dummy")

        assert "data_scanned_bytes" in result
        assert "execution_time_ms" in result
        assert isinstance(result["data_scanned_bytes"], int)
        assert isinstance(result["execution_time_ms"], int)

    @patch("execute_query.AccessWhitelist")
    def test_whitelist_rejection(self, mock_wl_cls):
        from access_whitelist import UnauthorizedResource

        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(
            authorized=False,
            unauthorized_resources=[UnauthorizedResource(database="secret_db", table="secret_tbl")],
        )

        with pytest.raises(PermissionError, match="unauthorized resources"):
            execute_query("SELECT * FROM secret_db.secret_tbl", whitelist_path="dummy")

    @patch("execute_query.AccessWhitelist")
    def test_whitelist_rejection_identifies_resource(self, mock_wl_cls):
        """Req 3.10: error message identifies the unauthorized resource."""
        from access_whitelist import UnauthorizedResource

        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(
            authorized=False,
            unauthorized_resources=[UnauthorizedResource(database="secret_db", table="secret_tbl")],
        )

        with pytest.raises(PermissionError, match="secret_db.secret_tbl"):
            execute_query("SELECT * FROM secret_db.secret_tbl", whitelist_path="dummy")

    @patch("execute_query.AccessWhitelist")
    def test_whitelist_config_error_propagates(self, mock_wl_cls):
        from access_whitelist import WhitelistConfigError

        mock_wl_cls.side_effect = WhitelistConfigError("file not found")

        with pytest.raises(WhitelistConfigError):
            execute_query("SELECT 1", whitelist_path="bad_path")

    @patch("execute_query.AccessWhitelist")
    @patch("execute_query.CredentialResolver")
    def test_credential_error_propagates(self, mock_resolver_cls, mock_wl_cls):
        from credential_resolver import CredentialResolutionError

        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(authorized=True, unauthorized_resources=[])
        mock_resolver_cls.return_value.resolve.side_effect = CredentialResolutionError(
            [{"method": "environment_variables", "result": "no credentials found"}]
        )

        with pytest.raises(CredentialResolutionError):
            execute_query("SELECT 1", whitelist_path="dummy")

    @patch("execute_query.boto3.Session")
    @patch("execute_query.CredentialResolver")
    @patch("execute_query.AccessWhitelist")
    def test_query_failure_raises(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(authorized=True, unauthorized_resources=[])
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = _make_athena_mock(state="FAILED")
        mock_session_cls.return_value.client.return_value = athena

        with pytest.raises(RuntimeError, match="FAILED"):
            execute_query("SELECT bad syntax", whitelist_path="dummy")

    @patch("execute_query.boto3.Session")
    @patch("execute_query.CredentialResolver")
    @patch("execute_query.AccessWhitelist")
    def test_empty_result_set(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(authorized=True, unauthorized_resources=[])
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = _make_athena_mock(
            columns=["id", "val"],
            rows=[
                {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "val"}]},
            ],
        )
        mock_session_cls.return_value.client.return_value = athena

        result = execute_query("SELECT * FROM db.tbl WHERE 1=0", whitelist_path="dummy")

        assert result["columns"] == ["id", "val"]
        assert result["rows"] == []
        assert "data_scanned_bytes" in result
        assert "execution_time_ms" in result

    @patch("execute_query.boto3.Session")
    @patch("execute_query.CredentialResolver")
    @patch("execute_query.AccessWhitelist")
    def test_passes_sql_to_athena(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(authorized=True, unauthorized_resources=[])
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = _make_athena_mock(columns=[], rows=[])
        mock_session_cls.return_value.client.return_value = athena

        sql = "SELECT count(*) FROM analytics.events"
        execute_query(sql, whitelist_path="dummy")

        call_args = athena.start_query_execution.call_args
        assert call_args[1]["QueryString"] == sql


class TestMain:
    """Tests for the main() entry point."""

    @patch("execute_query.execute_query")
    def test_prints_json_to_stdout(self, mock_exec, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["execute_query.py", "SELECT 1"])
        mock_exec.return_value = {
            "columns": ["_col0"],
            "rows": [{"_col0": "1"}],
            "data_scanned_bytes": 0,
            "execution_time_ms": 100,
        }

        main()

        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert output["header"]["source_type"] == "athena"
        assert output["header"]["result_count"] == 1
        assert output["data"][0]["_col0"] == "1"

    def test_missing_argument_exits_1(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["execute_query.py"])

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("execute_query.execute_query")
    def test_whitelist_error_exits_1(self, mock_exec, monkeypatch):
        from access_whitelist import WhitelistConfigError

        monkeypatch.setattr(sys, "argv", ["execute_query.py", "SELECT 1"])
        mock_exec.side_effect = WhitelistConfigError("missing file")

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("execute_query.execute_query")
    def test_access_denied_exits_1(self, mock_exec, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["execute_query.py", "SELECT * FROM secret.tbl"])
        mock_exec.side_effect = PermissionError("Access denied")

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        err = json.loads(captured.err)
        assert err["error"] == "access_denied"

    @patch("execute_query.execute_query")
    def test_credential_error_exits_1(self, mock_exec, monkeypatch):
        from credential_resolver import CredentialResolutionError

        monkeypatch.setattr(sys, "argv", ["execute_query.py", "SELECT 1"])
        mock_exec.side_effect = CredentialResolutionError(
            [{"method": "iam_role", "result": "timeout"}]
        )

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("execute_query.execute_query")
    def test_api_error_exits_1(self, mock_exec, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["execute_query.py", "SELECT 1"])
        mock_exec.side_effect = RuntimeError("Athena service unavailable")

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        err = json.loads(captured.err)
        assert err["error"] == "athena_api_error"
