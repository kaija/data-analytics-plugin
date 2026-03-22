"""Unit tests for preview_data.py."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, "skills/athena-glue/scripts")

from preview_data import preview_data, main, DEFAULT_MAX_ROWS


class TestPreviewData:
    """Tests for the preview_data function."""

    @patch("preview_data.boto3.Session")
    @patch("preview_data.CredentialResolver")
    @patch("preview_data.AccessWhitelist")
    def test_returns_columns_and_rows(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl_cls.return_value.is_authorized.return_value = True
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "id"},
                        {"Name": "name"},
                    ]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]},
                    {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "Alice"}]},
                    {"Data": [{"VarCharValue": "2"}, {"VarCharValue": "Bob"}]},
                ],
            }
        }
        mock_session_cls.return_value.client.return_value = athena

        result = preview_data("analytics_prod", "users", whitelist_path="dummy")

        assert result["columns"] == ["id", "name"]
        assert result["rows"] == [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]

    @patch("preview_data.boto3.Session")
    @patch("preview_data.CredentialResolver")
    @patch("preview_data.AccessWhitelist")
    def test_sql_contains_limit(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl_cls.return_value.is_authorized.return_value = True
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": []},
                "Rows": [],
            }
        }
        mock_session_cls.return_value.client.return_value = athena

        preview_data("db", "tbl", max_rows=50, whitelist_path="dummy")

        call_args = athena.start_query_execution.call_args
        sql = call_args[1]["QueryString"] if "QueryString" in call_args[1] else call_args[0][0]
        assert "LIMIT 50" in sql

    @patch("preview_data.AccessWhitelist")
    def test_whitelist_rejection(self, mock_wl_cls):
        mock_wl_cls.return_value.is_authorized.return_value = False

        with pytest.raises(PermissionError, match="access whitelist"):
            preview_data("secret_db", "secret_tbl", whitelist_path="dummy")

    @patch("preview_data.AccessWhitelist")
    def test_whitelist_rejection_identifies_resource(self, mock_wl_cls):
        """Req 3.10: error message identifies the unauthorized resource."""
        mock_wl_cls.return_value.is_authorized.return_value = False

        with pytest.raises(PermissionError, match="secret_db.secret_tbl"):
            preview_data("secret_db", "secret_tbl", whitelist_path="dummy")

    @patch("preview_data.AccessWhitelist")
    def test_whitelist_config_error_propagates(self, mock_wl_cls):
        from access_whitelist import WhitelistConfigError

        mock_wl_cls.side_effect = WhitelistConfigError("file not found")

        with pytest.raises(WhitelistConfigError):
            preview_data("db", "tbl", whitelist_path="bad_path")

    @patch("preview_data.AccessWhitelist")
    @patch("preview_data.CredentialResolver")
    def test_credential_error_propagates(self, mock_resolver_cls, mock_wl_cls):
        from credential_resolver import CredentialResolutionError

        mock_wl_cls.return_value.is_authorized.return_value = True
        mock_resolver_cls.return_value.resolve.side_effect = CredentialResolutionError(
            [{"method": "environment_variables", "result": "no credentials found"}]
        )

        with pytest.raises(CredentialResolutionError):
            preview_data("db", "tbl", whitelist_path="dummy")

    @patch("preview_data.boto3.Session")
    @patch("preview_data.CredentialResolver")
    @patch("preview_data.AccessWhitelist")
    def test_query_failure_raises(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl_cls.return_value.is_authorized.return_value = True
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "Syntax error",
                }
            }
        }
        mock_session_cls.return_value.client.return_value = athena

        with pytest.raises(RuntimeError, match="FAILED"):
            preview_data("db", "tbl", whitelist_path="dummy")

    @patch("preview_data.boto3.Session")
    @patch("preview_data.CredentialResolver")
    @patch("preview_data.AccessWhitelist")
    def test_empty_result_set(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl_cls.return_value.is_authorized.return_value = True
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "id"}, {"Name": "val"}]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "val"}]},
                ],
            }
        }
        mock_session_cls.return_value.client.return_value = athena

        result = preview_data("db", "tbl", whitelist_path="dummy")

        assert result["columns"] == ["id", "val"]
        assert result["rows"] == []

    @patch("preview_data.boto3.Session")
    @patch("preview_data.CredentialResolver")
    @patch("preview_data.AccessWhitelist")
    def test_default_max_rows_is_100(self, mock_wl_cls, mock_resolver_cls, mock_session_cls):
        mock_wl_cls.return_value.is_authorized.return_value = True
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": []},
                "Rows": [],
            }
        }
        mock_session_cls.return_value.client.return_value = athena

        preview_data("db", "tbl", whitelist_path="dummy")

        call_args = athena.start_query_execution.call_args
        sql = call_args[1]["QueryString"]
        assert "LIMIT 100" in sql


class TestMain:
    """Tests for the main() entry point."""

    @patch("preview_data.preview_data")
    def test_prints_json_to_stdout(self, mock_preview, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["preview_data.py", "my_db", "my_table"])
        mock_preview.return_value = {
            "columns": ["id"],
            "rows": [{"id": "1"}],
        }

        main()

        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert output["header"]["source_type"] == "athena"
        assert output["header"]["result_count"] == 1
        assert output["data"][0]["id"] == "1"

    @patch("preview_data.preview_data")
    def test_custom_max_rows(self, mock_preview, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["preview_data.py", "db", "tbl", "25"])
        mock_preview.return_value = {"columns": [], "rows": []}

        main()

        mock_preview.assert_called_once_with("db", "tbl", max_rows=25)

    @patch("preview_data.preview_data")
    def test_default_max_rows(self, mock_preview, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["preview_data.py", "db", "tbl"])
        mock_preview.return_value = {"columns": [], "rows": []}

        main()

        mock_preview.assert_called_once_with("db", "tbl", max_rows=DEFAULT_MAX_ROWS)

    def test_missing_arguments_exits_1(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["preview_data.py"])

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_missing_table_argument_exits_1(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["preview_data.py", "my_db"])

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_invalid_max_rows_exits_1(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["preview_data.py", "db", "tbl", "abc"])

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("preview_data.preview_data")
    def test_whitelist_error_exits_1(self, mock_preview, monkeypatch):
        from access_whitelist import WhitelistConfigError

        monkeypatch.setattr(sys, "argv", ["preview_data.py", "db", "tbl"])
        mock_preview.side_effect = WhitelistConfigError("missing file")

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("preview_data.preview_data")
    def test_access_denied_exits_1(self, mock_preview, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["preview_data.py", "db", "tbl"])
        mock_preview.side_effect = PermissionError("Access denied")

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        err = json.loads(captured.err)
        assert err["error"] == "access_denied"

    @patch("preview_data.preview_data")
    def test_credential_error_exits_1(self, mock_preview, monkeypatch):
        from credential_resolver import CredentialResolutionError

        monkeypatch.setattr(sys, "argv", ["preview_data.py", "db", "tbl"])
        mock_preview.side_effect = CredentialResolutionError(
            [{"method": "iam_role", "result": "timeout"}]
        )

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("preview_data.preview_data")
    def test_api_error_exits_1(self, mock_preview, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["preview_data.py", "db", "tbl"])
        mock_preview.side_effect = RuntimeError("Athena service unavailable")

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        err = json.loads(captured.err)
        assert err["error"] == "athena_api_error"
