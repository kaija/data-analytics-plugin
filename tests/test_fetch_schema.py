"""Unit tests for fetch_schema.py."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, "skills/athena-glue/scripts")

from fetch_schema import fetch_schema, main


class TestFetchSchema:
    """Tests for the fetch_schema function."""

    @patch("fetch_schema.boto3.Session")
    @patch("fetch_schema.CredentialResolver")
    def test_returns_columns_and_partition_keys(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        client = MagicMock()
        client.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "user_id", "Type": "string"},
                        {"Name": "event_ts", "Type": "timestamp"},
                    ]
                },
                "PartitionKeys": [
                    {"Name": "year", "Type": "int"},
                    {"Name": "month", "Type": "int"},
                ],
            }
        }
        mock_session_cls.return_value.client.return_value = client

        result = fetch_schema("analytics", "events")

        assert result == {
            "columns": [
                {"name": "user_id", "type": "string"},
                {"name": "event_ts", "type": "timestamp"},
            ],
            "partition_keys": [
                {"name": "year", "type": "int"},
                {"name": "month", "type": "int"},
            ],
        }
        client.get_table.assert_called_once_with(
            DatabaseName="analytics", Name="events"
        )

    @patch("fetch_schema.boto3.Session")
    @patch("fetch_schema.CredentialResolver")
    def test_no_partition_keys(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        client = MagicMock()
        client.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {
                    "Columns": [{"Name": "id", "Type": "bigint"}]
                },
                "PartitionKeys": [],
            }
        }
        mock_session_cls.return_value.client.return_value = client

        result = fetch_schema("db", "tbl")

        assert result["columns"] == [{"name": "id", "type": "bigint"}]
        assert result["partition_keys"] == []

    @patch("fetch_schema.boto3.Session")
    @patch("fetch_schema.CredentialResolver")
    def test_empty_columns_and_partitions(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        client = MagicMock()
        client.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {"Columns": []},
            }
        }
        mock_session_cls.return_value.client.return_value = client

        result = fetch_schema("db", "empty_tbl")

        assert result["columns"] == []
        assert result["partition_keys"] == []

    @patch("fetch_schema.boto3.Session")
    @patch("fetch_schema.CredentialResolver")
    def test_passes_region(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        client = MagicMock()
        client.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {"Columns": []},
                "PartitionKeys": [],
            }
        }
        mock_session_cls.return_value.client.return_value = client

        fetch_schema("db", "tbl", region="eu-west-1")

        mock_session_cls.assert_called_once_with(
            aws_access_key_id="ak",
            aws_secret_access_key="sk",
            aws_session_token=None,
            region_name="eu-west-1",
        )

    @patch("fetch_schema.CredentialResolver")
    def test_credential_error_propagates(self, mock_resolver_cls):
        from credential_resolver import CredentialResolutionError

        mock_resolver_cls.return_value.resolve.side_effect = CredentialResolutionError(
            [{"method": "environment_variables", "result": "no credentials found"}]
        )

        with pytest.raises(CredentialResolutionError):
            fetch_schema("db", "tbl")


class TestMain:
    """Tests for the main() entry point."""

    @patch("fetch_schema.fetch_schema")
    def test_prints_json_to_stdout(self, mock_fetch, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["fetch_schema.py", "my_db", "my_table"])
        mock_fetch.return_value = {
            "columns": [{"name": "id", "type": "int"}],
            "partition_keys": [],
        }

        main()

        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert output["header"]["source_type"] == "athena"
        assert output["header"]["result_count"] == 1
        assert output["data"][0]["name"] == "id"
        assert output["data"][0]["type"] == "int"

    @patch("fetch_schema.fetch_schema")
    def test_credential_error_exits_1(self, mock_fetch, monkeypatch):
        from credential_resolver import CredentialResolutionError

        monkeypatch.setattr(sys, "argv", ["fetch_schema.py", "db", "tbl"])
        mock_fetch.side_effect = CredentialResolutionError(
            [{"method": "iam_role", "result": "timeout"}]
        )

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("fetch_schema.fetch_schema")
    def test_credential_error_stderr_lists_attempted_methods(self, mock_fetch, capsys, monkeypatch):
        """Req 3.8: credential error message lists attempted discovery methods."""
        from credential_resolver import CredentialResolutionError

        monkeypatch.setattr(sys, "argv", ["fetch_schema.py", "db", "tbl"])
        mock_fetch.side_effect = CredentialResolutionError(
            [
                {"method": "iam_role", "result": "timeout"},
                {"method": "environment_variables", "result": "no credentials found"},
            ]
        )

        with pytest.raises(SystemExit):
            main()

        captured = capsys.readouterr()
        err = json.loads(captured.err)
        assert err["error"] == "credential_resolution_failed"
        assert len(err["attempted_methods"]) == 2
        assert err["attempted_methods"][0]["method"] == "iam_role"

    @patch("fetch_schema.fetch_schema")
    def test_api_error_exits_1(self, mock_fetch, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["fetch_schema.py", "db", "tbl"])
        mock_fetch.side_effect = RuntimeError("Glue service unavailable")

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("fetch_schema.fetch_schema")
    def test_api_error_stderr_format(self, mock_fetch, capsys, monkeypatch):
        """Verify API error stderr contains structured JSON error."""
        monkeypatch.setattr(sys, "argv", ["fetch_schema.py", "db", "tbl"])
        mock_fetch.side_effect = RuntimeError("Glue service unavailable")

        with pytest.raises(SystemExit):
            main()

        captured = capsys.readouterr()
        err = json.loads(captured.err)
        assert err["error"] == "glue_api_error"
        assert "Glue service unavailable" in err["message"]

    def test_missing_arguments_exits_1(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["fetch_schema.py"])

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_missing_table_argument_exits_1(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["fetch_schema.py", "my_db"])

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
