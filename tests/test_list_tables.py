"""Unit tests for list_tables.py."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, "skills/athena-glue/scripts")

from list_tables import list_tables, main


class TestListTables:
    """Tests for the list_tables function."""

    @patch("list_tables.boto3.Session")
    @patch("list_tables.CredentialResolver")
    def test_returns_sorted_table_names(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"TableList": [{"Name": "zebra_table"}, {"Name": "alpha_table"}]},
            {"TableList": [{"Name": "middle_table"}]},
        ]
        client = MagicMock()
        client.get_paginator.return_value = paginator
        mock_session_cls.return_value.client.return_value = client

        result = list_tables("my_database")

        assert result == ["alpha_table", "middle_table", "zebra_table"]
        paginator.paginate.assert_called_once_with(DatabaseName="my_database")

    @patch("list_tables.boto3.Session")
    @patch("list_tables.CredentialResolver")
    def test_empty_database(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        paginator = MagicMock()
        paginator.paginate.return_value = [{"TableList": []}]
        client = MagicMock()
        client.get_paginator.return_value = paginator
        mock_session_cls.return_value.client.return_value = client

        result = list_tables("empty_db")

        assert result == []

    @patch("list_tables.boto3.Session")
    @patch("list_tables.CredentialResolver")
    def test_passes_region(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        paginator = MagicMock()
        paginator.paginate.return_value = [{"TableList": []}]
        client = MagicMock()
        client.get_paginator.return_value = paginator
        mock_session_cls.return_value.client.return_value = client

        list_tables("my_db", region="us-west-2")

        mock_session_cls.assert_called_once_with(
            aws_access_key_id="ak",
            aws_secret_access_key="sk",
            aws_session_token=None,
            region_name="us-west-2",
        )

    @patch("list_tables.CredentialResolver")
    def test_credential_error_propagates(self, mock_resolver_cls):
        from credential_resolver import CredentialResolutionError

        mock_resolver_cls.return_value.resolve.side_effect = CredentialResolutionError(
            [{"method": "environment_variables", "result": "no credentials found"}]
        )

        with pytest.raises(CredentialResolutionError):
            list_tables("some_db")


class TestMain:
    """Tests for the main() entry point."""

    @patch("list_tables.list_tables")
    def test_prints_json_to_stdout(self, mock_list_tables, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["list_tables.py", "my_database"])
        mock_list_tables.return_value = ["table_a", "table_b"]

        main()

        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert output["header"]["source_type"] == "athena"
        assert output["header"]["result_count"] == 2
        names = [row["name"] for row in output["data"]]
        assert names == ["table_a", "table_b"]

    @patch("list_tables.list_tables")
    def test_credential_error_exits_1(self, mock_list_tables, monkeypatch):
        from credential_resolver import CredentialResolutionError

        monkeypatch.setattr(sys, "argv", ["list_tables.py", "my_database"])
        mock_list_tables.side_effect = CredentialResolutionError(
            [{"method": "iam_role", "result": "timeout"}]
        )

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("list_tables.list_tables")
    def test_credential_error_stderr_lists_attempted_methods(self, mock_list_tables, capsys, monkeypatch):
        """Req 3.8: credential error message lists attempted discovery methods."""
        from credential_resolver import CredentialResolutionError

        monkeypatch.setattr(sys, "argv", ["list_tables.py", "my_database"])
        mock_list_tables.side_effect = CredentialResolutionError(
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

    @patch("list_tables.list_tables")
    def test_api_error_exits_1(self, mock_list_tables, capsys, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["list_tables.py", "my_database"])
        mock_list_tables.side_effect = RuntimeError("Glue service unavailable")

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("list_tables.list_tables")
    def test_api_error_stderr_format(self, mock_list_tables, capsys, monkeypatch):
        """Verify API error stderr contains structured JSON error."""
        monkeypatch.setattr(sys, "argv", ["list_tables.py", "my_database"])
        mock_list_tables.side_effect = RuntimeError("Glue service unavailable")

        with pytest.raises(SystemExit):
            main()

        captured = capsys.readouterr()
        err = json.loads(captured.err)
        assert err["error"] == "glue_api_error"
        assert "Glue service unavailable" in err["message"]

    def test_missing_argument_exits_1(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["list_tables.py"])

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
