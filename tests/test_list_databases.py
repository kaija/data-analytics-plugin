"""Unit tests for list_databases.py."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, "skills/athena-glue/scripts")

from list_databases import list_databases, main


class TestListDatabases:
    """Tests for the list_databases function."""

    @patch("list_databases.boto3.Session")
    @patch("list_databases.CredentialResolver")
    def test_returns_sorted_database_names(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"DatabaseList": [{"Name": "zebra_db"}, {"Name": "alpha_db"}]},
            {"DatabaseList": [{"Name": "middle_db"}]},
        ]
        client = MagicMock()
        client.get_paginator.return_value = paginator
        mock_session_cls.return_value.client.return_value = client

        result = list_databases()

        assert result == ["alpha_db", "middle_db", "zebra_db"]

    @patch("list_databases.boto3.Session")
    @patch("list_databases.CredentialResolver")
    def test_empty_catalog(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        paginator = MagicMock()
        paginator.paginate.return_value = [{"DatabaseList": []}]
        client = MagicMock()
        client.get_paginator.return_value = paginator
        mock_session_cls.return_value.client.return_value = client

        result = list_databases()

        assert result == []

    @patch("list_databases.boto3.Session")
    @patch("list_databases.CredentialResolver")
    def test_passes_region(self, mock_resolver_cls, mock_session_cls):
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        paginator = MagicMock()
        paginator.paginate.return_value = [{"DatabaseList": []}]
        client = MagicMock()
        client.get_paginator.return_value = paginator
        mock_session_cls.return_value.client.return_value = client

        list_databases(region="us-west-2")

        mock_session_cls.assert_called_once_with(
            aws_access_key_id="ak",
            aws_secret_access_key="sk",
            aws_session_token=None,
            region_name="us-west-2",
        )

    @patch("list_databases.CredentialResolver")
    def test_credential_error_propagates(self, mock_resolver_cls):
        from credential_resolver import CredentialResolutionError

        mock_resolver_cls.return_value.resolve.side_effect = CredentialResolutionError(
            [{"method": "environment_variables", "result": "no credentials found"}]
        )

        with pytest.raises(CredentialResolutionError):
            list_databases()


class TestMain:
    """Tests for the main() entry point."""

    @patch("list_databases.list_databases")
    def test_prints_json_to_stdout(self, mock_list_db, capsys):
        mock_list_db.return_value = ["db_a", "db_b"]

        main()

        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert output["header"]["source_type"] == "athena"
        assert output["header"]["result_count"] == 2
        names = [row["name"] for row in output["data"]]
        assert names == ["db_a", "db_b"]

    @patch("list_databases.list_databases")
    def test_credential_error_exits_1(self, mock_list_db):
        from credential_resolver import CredentialResolutionError

        mock_list_db.side_effect = CredentialResolutionError(
            [{"method": "iam_role", "result": "timeout"}]
        )

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("list_databases.list_databases")
    def test_credential_error_stderr_lists_attempted_methods(self, mock_list_db, capsys):
        """Req 3.8: credential error message lists attempted discovery methods."""
        from credential_resolver import CredentialResolutionError

        mock_list_db.side_effect = CredentialResolutionError(
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
        assert err["attempted_methods"][1]["method"] == "environment_variables"

    @patch("list_databases.list_databases")
    def test_api_error_exits_1(self, mock_list_db, capsys):
        mock_list_db.side_effect = RuntimeError("Glue service unavailable")

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("list_databases.list_databases")
    def test_api_error_stderr_format(self, mock_list_db, capsys):
        """Verify API error stderr contains structured JSON error."""
        mock_list_db.side_effect = RuntimeError("Glue service unavailable")

        with pytest.raises(SystemExit):
            main()

        captured = capsys.readouterr()
        err = json.loads(captured.err)
        assert err["error"] == "glue_api_error"
        assert "Glue service unavailable" in err["message"]
