"""Property tests for Athena tool scripts.

Property 6: Fetch schema returns complete column metadata
**Validates: Requirements 3.3**

Property 7: Preview data respects row limit
**Validates: Requirements 3.4**

Property 8: Query execution metadata is always present
**Validates: Requirements 3.6**
"""

import os
import sys
from unittest.mock import MagicMock, patch

from hypothesis import given, settings, strategies as st

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "..", "skills", "athena-glue", "scripts"
    ),
)

from fetch_schema import fetch_schema
from preview_data import preview_data
from execute_query import execute_query

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Glue column type names (representative subset)
glue_types = st.sampled_from(
    [
        "string",
        "int",
        "bigint",
        "double",
        "float",
        "boolean",
        "timestamp",
        "date",
        "binary",
        "decimal(10,2)",
        "array<string>",
        "map<string,int>",
        "struct<a:int,b:string>",
    ]
)

# Simple identifier-like column names
column_name = st.from_regex(r"[a-z][a-z0-9_]{0,29}", fullmatch=True)

# A single Glue column entry: {"Name": ..., "Type": ...}
glue_column = st.fixed_dictionaries(
    {"Name": column_name, "Type": glue_types}
)

# Lists of columns (0-20) and partition keys (0-5)
glue_columns_list = st.lists(glue_column, min_size=0, max_size=20)
glue_partition_keys_list = st.lists(glue_column, min_size=0, max_size=5)

# max_rows for preview_data (1-1000)
max_rows_strategy = st.integers(min_value=1, max_value=1000)

# Number of data rows Athena might return (0-200)
num_data_rows = st.integers(min_value=0, max_value=200)

# Non-negative integers for Athena statistics
non_neg_int = st.integers(min_value=0, max_value=10**12)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_glue_response(columns, partition_keys):
    """Build a mock Glue get_table response."""
    return {
        "Table": {
            "StorageDescriptor": {"Columns": columns},
            "PartitionKeys": partition_keys,
        }
    }


def _build_athena_results(num_rows, num_cols):
    """Build mock Athena query results with the given dimensions."""
    col_names = [f"col_{i}" for i in range(num_cols)]
    col_info = [{"Name": c} for c in col_names]

    # Header row + data rows
    header_row = {"Data": [{"VarCharValue": c} for c in col_names]}
    data_rows = [
        {"Data": [{"VarCharValue": f"v{r}_{c}"} for c in range(num_cols)]}
        for r in range(num_rows)
    ]

    return {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": col_info},
            "Rows": [header_row] + data_rows,
        }
    }


# ---------------------------------------------------------------------------
# Property 6: Fetch schema returns complete column metadata
# ---------------------------------------------------------------------------

class TestFetchSchemaCompleteMetadata:
    """Property 6: For any valid database/table pair, fetch_schema should
    return a result containing column names, data types, and partition keys
    for every column in the table.

    **Validates: Requirements 3.3**
    """

    @given(columns=glue_columns_list, partition_keys=glue_partition_keys_list)
    @settings(max_examples=100)
    @patch("fetch_schema.boto3.Session")
    @patch("fetch_schema.CredentialResolver")
    def test_all_columns_and_partition_keys_present(
        self, mock_resolver_cls, mock_session_cls, columns, partition_keys
    ):
        """fetch_schema must return every column name/type and every
        partition key name/type from the Glue response."""
        # Arrange mocks
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        client = MagicMock()
        client.get_table.return_value = _build_glue_response(columns, partition_keys)
        mock_session_cls.return_value.client.return_value = client

        # Act
        result = fetch_schema("test_db", "test_table")

        # Assert structure
        assert "columns" in result, "Result must contain 'columns' key"
        assert "partition_keys" in result, "Result must contain 'partition_keys' key"

        # Assert column count matches
        assert len(result["columns"]) == len(columns), (
            f"Expected {len(columns)} columns, got {len(result['columns'])}"
        )
        assert len(result["partition_keys"]) == len(partition_keys), (
            f"Expected {len(partition_keys)} partition keys, "
            f"got {len(result['partition_keys'])}"
        )

        # Assert every column has name and type matching input
        for i, col in enumerate(result["columns"]):
            assert "name" in col, f"Column {i} missing 'name'"
            assert "type" in col, f"Column {i} missing 'type'"
            assert col["name"] == columns[i]["Name"]
            assert col["type"] == columns[i]["Type"]

        # Assert every partition key has name and type matching input
        for i, pk in enumerate(result["partition_keys"]):
            assert "name" in pk, f"Partition key {i} missing 'name'"
            assert "type" in pk, f"Partition key {i} missing 'type'"
            assert pk["name"] == partition_keys[i]["Name"]
            assert pk["type"] == partition_keys[i]["Type"]


# ---------------------------------------------------------------------------
# Property 7: Preview data respects row limit
# ---------------------------------------------------------------------------

class TestPreviewDataRowLimit:
    """Property 7: For any table and any max_rows parameter value,
    preview_data should return at most max_rows rows.

    **Validates: Requirements 3.4**
    """

    @given(max_rows=max_rows_strategy, available_rows=num_data_rows)
    @settings(max_examples=100)
    @patch("preview_data.boto3.Session")
    @patch("preview_data.CredentialResolver")
    @patch("preview_data.AccessWhitelist")
    def test_row_count_never_exceeds_max_rows(
        self,
        mock_wl_cls,
        mock_resolver_cls,
        mock_session_cls,
        max_rows,
        available_rows,
    ):
        """The number of rows returned must never exceed max_rows.
        Athena enforces the LIMIT clause, so the mock returns
        min(available_rows, max_rows) rows — matching real behaviour."""
        # Arrange mocks
        mock_wl_cls.return_value.is_authorized.return_value = True
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        num_cols = 3
        # Athena respects the SQL LIMIT clause, so it returns at most max_rows
        returned_rows = min(available_rows, max_rows)
        results_resp = _build_athena_results(returned_rows, num_cols)

        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        athena.get_query_results.return_value = results_resp
        mock_session_cls.return_value.client.return_value = athena

        # Act
        result = preview_data(
            "test_db", "test_table", max_rows=max_rows, whitelist_path="dummy"
        )

        # Assert
        assert "rows" in result, "Result must contain 'rows' key"
        assert len(result["rows"]) <= max_rows, (
            f"Got {len(result['rows'])} rows but max_rows={max_rows}"
        )

    @given(max_rows=max_rows_strategy)
    @settings(max_examples=100)
    @patch("preview_data.boto3.Session")
    @patch("preview_data.CredentialResolver")
    @patch("preview_data.AccessWhitelist")
    def test_sql_limit_clause_matches_max_rows(
        self,
        mock_wl_cls,
        mock_resolver_cls,
        mock_session_cls,
        max_rows,
    ):
        """The SQL query sent to Athena must contain a LIMIT clause
        equal to max_rows."""
        mock_wl_cls.return_value.is_authorized.return_value = True
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid"}
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

        preview_data(
            "test_db", "test_table", max_rows=max_rows, whitelist_path="dummy"
        )

        call_args = athena.start_query_execution.call_args
        sql = call_args[1]["QueryString"]
        assert f"LIMIT {max_rows}" in sql, (
            f"SQL should contain 'LIMIT {max_rows}', got: {sql}"
        )


# ---------------------------------------------------------------------------
# Property 8: Query execution metadata is always present
# ---------------------------------------------------------------------------

class TestExecuteQueryMetadata:
    """Property 8: For any SQL query executed via execute_query, the result
    should include data_scanned_bytes (non-negative integer) and
    execution_time_ms (non-negative integer) metadata fields.

    **Validates: Requirements 3.6**
    """

    @given(
        scanned_bytes=non_neg_int,
        exec_time_ms=non_neg_int,
    )
    @settings(max_examples=100)
    @patch("execute_query.boto3.Session")
    @patch("execute_query.CredentialResolver")
    @patch("execute_query.AccessWhitelist")
    def test_metadata_fields_present_and_non_negative(
        self,
        mock_wl_cls,
        mock_resolver_cls,
        mock_session_cls,
        scanned_bytes,
        exec_time_ms,
    ):
        """data_scanned_bytes and execution_time_ms must always be present
        as non-negative integers in the result."""
        # Arrange mocks
        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(
            authorized=True, unauthorized_resources=[]
        )
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": scanned_bytes,
                    "TotalExecutionTimeInMillis": exec_time_ms,
                },
            }
        }
        athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": []},
                "Rows": [],
            }
        }
        mock_session_cls.return_value.client.return_value = athena

        # Act
        result = execute_query("SELECT 1", whitelist_path="dummy")

        # Assert presence
        assert "data_scanned_bytes" in result, (
            "Result must contain 'data_scanned_bytes'"
        )
        assert "execution_time_ms" in result, (
            "Result must contain 'execution_time_ms'"
        )

        # Assert type
        assert isinstance(result["data_scanned_bytes"], int), (
            f"data_scanned_bytes must be int, got {type(result['data_scanned_bytes'])}"
        )
        assert isinstance(result["execution_time_ms"], int), (
            f"execution_time_ms must be int, got {type(result['execution_time_ms'])}"
        )

        # Assert non-negative
        assert result["data_scanned_bytes"] >= 0, (
            f"data_scanned_bytes must be >= 0, got {result['data_scanned_bytes']}"
        )
        assert result["execution_time_ms"] >= 0, (
            f"execution_time_ms must be >= 0, got {result['execution_time_ms']}"
        )

        # Assert values match what Athena reported
        assert result["data_scanned_bytes"] == scanned_bytes
        assert result["execution_time_ms"] == exec_time_ms

    @given(
        scanned_bytes=non_neg_int,
        exec_time_ms=non_neg_int,
        num_rows=st.integers(min_value=0, max_value=50),
    )
    @settings(max_examples=100)
    @patch("execute_query.boto3.Session")
    @patch("execute_query.CredentialResolver")
    @patch("execute_query.AccessWhitelist")
    def test_metadata_present_regardless_of_result_size(
        self,
        mock_wl_cls,
        mock_resolver_cls,
        mock_session_cls,
        scanned_bytes,
        exec_time_ms,
        num_rows,
    ):
        """Metadata must be present whether the query returns 0 rows or many."""
        mock_wl = mock_wl_cls.return_value
        mock_wl.validate_query.return_value = MagicMock(
            authorized=True, unauthorized_resources=[]
        )
        creds = MagicMock(access_key="ak", secret_key="sk", token=None)
        mock_resolver_cls.return_value.resolve.return_value = creds

        num_cols = 2
        results_resp = _build_athena_results(num_rows, num_cols)

        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": scanned_bytes,
                    "TotalExecutionTimeInMillis": exec_time_ms,
                },
            }
        }
        athena.get_query_results.return_value = results_resp
        mock_session_cls.return_value.client.return_value = athena

        result = execute_query("SELECT * FROM db.tbl", whitelist_path="dummy")

        # Metadata always present and non-negative
        assert "data_scanned_bytes" in result
        assert "execution_time_ms" in result
        assert result["data_scanned_bytes"] >= 0
        assert result["execution_time_ms"] >= 0
