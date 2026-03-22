"""Preview raw data from a table in AWS Athena.

Uses the Credential Resolver to obtain AWS credentials, validates the
requested database/table against the Access Whitelist, builds a
SELECT * ... LIMIT query, executes it via Athena, and outputs results
as JSON to stdout.

Requirements: 3.4, 3.9
"""

import importlib.util as _ilu
import json
import os
import os as _os
import sys
import time

import boto3

from access_whitelist import AccessWhitelist, WhitelistConfigError
from credential_resolver import CredentialResolutionError, CredentialResolver

try:
    _fqo_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "..", "..", "scripts", "format-query-output.py")
    _fqo_spec = _ilu.spec_from_file_location("format_query_output", _fqo_path)
    _fqo_mod = _ilu.module_from_spec(_fqo_spec)
    _fqo_spec.loader.exec_module(_fqo_mod)
    format_query_output = _fqo_mod.format_query_output
except Exception:
    def format_query_output(source_type, result_set, query_timestamp=None):
        return json.dumps(result_set, indent=2)

DEFAULT_MAX_ROWS = 100
DEFAULT_WHITELIST_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "assets",
    "access-whitelist.json",
)


def preview_data(
    database: str,
    table: str,
    max_rows: int = DEFAULT_MAX_ROWS,
    region: str | None = None,
    whitelist_path: str | None = None,
) -> dict:
    """Preview raw data from a table with a configurable row limit.

    Args:
        database: Name of the Glue database.
        table: Name of the table.
        max_rows: Maximum number of rows to return (default 100).
        region: Optional AWS region. Falls back to AWS_DEFAULT_REGION
            env var or boto3 default.
        whitelist_path: Optional path to the access whitelist config.
            Defaults to the skill's assets directory.

    Returns:
        Dictionary with 'columns' (list of column names) and 'rows'
        (list of row dicts).

    Raises:
        WhitelistConfigError: If the whitelist config is missing or malformed.
        PermissionError: If the database/table is not in the whitelist.
        CredentialResolutionError: If no valid AWS credentials are found.
        Exception: On AWS API errors.
    """
    wl_path = whitelist_path or DEFAULT_WHITELIST_PATH
    whitelist = AccessWhitelist(config_path=wl_path)

    if not whitelist.is_authorized(database, table):
        raise PermissionError(
            f"Access denied: {database}.{table} is not in the access whitelist"
        )

    resolver = CredentialResolver()
    creds = resolver.resolve()

    session = boto3.Session(
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_session_token=creds.token,
        region_name=region,
    )
    athena = session.client("athena")

    sql = f'SELECT * FROM "{database}"."{table}" LIMIT {int(max_rows)}'

    start_resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
    )
    query_execution_id = start_resp["QueryExecutionId"]

    # Poll until the query completes
    while True:
        status_resp = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = status_resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)

    if state != "SUCCEEDED":
        reason = (
            status_resp["QueryExecution"]["Status"]
            .get("StateChangeReason", "Unknown error")
        )
        raise RuntimeError(f"Athena query {state}: {reason}")

    results_resp = athena.get_query_results(
        QueryExecutionId=query_execution_id
    )

    result_set = results_resp["ResultSet"]
    column_info = result_set.get("ResultSetMetadata", {}).get("ColumnInfo", [])
    columns = [col["Name"] for col in column_info]

    raw_rows = result_set.get("Rows", [])
    # First row is the header row in Athena results; skip it
    data_rows = raw_rows[1:] if raw_rows else []

    rows = []
    for row in data_rows:
        values = [datum.get("VarCharValue", "") for datum in row.get("Data", [])]
        rows.append(dict(zip(columns, values)))

    return {"columns": columns, "rows": rows}


def main() -> None:
    """Entry point for standalone invocation."""
    if len(sys.argv) < 3:
        error = {
            "error": "missing_argument",
            "message": "Usage: preview_data.py <database> <table> [max_rows]",
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)

    database = sys.argv[1]
    table = sys.argv[2]
    max_rows = DEFAULT_MAX_ROWS

    if len(sys.argv) >= 4:
        try:
            max_rows = int(sys.argv[3])
        except ValueError:
            error = {
                "error": "invalid_argument",
                "message": f"max_rows must be an integer, got: {sys.argv[3]}",
            }
            print(json.dumps(error), file=sys.stderr)
            sys.exit(1)

    try:
        result = preview_data(database, table, max_rows=max_rows)
        print(format_query_output("athena", result["rows"]))
    except WhitelistConfigError as exc:
        print(json.dumps(exc.to_dict()), file=sys.stderr)
        sys.exit(1)
    except PermissionError as exc:
        error = {
            "error": "access_denied",
            "message": str(exc),
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)
    except CredentialResolutionError as exc:
        print(json.dumps(exc.to_dict()), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        error = {
            "error": "athena_api_error",
            "message": str(exc),
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
