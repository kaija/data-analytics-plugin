"""Execute an arbitrary SQL query against AWS Athena.

Uses the Credential Resolver to obtain AWS credentials, validates the
SQL query against the Access Whitelist via validate_query(), executes
the query via Athena, and returns the result set with data_scanned_bytes
and execution_time_ms metadata.

Requirements: 3.5, 3.6, 3.9
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

DEFAULT_WHITELIST_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "assets",
    "access-whitelist.json",
)


def execute_query(
    sql: str,
    region: str | None = None,
    whitelist_path: str | None = None,
) -> dict:
    """Execute an arbitrary SQL query against AWS Athena.

    Args:
        sql: The SQL query string.
        region: Optional AWS region. Falls back to AWS_DEFAULT_REGION
            env var or boto3 default.
        whitelist_path: Optional path to the access whitelist config.
            Defaults to the skill's assets directory.

    Returns:
        Dictionary with 'columns' (list of column names), 'rows'
        (list of row dicts), 'data_scanned_bytes' (int), and
        'execution_time_ms' (int).

    Raises:
        WhitelistConfigError: If the whitelist config is missing or malformed.
        PermissionError: If the query references unauthorized resources.
        CredentialResolutionError: If no valid AWS credentials are found.
        RuntimeError: On Athena query failure.
    """
    wl_path = whitelist_path or DEFAULT_WHITELIST_PATH
    whitelist = AccessWhitelist(config_path=wl_path)

    validation = whitelist.validate_query(sql)
    if not validation.authorized:
        unauthorized = ", ".join(
            f"{r.database}.{r.table}" if r.table else r.database
            for r in validation.unauthorized_resources
        )
        raise PermissionError(
            f"Access denied: query references unauthorized resources: {unauthorized}"
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

    start_kwargs: dict = {"QueryString": sql}

    # Resolve Athena output location: env var > workgroup default.
    # Without an output location or a workgroup that has one configured,
    # Athena will reject the query with InvalidRequestException.
    output_location = os.environ.get("ATHENA_OUTPUT_LOCATION")
    workgroup = os.environ.get("ATHENA_WORKGROUP")
    if output_location:
        start_kwargs["ResultConfiguration"] = {
            "OutputLocation": output_location,
        }
    if workgroup:
        start_kwargs["WorkGroup"] = workgroup

    start_resp = athena.start_query_execution(**start_kwargs)
    query_execution_id = start_resp["QueryExecutionId"]

    # Poll until the query completes
    while True:
        status_resp = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        query_execution = status_resp["QueryExecution"]
        state = query_execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)

    if state != "SUCCEEDED":
        reason = (
            query_execution["Status"]
            .get("StateChangeReason", "Unknown error")
        )
        raise RuntimeError(f"Athena query {state}: {reason}")

    # Extract execution statistics
    statistics = query_execution.get("Statistics", {})
    data_scanned_bytes = statistics.get("DataScannedInBytes", 0)
    execution_time_ms = statistics.get("TotalExecutionTimeInMillis", 0)

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

    return {
        "columns": columns,
        "rows": rows,
        "data_scanned_bytes": data_scanned_bytes,
        "execution_time_ms": execution_time_ms,
    }


def main() -> None:
    """Entry point for standalone invocation."""
    if len(sys.argv) < 2:
        error = {
            "error": "missing_argument",
            "message": "Usage: execute_query.py <sql>",
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)

    sql = sys.argv[1]

    try:
        result = execute_query(sql)
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
