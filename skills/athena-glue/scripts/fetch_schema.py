"""Fetch the full schema of a table in the AWS Glue Data Catalog.

Uses the Credential Resolver to obtain AWS credentials, creates a Glue
client, and calls get_table to retrieve column names, data types, and
partition keys. Outputs schema as JSON to stdout.

Requirements: 3.3
"""

import importlib.util as _ilu
import json
import os as _os
import sys

import boto3

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


def fetch_schema(
    database: str, table: str, region: str | None = None
) -> dict:
    """Fetch the full schema of a table including columns and partition keys.

    Args:
        database: Name of the Glue database.
        table: Name of the table.
        region: Optional AWS region. Falls back to AWS_DEFAULT_REGION
            env var or boto3 default.

    Returns:
        Dictionary with 'columns' and 'partition_keys' lists, each
        containing dicts with 'name' and 'type' keys.

    Raises:
        CredentialResolutionError: If no valid AWS credentials are found.
        Exception: On AWS API errors.
    """
    resolver = CredentialResolver()
    creds = resolver.resolve()

    session = boto3.Session(
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_session_token=creds.token,
        region_name=region,
    )
    client = session.client("glue")

    response = client.get_table(DatabaseName=database, Name=table)
    table_data = response["Table"]

    columns = [
        {"name": col["Name"], "type": col["Type"]}
        for col in table_data.get("StorageDescriptor", {}).get("Columns", [])
    ]

    partition_keys = [
        {"name": pk["Name"], "type": pk["Type"]}
        for pk in table_data.get("PartitionKeys", [])
    ]

    return {"columns": columns, "partition_keys": partition_keys}


def main() -> None:
    """Entry point for standalone invocation."""
    if len(sys.argv) < 3:
        error = {
            "error": "missing_argument",
            "message": "Usage: fetch_schema.py <database> <table>",
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)

    database = sys.argv[1]
    table = sys.argv[2]

    try:
        schema = fetch_schema(database, table)
        result_set = schema["columns"]
        print(format_query_output("athena", result_set))
    except CredentialResolutionError as exc:
        print(json.dumps(exc.to_dict()), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        error = {
            "error": "glue_api_error",
            "message": str(exc),
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
