"""List all tables in a specified AWS Glue Data Catalog database.

Uses the Credential Resolver to obtain AWS credentials, creates a Glue
client, and paginates through get_tables results. Outputs sorted table
names as JSON to stdout.

Requirements: 3.2
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


def list_tables(database: str, region: str | None = None) -> list[str]:
    """List all tables in a specified Glue Data Catalog database.

    Args:
        database: Name of the Glue database.
        region: Optional AWS region. Falls back to AWS_DEFAULT_REGION
            env var or boto3 default.

    Returns:
        Sorted list of table names.

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

    tables: list[str] = []
    paginator = client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        for table in page.get("TableList", []):
            tables.append(table["Name"])

    tables.sort()
    return tables


def main() -> None:
    """Entry point for standalone invocation."""
    if len(sys.argv) < 2:
        error = {
            "error": "missing_argument",
            "message": "Usage: list_tables.py <database>",
        }
        print(json.dumps(error), file=sys.stderr)
        sys.exit(1)

    database = sys.argv[1]

    try:
        tables = list_tables(database)
        result_set = [{"name": t} for t in tables]
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
