"""
Generic Query Script — format-query-output.py

Transforms raw result sets from Athena, Vector, Graph, and Catalog sources
into a consistent LLM-Friendly JSON format.
"""

import argparse
import json
import sys
from datetime import datetime, timezone


class FormatError(Exception):
    def __init__(self, source_type, reason):
        self.source_type = source_type
        self.reason = reason
        super().__init__(f"FormatError for source '{source_type}': {reason}")


SUPPORTED_SOURCE_TYPES = {"athena", "vector", "graph", "catalog"}

VECTOR_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "score", "type": "float"},
    {"name": "metadata", "type": "object"},
    {"name": "payload", "type": "array"},
]

GRAPH_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "labels", "type": "string"},
    {"name": "properties", "type": "object"},
]


def _infer_type(value) -> str:
    """Infer a type string from a Python value."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    return "unknown"


def _now_iso8601() -> str:
    """Return current UTC time as ISO 8601 string with Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_athena(result_set, source_type: str):
    """Format Athena result set (list of dicts)."""
    if not isinstance(result_set, list):
        raise FormatError(source_type, "expected a list of row dicts")

    if len(result_set) == 0:
        return [], []

    first = result_set[0]
    if not isinstance(first, dict):
        raise FormatError(source_type, "each row must be a dict")

    fields = [{"name": k, "type": _infer_type(v)} for k, v in first.items()]
    data = []
    for i, row in enumerate(result_set):
        if not isinstance(row, dict):
            raise FormatError(source_type, f"row {i} is not a dict")
        data.append(dict(row))

    return fields, data


def _format_vector(result_set, source_type: str):
    """Format Vector result set (list of VectorResult-like dicts)."""
    if not isinstance(result_set, list):
        raise FormatError(source_type, "expected a list of vector result dicts")

    data = []
    for i, item in enumerate(result_set):
        if not isinstance(item, dict):
            raise FormatError(source_type, f"item {i} is not a dict")
        row = {
            "id": item.get("id"),
            "score": item.get("score"),
            "metadata": item.get("metadata"),
            "payload": item.get("payload"),
        }
        data.append(row)

    return VECTOR_SCHEMA, data


def _format_graph(result_set, source_type: str):
    """Format Graph result set (dict with nodes and relationships)."""
    if not isinstance(result_set, dict):
        raise FormatError(source_type, "expected a dict with 'nodes' and 'relationships' keys")

    nodes = result_set.get("nodes")
    if nodes is None:
        raise FormatError(source_type, "missing 'nodes' key in graph result")
    if not isinstance(nodes, list):
        raise FormatError(source_type, "'nodes' must be a list")

    data = []
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise FormatError(source_type, f"node {i} is not a dict")
        labels = node.get("labels", [])
        if isinstance(labels, list):
            labels_str = ", ".join(str(l) for l in labels)
        else:
            labels_str = str(labels)
        properties = node.get("properties", {})
        row = {
            "id": node.get("id"),
            "labels": labels_str,
            "properties": properties,
        }
        data.append(row)

    return GRAPH_SCHEMA, data


def _format_catalog(result_set, source_type: str):
    """Format Catalog result set (list of asset or relationship dicts)."""
    # Same logic as athena — infer schema from first row
    return _format_athena(result_set, source_type)


def format_query_output(source_type: str, result_set, query_timestamp: str = None) -> str:
    """Transform a raw result set into LLM-Friendly JSON format.

    Args:
        source_type: One of "athena", "vector", "graph", "catalog".
        result_set: The raw result from the skill.
        query_timestamp: ISO 8601 timestamp (auto-generated if not provided).

    Returns:
        JSON string in LLM-Friendly Format.

    Raises:
        FormatError if source_type is unknown or result_set cannot be parsed.
    """
    if source_type not in SUPPORTED_SOURCE_TYPES:
        raise FormatError(
            source_type,
            f"unknown source type '{source_type}'; supported: {sorted(SUPPORTED_SOURCE_TYPES)}",
        )

    if query_timestamp is None:
        query_timestamp = _now_iso8601()

    try:
        if source_type == "athena":
            fields, data = _format_athena(result_set, source_type)
        elif source_type == "vector":
            fields, data = _format_vector(result_set, source_type)
        elif source_type == "graph":
            fields, data = _format_graph(result_set, source_type)
        elif source_type == "catalog":
            fields, data = _format_catalog(result_set, source_type)
    except FormatError:
        raise
    except Exception as exc:
        raise FormatError(source_type, f"unexpected parse failure: {exc}") from exc

    output = {
        "header": {
            "source_type": source_type,
            "query_timestamp": query_timestamp,
            "result_count": len(data),
        },
        "schema": {"fields": fields},
        "data": data,
    }

    return json.dumps(output, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Format query output from data skills into LLM-Friendly JSON."
    )
    parser.add_argument(
        "--source-type",
        required=True,
        choices=sorted(SUPPORTED_SOURCE_TYPES),
        help="Source type: athena, vector, graph, or catalog",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--input",
        help="JSON string of the result set",
    )
    group.add_argument(
        "--input-file",
        help="Path to a JSON file containing the result set",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="ISO 8601 query timestamp (auto-generated if omitted)",
    )

    args = parser.parse_args()

    if args.input:
        try:
            result_set = json.loads(args.input)
        except json.JSONDecodeError as exc:
            print(f"Error: could not parse --input as JSON: {exc}", file=sys.stderr)
            sys.exit(1)
    else:
        try:
            with open(args.input_file, "r", encoding="utf-8") as f:
                result_set = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Error: could not read --input-file '{args.input_file}': {exc}", file=sys.stderr)
            sys.exit(1)

    try:
        output = format_query_output(args.source_type, result_set, args.timestamp)
        print(output)
    except FormatError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
