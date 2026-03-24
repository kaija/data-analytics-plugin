"""Ingest semantic data catalog markdown files into the vector store.

Parses structured semantic catalog documents and stores each table,
job, relationship type, and glossary entry as a separate vector record
with rich metadata for filtered retrieval.

Usage:
    python3 ingest_catalog.py <catalog_dir_or_file> [collection_name]
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CatalogChunk:
    """A parsed chunk from a semantic catalog document."""

    chunk_id: str
    chunk_type: str  # table, job, relationship, glossary, overview, lineage
    content: str  # the text content for embedding
    metadata: dict[str, Any] = field(default_factory=dict)


def _stable_id(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _extract_yaml_block(text: str) -> dict[str, Any] | None:
    """Extract the first YAML code block from text and parse key-value pairs."""
    match = re.search(r"```ya?ml\s*\n(.*?)```", text, re.DOTALL)
    if not match:
        return None
    yaml_text = match.group(1)
    result: dict[str, Any] = {}
    for line in yaml_text.splitlines():
        line = line.strip()
        if ":" in line and not line.startswith("-") and not line.startswith("#"):
            key, _, value = line.partition(":")
            key = key.strip().strip('"')
            value = value.strip().strip('"')
            if value:
                result[key] = value
    return result


def _extract_section(text: str, heading_pattern: str, max_len: int = 3000) -> str | None:
    """Extract content under a heading matching the pattern until the next heading of same or higher level."""
    # Find the heading line first
    heading_match = re.search(
        rf"^(#{{1,4}})\s+.*?{heading_pattern}.*$",
        text,
        re.MULTILINE | re.IGNORECASE,
    )
    if not heading_match or heading_match.group(1) is None:
        return None

    level = len(heading_match.group(1))  # number of # chars
    start = heading_match.start()

    # Find the next heading of same or higher level
    rest = text[heading_match.end():]
    end_pattern = rf"^#{{1,{level}}}\s+"
    end_match = re.search(end_pattern, rest, re.MULTILINE)
    if end_match:
        section = text[start : heading_match.end() + end_match.start()]
    else:
        section = text[start:]

    return section.strip()[:max_len]


def parse_catalog(filepath: str) -> list[CatalogChunk]:
    """Parse a semantic catalog markdown file into chunks."""
    with open(filepath, "r") as f:
        content = f.read()

    chunks: list[CatalogChunk] = []
    filename = os.path.basename(filepath)

    # Extract repository and domain from header
    repo_match = re.search(r"\*\*Repository\*\*[:\s]+(\S+)", content)
    domain_match = re.search(r"\*\*Domain\*\*[:\s]+(.+?)(?:\n|$)", content)
    repository = repo_match.group(1) if repo_match else filename
    domain = domain_match.group(1).strip() if domain_match else ""

    base_metadata = {
        "repository": repository,
        "domain": domain,
        "source_file": filename,
    }

    # --- Parse Overview section ---
    overview = _extract_section(content, r"Overview")
    if overview:
        chunks.append(CatalogChunk(
            chunk_id=f"{repository}::overview",
            chunk_type="overview",
            content=overview[:2000],
            metadata={**base_metadata, "section": "overview"},
        ))

    # --- Parse Table sections ---
    # Match table blocks by looking for table_id in yaml blocks
    table_sections = re.split(r"(?=###\s+(?:<a\s+id=|Table:\s|table-))", content)
    for section in table_sections:
        yaml_data = _extract_yaml_block(section)
        if not yaml_data or "table_id" not in yaml_data:
            continue

        table_id = yaml_data["table_id"]

        # Extract schema table if present
        schema_match = re.search(
            r"\|\s*column_name.*?\n\|[-\s|]+\n((?:\|.*\n)*)",
            section,
            re.IGNORECASE,
        )
        schema_text = schema_match.group(0).strip() if schema_match else ""

        # Build a concise text representation for embedding
        description = yaml_data.get("description", "")
        zone = yaml_data.get("zone", "")
        table_type = yaml_data.get("table_type", yaml_data.get("type", ""))

        text_parts = [
            f"Table: {table_id}",
            f"Description: {description}",
            f"Zone: {zone}, Type: {table_type}",
        ]

        # Add primary keys
        pk_match = re.search(r"primary_keys:\s*\[([^\]]+)\]", section)
        if pk_match:
            text_parts.append(f"Primary keys: {pk_match.group(1)}")

        # Add key behaviors
        behavior_section = _extract_section(section, r"Key Behaviors")
        if behavior_section:
            text_parts.append(behavior_section[:500])

        # Add schema summary
        if schema_text:
            text_parts.append(f"Schema:\n{schema_text[:1000]}")

        # Add relationship semantics if present
        rel_section = _extract_section(section, r"Relationship Semantics|Entity Type")
        if rel_section:
            text_parts.append(rel_section[:500])

        chunk_text = "\n".join(text_parts)

        metadata = {
            **base_metadata,
            "section": "table",
            "table_id": table_id,
            "zone": zone,
            "table_type": table_type,
            "description": description[:200],
        }

        # Extract consumers
        consumers_match = re.search(r"consumers:\s*\[([^\]]+)\]", section)
        if consumers_match:
            metadata["consumers"] = consumers_match.group(1).strip()

        # Extract primary keys
        if pk_match:
            metadata["primary_keys"] = pk_match.group(1).strip()

        # Extract upstream tables
        upstream_match = re.search(r"source_iceberg_tables:\s*\[([^\]]*)\]", section)
        if upstream_match and upstream_match.group(1).strip():
            metadata["upstream_tables"] = upstream_match.group(1).strip()

        chunks.append(CatalogChunk(
            chunk_id=f"{repository}::table::{table_id}",
            chunk_type="table",
            content=chunk_text,
            metadata=metadata,
        ))

    # --- Parse Job sections ---
    job_sections = re.split(r"(?=###\s+(?:<a\s+id=\"job-|Job:\s|job-))", content)
    for section in job_sections:
        yaml_data = _extract_yaml_block(section)
        if not yaml_data or "job_id" not in yaml_data:
            continue

        job_id = yaml_data["job_id"]

        text_parts = [f"Job: {job_id}"]

        # Add class and code pointer
        if "class_name" in yaml_data:
            text_parts.append(f"Class: {yaml_data['class_name']}")

        # Add inputs/outputs from yaml
        inputs_match = re.findall(r"dataset:\s*\"?([^\"}\n]+)\"?", section)
        if inputs_match:
            text_parts.append(f"Datasets: {', '.join(inputs_match)}")

        # Add transformation steps
        transform_matches = re.findall(r'step:\s*"([^"]+)".*?logic:\s*"([^"]+)"', section)
        if transform_matches:
            text_parts.append("Transformations:")
            for step, logic in transform_matches:
                text_parts.append(f"  - {step}: {logic}")

        # Add write semantics
        write_section = _extract_section(section, r"Write Semantics|Write behavior")
        if write_section:
            text_parts.append(write_section[:400])

        chunk_text = "\n".join(text_parts)

        # Extract input/output datasets
        input_match = re.search(r"inputs:\s*\n\s+- dataset:\s*\"?([^\"}\n]+)", section)
        output_datasets = re.findall(r"outputs:\s*\n(?:\s+- dataset:\s*\"?([^\"}\n]+)\"?\s*\n?)+", section)

        metadata = {
            **base_metadata,
            "section": "job",
            "job_id": job_id,
            "class_name": yaml_data.get("class_name", ""),
        }

        # Extract schedule
        schedule_match = re.search(r"schedule:\s*\"?([^\"}\n]+)", section)
        if schedule_match:
            metadata["schedule"] = schedule_match.group(1).strip()

        if input_match:
            metadata["input_dataset"] = input_match.group(1).strip()

        chunks.append(CatalogChunk(
            chunk_id=f"{repository}::job::{job_id}",
            chunk_type="job",
            content=chunk_text,
            metadata=metadata,
        ))

    # --- Parse Glossary / Relationship Types ---
    glossary_section = _extract_section(content, r"Domain Glossary|Glossary")
    if glossary_section:
        # Split into entity types and relationship types
        entity_block = _extract_section(glossary_section, r"Entity Types")
        if entity_block:
            chunks.append(CatalogChunk(
                chunk_id=f"{repository}::glossary::entity_types",
                chunk_type="glossary",
                content=entity_block[:1500],
                metadata={**base_metadata, "section": "glossary", "topic": "entity_types"},
            ))

        rel_block = _extract_section(glossary_section, r"Relationship (?:Types|Semantics)")
        if rel_block:
            chunks.append(CatalogChunk(
                chunk_id=f"{repository}::glossary::relationship_types",
                chunk_type="glossary",
                content=rel_block[:1500],
                metadata={**base_metadata, "section": "glossary", "topic": "relationship_types"},
            ))

        id_block = _extract_section(glossary_section, r"Key Identifiers")
        if id_block:
            chunks.append(CatalogChunk(
                chunk_id=f"{repository}::glossary::identifiers",
                chunk_type="glossary",
                content=id_block[:1000],
                metadata={**base_metadata, "section": "glossary", "topic": "identifiers"},
            ))

    # --- Parse Lineage section ---
    lineage_section = _extract_section(content, r"Lineage")
    if lineage_section:
        chunks.append(CatalogChunk(
            chunk_id=f"{repository}::lineage",
            chunk_type="lineage",
            content=lineage_section[:2000],
            metadata={**base_metadata, "section": "lineage"},
        ))

    return chunks


def ingest_to_chromadb(
    chunks: list[CatalogChunk],
    collection_name: str = "semantic_catalog",
    persist_directory: str | None = None,
) -> dict:
    """Ingest parsed catalog chunks into ChromaDB.

    Args:
        chunks: List of CatalogChunk objects to ingest.
        collection_name: ChromaDB collection name.
        persist_directory: Path for persistent storage. If None, uses
            the default from vector-config.json.

    Returns:
        Summary dict with counts.
    """
    import chromadb  # type: ignore[import-untyped]

    if persist_directory is None:
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "assets", "vector-config.json",
        )
        with open(config_path) as f:
            config = json.load(f)
        persist_directory = config.get("connection", {}).get("persist_directory", "")

    if persist_directory:
        os.makedirs(persist_directory, exist_ok=True)
        client = chromadb.PersistentClient(path=persist_directory)
    else:
        client = chromadb.Client()

    # Get or create collection — ChromaDB uses its default embedding function
    # (all-MiniLM-L6-v2) which auto-embeds document strings.
    collection = client.get_or_create_collection(
        name=collection_name,
        metadata={"description": "Semantic data catalog for asset graph pipelines"},
    )

    # Prepare batch data
    ids: list[str] = []
    documents: list[str] = []
    metadatas: list[dict] = []

    for chunk in chunks:
        ids.append(chunk.chunk_id)
        documents.append(chunk.content)
        meta = dict(chunk.metadata)
        meta["chunk_type"] = chunk.chunk_type
        # ChromaDB metadata values must be str, int, float, or bool
        for k, v in list(meta.items()):
            if v is None:
                meta[k] = ""
            elif not isinstance(v, (str, int, float, bool)):
                meta[k] = str(v)
        metadatas.append(meta)

    # Upsert in batches (ChromaDB limit is ~5000 per call)
    batch_size = 100
    for i in range(0, len(ids), batch_size):
        collection.upsert(
            ids=ids[i : i + batch_size],
            documents=documents[i : i + batch_size],
            metadatas=metadatas[i : i + batch_size],
        )

    # Count by type
    type_counts: dict[str, int] = {}
    for chunk in chunks:
        type_counts[chunk.chunk_type] = type_counts.get(chunk.chunk_type, 0) + 1

    return {
        "collection": collection_name,
        "total_chunks": len(chunks),
        "type_counts": type_counts,
        "persist_directory": persist_directory or "(in-memory)",
    }


def main() -> None:
    if len(sys.argv) < 2:
        print(
            json.dumps({
                "error": "missing_argument",
                "message": "Usage: ingest_catalog.py <catalog_dir_or_file> [collection_name]",
            }),
            file=sys.stderr,
        )
        sys.exit(1)

    path = sys.argv[1]
    collection_name = sys.argv[2] if len(sys.argv) >= 3 else "semantic_catalog"

    # Collect markdown files
    if os.path.isdir(path):
        files = sorted(
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.endswith(".md")
        )
    elif os.path.isfile(path):
        files = [path]
    else:
        print(
            json.dumps({"error": "path_not_found", "message": f"Path not found: {path}"}),
            file=sys.stderr,
        )
        sys.exit(1)

    all_chunks: list[CatalogChunk] = []
    for filepath in files:
        chunks = parse_catalog(filepath)
        all_chunks.extend(chunks)
        print(f"Parsed {filepath}: {len(chunks)} chunks", file=sys.stderr)

    if not all_chunks:
        print(
            json.dumps({"error": "no_chunks", "message": "No catalog chunks found in the provided files."}),
            file=sys.stderr,
        )
        sys.exit(1)

    result = ingest_to_chromadb(all_chunks, collection_name=collection_name)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
