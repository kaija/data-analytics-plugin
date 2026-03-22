# Data Analytics Skill Suite — SKILLS.md

A multi-agent plugin for querying, exploring, and correlating data across AWS Athena/Glue, vector stores, graph databases, and a cross-paradigm catalog mapper. Each skill runs as an isolated sub-agent; an orchestrator routes requests to the right skill automatically.

---

## Table of Contents

1. [Overview](#overview)
2. [Athena/Glue Skill](#athenaglueskill)
3. [Vector DB Skill](#vector-db-skill)
4. [Graph DB Skill](#graph-db-skill)
5. [Catalog Mapper Skill](#catalog-mapper-skill)
6. [Generic Query Script](#generic-query-script)
7. [Query Best Practices](#query-best-practices)
8. [Access Whitelist Configuration](#access-whitelist-configuration)
9. [Credential Resolver](#credential-resolver)

---

## Overview

| Skill | Paradigm | Config file |
|---|---|---|
| `athena-glue` | Structured SQL (AWS Athena + Glue) | `skills/athena-glue/assets/access-whitelist.json` |
| `vector-db` | Vector similarity search | `skills/vector-db/assets/vector-config.json` |
| `graph-db` | Graph traversal (Neo4j / Neptune) | `skills/graph-db/assets/graph-config.json` |
| `catalog-mapper` | Cross-paradigm lineage & relationships | `skills/catalog-mapper/assets/catalog-map.json` |

The orchestrator agent (`agents/orchestrator.md`) inspects each incoming request and delegates to the appropriate skill. A single request can span multiple skills — the orchestrator aggregates the results before replying.

---

## Athena/Glue Skill

**Skill directory:** `skills/athena-glue/`

### Tools

| Script | Purpose | Signature |
|---|---|---|
| `list_databases.py` | List all Glue Data Catalog databases | `python skills/athena-glue/scripts/list_databases.py` |
| `list_tables.py` | List tables in a database | `python skills/athena-glue/scripts/list_tables.py <database>` |
| `fetch_schema.py` | Fetch column names, types, partition keys | `python skills/athena-glue/scripts/fetch_schema.py <database> <table>` |
| `preview_data.py` | Preview up to N rows (default 100) | `python skills/athena-glue/scripts/preview_data.py <database> <table> [max_rows]` |
| `execute_query.py` | Run arbitrary SQL; reports bytes scanned and execution time | `python skills/athena-glue/scripts/execute_query.py "<sql>"` |

### Configuration

All queries are validated against the access whitelist before execution. See [Access Whitelist Configuration](#access-whitelist-configuration) for the format.

AWS credentials are resolved automatically. See [Credential Resolver](#credential-resolver) for the discovery order.

### Usage Examples

```bash
# List all databases
python skills/athena-glue/scripts/list_databases.py

# List tables in the analytics_prod database
python skills/athena-glue/scripts/list_tables.py analytics_prod

# Fetch schema for a table
python skills/athena-glue/scripts/fetch_schema.py analytics_prod events

# Preview 10 rows
python skills/athena-glue/scripts/preview_data.py analytics_prod events 10

# Execute a SQL query
python skills/athena-glue/scripts/execute_query.py \
  "SELECT user_id, COUNT(*) AS cnt FROM analytics_prod.events GROUP BY user_id LIMIT 100"
```

`execute_query.py` prints the result set followed by a summary line:

```
Scanned: 2.3 MB  |  Execution time: 1.42 s
```

---

## Vector DB Skill

**Skill directory:** `skills/vector-db/`

### Supported Backends

Pinecone, Weaviate, Qdrant, ChromaDB, Milvus, pgvector.

### Tools

| Script | Purpose | Signature |
|---|---|---|
| `list_collections.py` | List available collections / indices | `python skills/vector-db/scripts/list_collections.py` |
| `vector_search.py` | Similarity search by embedding | `python skills/vector-db/scripts/vector_search.py <collection> '<embedding_json>' [top_k]` |
| `metadata_filter.py` | Filter vectors by metadata key-value pairs | `python skills/vector-db/scripts/metadata_filter.py <collection> '<filters_json>'` |
| `retrieve_by_id.py` | Retrieve a single vector by ID | `python skills/vector-db/scripts/retrieve_by_id.py <collection> <vector_id>` |

### Configuration

Edit `skills/vector-db/assets/vector-config.json`:

```json
{
  "backend": "qdrant",          // pinecone | weaviate | qdrant | chromadb | milvus | pgvector
  "connection": {
    "host": "localhost",        // hostname or cloud endpoint
    "port": 6333,               // backend-specific port
    "api_key": "",              // API key (Pinecone, Weaviate cloud, etc.)
    "environment": "",          // Pinecone environment (e.g. "us-east1-gcp")
    "database": "",             // pgvector: PostgreSQL database name
    "extra": {}                 // backend-specific overrides
  }
}
```

### Usage Examples

```bash
# List collections
python skills/vector-db/scripts/list_collections.py

# Similarity search — top 5 results
python skills/vector-db/scripts/vector_search.py product-embeddings \
  '[0.12, -0.34, 0.56, ...]' 5

# Metadata filter
python skills/vector-db/scripts/metadata_filter.py product-embeddings \
  '{"category": "electronics", "in_stock": true}'

# Retrieve by ID
python skills/vector-db/scripts/retrieve_by_id.py product-embeddings vec_00123
```

---

## Graph DB Skill

**Skill directory:** `skills/graph-db/`

### Supported Backends

- **Neo4j** — Cypher query language, `bolt://` URI, username/password auth
- **Amazon Neptune / JanusGraph** — Gremlin query language, `wss://` URI, optional IAM auth

### Tools

| Script | Purpose | Signature |
|---|---|---|
| `list_schema.py` | List all node labels and relationship types | `python skills/graph-db/scripts/list_schema.py` |
| `get_properties.py` | Get properties for a label or relationship type | `python skills/graph-db/scripts/get_properties.py <label_or_type>` |
| `execute_cypher.py` | Run a Cypher query (Neo4j) | `python skills/graph-db/scripts/execute_cypher.py "<cypher>"` |
| `execute_gremlin.py` | Run a Gremlin query (Neptune / JanusGraph) | `python skills/graph-db/scripts/execute_gremlin.py "<gremlin>"` |
| `traverse_graph.py` | Traverse from a start node up to N hops (default 3) | `python skills/graph-db/scripts/traverse_graph.py <node_id> [depth]` |

### Configuration

Edit `skills/graph-db/assets/graph-config.json`:

```json
// Neo4j
{
  "backend": "neo4j",
  "connection": {
    "uri": "bolt://localhost:7687",
    "username": "neo4j",
    "password": "secret",
    "database": "neo4j"
  }
}

// Amazon Neptune (Gremlin + IAM auth)
{
  "backend": "neptune-gremlin",
  "connection": {
    "uri": "wss://your-cluster.cluster-xxxx.us-east-1.neptune.amazonaws.com:8182/gremlin",
    "region": "us-east-1",
    "iam_auth": true
  }
}
```

### Usage Examples

```bash
# List schema
python skills/graph-db/scripts/list_schema.py

# Get properties of the Customer node label
python skills/graph-db/scripts/get_properties.py Customer

# Cypher query (Neo4j)
python skills/graph-db/scripts/execute_cypher.py \
  "MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN c.name, p.title LIMIT 25"

# Gremlin query (Neptune)
python skills/graph-db/scripts/execute_gremlin.py \
  "g.V().hasLabel('Customer').limit(10).valueMap()"

# Traverse 2 hops from node 42
python skills/graph-db/scripts/traverse_graph.py 42 2
```

---

## Catalog Mapper Skill

**Skill directory:** `skills/catalog-mapper/`

Tracks relationships and lineage between data assets across all paradigms. Persists state to `skills/catalog-mapper/assets/catalog-map.json` (auto-created if missing).

### Asset URI Scheme

```
{paradigm}://{identifier}

athena://analytics_prod.events     # Athena table
vector://product-embeddings        # Vector collection
graph://Customer                   # Graph node label
```

### Relationship Types

| Type | Meaning |
|---|---|
| `derived_from` | Target was computed/derived from source |
| `feeds_into` | Source feeds data into target |
| `references` | Source references target |
| `mirrors` | Same data in different paradigms |
| `enriches` | Source adds context or features to target |

### Tools

| Script | Purpose | Signature |
|---|---|---|
| `register_relationship.py` | Register a cross-paradigm relationship | `python skills/catalog-mapper/scripts/register_relationship.py <source_uri> <target_uri> <type> [description]` |
| `query_relationships.py` | Query all relationships for an asset | `python skills/catalog-mapper/scripts/query_relationships.py <asset_uri>` |
| `list_assets.py` | List all registered assets | `python skills/catalog-mapper/scripts/list_assets.py` |
| `generate_lineage.py` | Full upstream + downstream lineage report | `python skills/catalog-mapper/scripts/generate_lineage.py <asset_uri>` |

### Usage Examples

```bash
# Register a relationship
python skills/catalog-mapper/scripts/register_relationship.py \
  athena://analytics_prod.events \
  vector://event-embeddings \
  derived_from \
  "Event embeddings generated from analytics_prod.events"

# Query relationships for an asset
python skills/catalog-mapper/scripts/query_relationships.py athena://analytics_prod.events

# List all registered assets
python skills/catalog-mapper/scripts/list_assets.py

# Generate full lineage report
python skills/catalog-mapper/scripts/generate_lineage.py vector://event-embeddings
```

---

## Generic Query Script

**Script:** `scripts/format-query-output.py`

Transforms raw result sets from any skill into a consistent **LLM-Friendly JSON** format.

### Usage

```bash
# From a JSON string
python scripts/format-query-output.py \
  --source-type athena \
  --input '[{"user_id": "u1", "cnt": 42}]'

# From a file
python scripts/format-query-output.py \
  --source-type vector \
  --input-file results.json

# With an explicit timestamp
python scripts/format-query-output.py \
  --source-type graph \
  --input-file graph_results.json \
  --timestamp 2024-01-15T10:30:00Z
```

`--source-type` accepts: `athena`, `vector`, `graph`, `catalog`.

### LLM-Friendly Format

All output is serialized as JSON with three top-level sections:

```json
{
  "header": {
    "source_type": "athena",
    "query_timestamp": "2024-01-15T10:30:00Z",
    "result_count": 2
  },
  "schema": {
    "fields": [
      { "name": "user_id", "type": "string" },
      { "name": "cnt",     "type": "integer" }
    ]
  },
  "data": [
    { "user_id": "u1", "cnt": 42 },
    { "user_id": "u2", "cnt": 17 }
  ]
}
```

| Section | Contents |
|---|---|
| `header` | Source type, ISO 8601 query timestamp, result count |
| `schema.fields` | Field names and inferred data types |
| `data` | Rows as key-value pairs; empty array when result set is empty |

**Vector schema** always includes: `id`, `score`, `metadata`, `payload`.  
**Graph schema** always includes: `id`, `labels`, `properties`.

---

## Query Best Practices

### Athena — Minimizing Scan Costs

Athena charges per byte scanned. These practices reduce cost significantly.

**Partition pruning** — always filter on partition columns in `WHERE`:

```sql
-- Good: partition filter eliminates entire S3 prefixes
SELECT * FROM analytics_prod.events
WHERE year = '2024' AND month = '01' AND day = '15'
  AND event_type = 'purchase'
LIMIT 1000;

-- Bad: full table scan
SELECT * FROM analytics_prod.events WHERE event_type = 'purchase';
```

**Columnar filtering** — select only the columns you need (Parquet/ORC skip unread columns):

```sql
-- Good: reads only two columns
SELECT user_id, event_type FROM analytics_prod.events WHERE ...;

-- Bad: reads every column
SELECT * FROM analytics_prod.events WHERE ...;
```

**Use LIMIT during exploration** — always add `LIMIT` when exploring unknown tables:

```sql
SELECT * FROM analytics_prod.events LIMIT 100;
```

**Prefer `preview_data.py` over raw queries** for initial data inspection — it enforces a row cap automatically.

**Avoid `SELECT COUNT(*)` on large tables** — use Glue table statistics or `fetch_schema.py` instead.

### Vector & Graph — Performance

**Batch sizing**

- Vector search: keep `top_k` ≤ 100 per request; batch multiple queries rather than issuing one very large `top_k`.
- Graph traversal: limit depth to 3–4 hops (`traverse_graph.py <id> 3`). Unbounded traversals on dense graphs can return millions of nodes.

**Index selection**

- Vector: ensure the target collection has an HNSW or IVF index built before running similarity search. Unindexed collections fall back to brute-force scan.
- Graph (Neo4j): create indexes on frequently filtered node properties (`CREATE INDEX ON :Customer(email)`). Use `EXPLAIN` / `PROFILE` to verify index usage.

**Connection pooling**

- Both `vector_backend.py` and `graph_backend.py` reuse a single connection per process. Avoid spawning many short-lived subprocesses in a tight loop — batch your operations into a single script invocation where possible.
- For Neptune, enable IAM auth (`"iam_auth": true` in `graph-config.json`) and reuse the signed WebSocket connection across queries.

---

## Access Whitelist Configuration

**File:** `skills/athena-glue/assets/access-whitelist.json`

The whitelist controls which Athena databases and tables the skill is authorized to query. Any query referencing an unlisted resource is rejected before execution.

### Format

```json
{
  "version": "1.0",
  "allowed": [
    {
      "database": "analytics_prod",
      "tables": ["events", "users", "sessions"]   // explicit table list
    },
    {
      "database": "data_lake",
      "tables": ["*"]                              // wildcard — all tables allowed
    }
  ]
}
```

### Adding a dataset

Append an entry to the `allowed` array:

```json
{
  "database": "ml_features",
  "tables": ["feature_store", "label_store"]
}
```

### Removing a dataset

Delete the corresponding entry from `allowed`, or remove individual table names from the `tables` array.

### Wildcard vs explicit

Use `"tables": ["*"]` only for trusted, read-only databases. Prefer explicit table lists for production databases to limit blast radius.

### Error behavior

- Missing or malformed `access-whitelist.json` → all data access operations are refused.
- Query references an unlisted database or table → error identifying the unauthorized resource; query is not submitted to Athena.

---

## Credential Resolver

The Athena/Glue skill resolves AWS credentials automatically using the following priority order. The first source that returns valid credentials wins.

| Priority | Source | How to configure |
|---|---|---|
| 1 | **IAM role** (EC2 / ECS / Lambda) | Attach an IAM role to the compute resource; no local config needed |
| 2 | **Environment variables** | Set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN` |
| 3 | **Shared credential file** | `~/.aws/credentials` — `[default]` or named profile |
| 4 | **AWS config file** | `~/.aws/config` — `[profile default]` or named profile |
| 5 | **Named profile** | Set `AWS_PROFILE=<profile_name>`; profile must exist in `~/.aws/credentials` or `~/.aws/config` |

### Configuring each method

**IAM role** — no local configuration required. Assign the role in the AWS console or via IaC (EC2 instance profile, ECS task role, Lambda execution role).

**Environment variables:**

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_SESSION_TOKEN=AQoXnyc...   # optional, for temporary credentials
```

**Shared credential file** (`~/.aws/credentials`):

```ini
[default]
aws_access_key_id     = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

[data-analytics]
aws_access_key_id     = AKIAI44QH8DHBEXAMPLE
aws_secret_access_key = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY
```

**AWS config file** (`~/.aws/config`):

```ini
[default]
region = us-east-1

[profile data-analytics]
region             = us-west-2
role_arn           = arn:aws:iam::123456789012:role/DataAnalyticsRole
source_profile     = default
```

**Named profile:**

```bash
export AWS_PROFILE=data-analytics
```

### Failure behavior

If all five methods fail, the skill returns a `credential_resolution_failed` error listing every attempted method and its failure reason:

```json
{
  "error": "credential_resolution_failed",
  "message": "Failed to resolve AWS credentials",
  "attempted_methods": [
    { "method": "iam_role",               "result": "no credentials found" },
    { "method": "environment_variables",  "result": "no credentials found" },
    { "method": "shared_credential_file", "result": "no credentials found" },
    { "method": "aws_config_file",        "result": "no credentials found" },
    { "method": "named_profile",          "result": "no credentials found" }
  ]
}
```
