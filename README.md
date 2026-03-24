# Data Analytics Skill Suite

A Claude Code plugin that provides a multi-agent system for querying, exploring, and correlating data across AWS Athena/Glue, vector stores, and graph databases. An orchestrator agent routes requests to specialized skill agents, each running in isolated context.

## Architecture

```
User Request → Orchestrator
                 ├── athena-glue     (Primary — SQL analysis via AWS Athena & Glue)
                 ├── vector-db       (Assistive — semantic clue discovery)
                 ├── graph-db        (Assistive — relationship discovery)
                 └── catalog-mapper  (Assistive — lineage tracking)
```

All data analysis happens through the Athena/Glue skill. The other three skills are assistive tools that record data relationships and find relation clues to support the analysis workflow.

## Installation

### From GitHub

Add the repository as a marketplace source, then install:

```
/plugin marketplace add kaija/data-analytics-plugin
/plugin install data-analytics-skill-suite@kaija-data-analytics-plugin
```

### From a local directory

Clone the repo and add it directly:

```bash
git clone https://github.com/kaija/data-analytics-plugin.git
```

```
/plugin marketplace add ./data-analytics-plugin
/plugin install data-analytics-skill-suite@data-analytics-plugin
```

After installation, run `/reload-plugins` to activate.

## Skills

### Athena/Glue (Primary Analysis)

Query and explore structured datasets in AWS Athena and Glue Data Catalog.

| Tool | Purpose |
|---|---|
| `list_databases.py` | List all Glue Data Catalog databases |
| `list_tables.py` | List tables in a database |
| `fetch_schema.py` | Fetch column names, types, partition keys |
| `preview_data.py` | Preview up to N rows |
| `execute_query.py` | Run SQL queries with scan cost reporting |

Requires AWS credentials (auto-resolved via IAM role, env vars, `~/.aws/credentials`, or named profile). Queries are validated against an access whitelist before execution.

### Vector DB (Assistive — Semantic Clue Discovery)

Find relation clues through vector similarity search. Supports Pinecone, Weaviate, Qdrant, ChromaDB, Milvus, pgvector, and AlloyDB.

| Tool | Purpose |
|---|---|
| `vector_search.py` | Similarity search by embedding or text query |
| `metadata_filter.py` | Filter vectors by metadata key-value pairs |
| `retrieve_by_id.py` | Retrieve a specific record by ID |
| `list_collections.py` | List available collections |

AlloyDB has built-in embedding support — pass a plain text string instead of a pre-computed embedding vector. AlloyDB computes the embedding server-side and auto-creates the table if it doesn't exist.

### Graph DB (Assistive — Relationship Discovery)

Record and discover entity relationships via graph traversal. Supports Neo4j (Cypher) and Amazon Neptune/JanusGraph (Gremlin).

| Tool | Purpose |
|---|---|
| `execute_cypher.py` | Run Cypher queries (Neo4j) |
| `execute_gremlin.py` | Run Gremlin queries (Neptune) |
| `list_schema.py` | List node labels and relationship types |
| `get_properties.py` | Get properties for a label or type |
| `traverse_graph.py` | Traverse from a start node to find connections |

### Catalog Mapper (Assistive — Lineage Tracking)

Track relationships and lineage between data assets across paradigms.

| Tool | Purpose |
|---|---|
| `register_relationship.py` | Record a cross-paradigm relationship |
| `query_relationships.py` | Query relationships for an asset |
| `list_assets.py` | List all registered assets |
| `generate_lineage.py` | Full upstream + downstream lineage report |

Assets use URI-style identifiers: `athena://db.table`, `vector://collection`, `graph://label`.

## Configuration

### AWS Credentials (Athena/Glue)

Credentials are resolved automatically in this order:

1. IAM role (EC2/ECS/Lambda)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. Shared credential file (`~/.aws/credentials`)
4. AWS config file (`~/.aws/config`)
5. Named profile (`AWS_PROFILE`)

### Access Whitelist

Edit `skills/athena-glue/assets/access-whitelist.json` to control which databases and tables are queryable:

```json
{
  "version": "1.0",
  "allowed": [
    { "database": "analytics_prod", "tables": ["events", "users"] },
    { "database": "data_lake", "tables": ["*"] }
  ]
}
```

### Vector DB Backend

Edit `skills/vector-db/assets/vector-config.json`:

```json
{
  "backend": "alloydb",
  "connection": {
    "host": "your-alloydb-ip",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "your-password",
    "table": "product",
    "embedding_column": "embedding",
    "embedding_model": "text-embedding-005"
  }
}
```

Supported backends: `pinecone`, `weaviate`, `qdrant`, `chromadb`, `milvus`, `pgvector`, `alloydb`.

### Graph DB Backend

Edit `skills/graph-db/assets/graph-config.json`:

```json
{
  "backend": "neo4j",
  "connection": {
    "uri": "bolt://localhost:7687",
    "username": "neo4j",
    "password": "secret",
    "database": "neo4j"
  }
}
```

Supported backends: `neo4j`, `neptune-gremlin`.

## Plugin Structure

```
data-analytics-skill-suite/
├── .claude-plugin/
│   ├── plugin.json              # Plugin manifest
│   └── marketplace.json         # Marketplace metadata
├── agents/
│   └── orchestrator.md          # Orchestrator agent (intent routing)
├── skills/
│   ├── athena-glue/             # Primary analysis skill
│   ├── vector-db/               # Assistive — semantic search
│   ├── graph-db/                # Assistive — graph traversal
│   └── catalog-mapper/          # Assistive — lineage tracking
├── scripts/
│   └── format-query-output.py   # LLM-friendly output formatter
├── hooks/
│   └── hooks.json               # Event handlers
├── settings.json                # Default agent config
├── SKILLS.md                    # Detailed skill documentation
└── README.md
```

Skills are auto-discovered by the presence of a `SKILL.md` file — no manifest changes needed when adding new skills.

## Development

### Prerequisites

- Python 3.12+
- AWS credentials configured (for Athena/Glue)

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Running Tests

```bash
python -m pytest -v
```

The test suite includes unit tests and Hypothesis property-based tests covering all skills, the orchestrator routing, plugin manifest validation, and output formatting.

## License

MIT — see [LICENSE](LICENSE).
