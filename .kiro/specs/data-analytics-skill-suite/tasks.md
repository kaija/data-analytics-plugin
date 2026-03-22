# Implementation Plan: Data Analytics Skill Suite

## Overview

Incremental implementation of the Data Analytics Skill Suite Claude Code plugin. Each task builds on previous work, starting with the plugin scaffold and progressing through each skill, the generic formatter, documentation, and tests. All code is Python.

## Tasks

- [x] 1. Plugin scaffold and orchestrator
  - [x] 1.1 Create plugin manifest and marketplace metadata
    - Create `.claude-plugin/plugin.json` with name, description, version, author object, keywords, license per the design schema
    - Create `.claude-plugin/marketplace.json` with owner, metadata, and plugins array
    - _Requirements: 1.1, 1.2_

  - [x] 1.2 Create plugin configuration files
    - Create `settings.json` with `{"agent": "orchestrator"}`
    - Create `.mcp.json` with empty/default MCP server configuration
    - Create empty `commands/` directory with a `.gitkeep`
    - Create `hooks/hooks.json` with empty hooks array
    - _Requirements: 1.4, 1.5, 1.6, 1.7_

  - [x] 1.3 Create orchestrator agent definition
    - Create `agents/orchestrator.md` with YAML frontmatter (name, description) and markdown body
    - Include intent classification rules mapping keywords to skill names
    - Include multi-paradigm sequential delegation instructions
    - Include ambiguous-request clarification instructions
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [x] 1.4 Write property test for plugin manifest required fields
    - **Property 1: Plugin manifest contains all required fields**
    - **Validates: Requirements 1.1**

- [x] 2. Athena/Glue skill — core infrastructure
  - [x] 2.1 Create Athena/Glue SKILL.md and directory structure
    - Create `skills/athena-glue/SKILL.md` with YAML frontmatter (`name: athena-glue`, `description`, `context: fork`, `allowed-tools`)
    - Create markdown body with tool usage instructions and security note
    - Create `skills/athena-glue/references/athena-best-practices.md` placeholder
    - _Requirements: 1.3, 2.6, 2.7_

  - [x] 2.2 Implement credential resolver
    - Create `skills/athena-glue/scripts/credential_resolver.py`
    - Implement `CredentialResolver` class with `DISCOVERY_ORDER` list and `resolve()` method
    - Implement each discovery method: IAM role, env vars, shared credential file, AWS config, named profile
    - Raise `CredentialResolutionError` with `attempted_methods` list on failure
    - _Requirements: 3.7, 3.8_

  - [x] 2.3 Implement access whitelist enforcement
    - Create `skills/athena-glue/assets/access-whitelist.json` with example config per the design schema
    - Create `skills/athena-glue/scripts/access_whitelist.py`
    - Implement `AccessWhitelist` class with `__init__` (load/validate), `is_authorized(database, table)`, and `validate_query(sql)` methods
    - Handle missing file, malformed JSON, and missing required keys errors
    - Parse SQL to extract referenced tables for validation
    - _Requirements: 3.9, 3.10, 3.11, 3.12_

  - [x] 2.4 Write property test for credential resolver priority order
    - **Property 9: Credential resolver follows priority order**
    - **Validates: Requirements 3.7**

  - [x] 2.5 Write property test for credential resolution failure
    - **Property 10: Credential resolution failure lists all attempted methods**
    - **Validates: Requirements 3.8**

  - [x] 2.6 Write property test for whitelist enforcement
    - **Property 11: Whitelist enforcement rejects unauthorized resources and identifies them**
    - **Validates: Requirements 3.9, 3.10**

- [x] 3. Athena/Glue skill — tool scripts
  - [x] 3.1 Implement list_databases.py
    - Create `skills/athena-glue/scripts/list_databases.py`
    - Use credential resolver, call Glue `get_databases`, format output via generic formatter
    - _Requirements: 3.1_

  - [x] 3.2 Implement list_tables.py
    - Create `skills/athena-glue/scripts/list_tables.py`
    - Accept `database` argument, use credential resolver, call Glue `get_tables`
    - _Requirements: 3.2_

  - [x] 3.3 Implement fetch_schema.py
    - Create `skills/athena-glue/scripts/fetch_schema.py`
    - Accept `database` and `table` arguments, return column names, data types, partition keys
    - _Requirements: 3.3_

  - [x] 3.4 Implement preview_data.py
    - Create `skills/athena-glue/scripts/preview_data.py`
    - Accept `database`, `table`, optional `max_rows` (default 100), enforce row limit via SQL LIMIT
    - Validate against access whitelist before executing
    - _Requirements: 3.4, 3.9_

  - [x] 3.5 Implement execute_query.py
    - Create `skills/athena-glue/scripts/execute_query.py`
    - Accept SQL string, validate against access whitelist, execute via Athena, return result set with `data_scanned_bytes` and `execution_time_ms` metadata
    - _Requirements: 3.5, 3.6, 3.9_

  - [x] 3.6 Write property tests for Athena tool scripts
    - **Property 6: Fetch schema returns complete column metadata**
    - **Property 7: Preview data respects row limit**
    - **Property 8: Query execution metadata is always present**
    - **Validates: Requirements 3.3, 3.4, 3.6**

  - [x] 3.7 Write unit tests for Athena tools
    - Test list_databases, list_tables, fetch_schema with mock AWS responses
    - Test whitelist rejection scenarios and credential error paths
    - _Requirements: 3.1–3.12_

- [x] 4. Checkpoint — Athena/Glue skill complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Vector DB skill
  - [x] 5.1 Create Vector DB SKILL.md and directory structure
    - Create `skills/vector-db/SKILL.md` with YAML frontmatter (`name: vector-db`, `description`, `context: fork`, `allowed-tools`)
    - Create markdown body with tool usage instructions
    - Create `skills/vector-db/assets/vector-config.json` with example config per the design schema
    - _Requirements: 1.3, 2.6, 2.7, 4.7_

  - [x] 5.2 Implement vector backend adapter interface and factory
    - Create `skills/vector-db/scripts/vector_backend.py`
    - Define `VectorBackendAdapter` ABC with `connect`, `search`, `filter_by_metadata`, `get_by_id`, `list_collections` abstract methods
    - Define `VectorResult` data class (id, score, metadata, payload)
    - Implement `create_adapter(backend_type, config)` factory function
    - Raise `UnsupportedBackendError` listing supported backends for unknown types
    - _Requirements: 4.1, 4.9_

  - [x] 5.3 Implement all six vector backend adapters
    - Implement `PineconeAdapter`, `WeaviateAdapter`, `QdrantAdapter`, `ChromaDBAdapter`, `MilvusAdapter`, `PgvectorAdapter` in `skills/vector-db/scripts/vector_backend.py` (or separate files)
    - Each adapter: `connect` with config, `search` with embedding + top_k, `filter_by_metadata`, `get_by_id`, `list_collections`
    - Normalize results to `VectorResult` format
    - Return descriptive errors on connection failure with backend name and reason
    - _Requirements: 4.1, 4.6, 4.8_

  - [x] 5.4 Implement vector tool scripts
    - Create `skills/vector-db/scripts/vector_search.py` — load config, create adapter, perform search, format output
    - Create `skills/vector-db/scripts/metadata_filter.py` — filter by metadata key-value expressions
    - Create `skills/vector-db/scripts/retrieve_by_id.py` — retrieve vector by ID
    - Create `skills/vector-db/scripts/list_collections.py` — list available collections
    - _Requirements: 4.2, 4.3, 4.4, 4.5_

  - [x] 5.5 Write property tests for vector skill
    - **Property 12: Vector backend adapter factory creates valid adapters for all supported backends**
    - **Property 13: Vector search respects top-k limit**
    - **Property 14: Metadata filter returns only matching vectors**
    - **Property 15: Vector retrieve-by-ID round trip**
    - **Property 16: Vector result normalization includes all required fields**
    - **Property 17: Unsupported vector backend error lists all supported backends**
    - **Property 18: Vector connection failure error identifies backend and reason**
    - **Validates: Requirements 4.1–4.9**

  - [x] 5.6 Write unit tests for vector adapters and tools
    - Test adapter factory with each supported backend
    - Test connection failure error format
    - Test empty search results return valid structure
    - _Requirements: 4.1–4.9_

- [x] 6. Checkpoint — Vector DB skill complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Graph DB skill
  - [x] 7.1 Create Graph DB SKILL.md and directory structure
    - Create `skills/graph-db/SKILL.md` with YAML frontmatter (`name: graph-db`, `description`, `context: fork`, `allowed-tools`)
    - Create markdown body with tool usage instructions
    - Create `skills/graph-db/assets/graph-config.json` with example config per the design schema
    - _Requirements: 1.3, 2.6, 2.7, 5.8_

  - [x] 7.2 Implement graph backend adapter interface and factory
    - Create `skills/graph-db/scripts/graph_backend.py`
    - Define `GraphBackendAdapter` ABC with `connect`, `execute_query`, `list_node_labels`, `list_relationship_types`, `get_properties`, `traverse` abstract methods
    - Define `GraphQueryResult`, `SchemaProperties`, `GraphTraversalResult` data classes
    - Implement `create_adapter(backend_type, config)` factory function
    - Raise `UnsupportedBackendError` for unknown types
    - _Requirements: 5.1_

  - [x] 7.3 Implement Neo4j and Neptune/Gremlin backend adapters
    - Implement `Neo4jAdapter(GraphBackendAdapter)` — Cypher queries, bolt:// connection
    - Implement `NeptuneGremlinAdapter(GraphBackendAdapter)` — Gremlin queries, wss:// connection, optional IAM auth
    - Normalize results to nodes (id, labels, properties) and relationships (id, type, source, target, properties)
    - Return descriptive errors on connection failure with backend name and reason
    - _Requirements: 5.1, 5.7, 5.9_

  - [x] 7.4 Implement graph tool scripts
    - Create `skills/graph-db/scripts/execute_cypher.py` — load config, create Neo4j adapter, execute query
    - Create `skills/graph-db/scripts/execute_gremlin.py` — load config, create Neptune adapter, execute query
    - Create `skills/graph-db/scripts/list_schema.py` — list node labels and relationship types
    - Create `skills/graph-db/scripts/get_properties.py` — get properties for a label or type
    - Create `skills/graph-db/scripts/traverse_graph.py` — traverse from start node with configurable depth (default 3)
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6_

  - [x] 7.5 Write property tests for graph skill
    - **Property 19: Graph backend adapter factory creates valid adapters for all supported backends**
    - **Property 20: Graph traversal respects depth limit**
    - **Property 21: Graph result normalization includes all required fields**
    - **Property 22: Graph connection failure error identifies backend and reason**
    - **Validates: Requirements 5.1, 5.6, 5.7, 5.9**

  - [x] 7.6 Write unit tests for graph adapters and tools
    - Test adapter factory with neo4j and neptune-gremlin
    - Test Cypher and Gremlin query execution with mock backends
    - Test traversal depth enforcement
    - Test connection failure and invalid syntax error handling
    - _Requirements: 5.1–5.9_

- [x] 8. Checkpoint — Graph DB skill complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Catalog Mapper skill
  - [x] 9.1 Create Catalog Mapper SKILL.md and directory structure
    - Create `skills/catalog-mapper/SKILL.md` with YAML frontmatter (`name: catalog-mapper`, `description`, `context: fork`, `allowed-tools`)
    - Create markdown body with tool usage instructions and asset URI scheme documentation
    - Create `skills/catalog-mapper/assets/catalog-map.json` with empty catalog: `{"assets": [], "relationships": []}`
    - _Requirements: 1.3, 2.6, 2.7, 6.5_

  - [x] 9.2 Implement catalog persistence layer
    - Create `skills/catalog-mapper/scripts/catalog_persistence.py`
    - Implement load/save functions for `catalog-map.json`
    - Auto-create empty catalog if file is missing
    - Return descriptive error and refuse writes if file is malformed
    - Implement duplicate relationship detection (idempotent registration)
    - _Requirements: 6.5, 6.8, 6.9_

  - [x] 9.3 Implement catalog tool scripts
    - Create `skills/catalog-mapper/scripts/register_relationship.py` — register cross-paradigm relationship with source, target, type, optional description; generate UUID for relationship ID; set `created_at` timestamp; flag unresolved assets
    - Create `skills/catalog-mapper/scripts/query_relationships.py` — query all relationships for a given asset
    - Create `skills/catalog-mapper/scripts/list_assets.py` — list all registered assets with paradigm type
    - Create `skills/catalog-mapper/scripts/generate_lineage.py` — generate lineage report with upstream and downstream transitive relationships
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.6, 6.7_

  - [x] 9.4 Write property tests for catalog mapper
    - **Property 23: Catalog register/query round trip**
    - **Property 24: Catalog list assets returns all registered assets with paradigm**
    - **Property 25: Catalog persistence round trip**
    - **Property 26: Unresolved assets are flagged**
    - **Property 27: Lineage report includes all transitive relationships**
    - **Validates: Requirements 6.2–6.7**

  - [x] 9.5 Write unit tests for catalog mapper
    - Test register, query, list, lineage with known data
    - Test missing catalog file auto-creation
    - Test malformed catalog file error handling
    - Test duplicate relationship idempotency
    - _Requirements: 6.1–6.9_

- [x] 10. Generic Query Script and output formatting
  - [x] 10.1 Implement format-query-output.py
    - Create `scripts/format-query-output.py`
    - Implement `format_query_output(source_type, result_set, query_timestamp)` function
    - Accept source types: athena, vector, graph, catalog
    - Transform result sets into LLM-Friendly Format with header (source_type, query_timestamp, result_count), schema (fields with name and type), and data (key-value rows)
    - Auto-generate ISO 8601 timestamp if not provided
    - Handle empty result sets (return header + schema + empty data array)
    - Raise `FormatError` for unparseable input with source and failure details
    - Make script invocable standalone via `if __name__ == "__main__"` with CLI args
    - _Requirements: 7.1–7.9_

  - [x] 10.2 Write property tests for generic query script
    - **Property 28: LLM-Friendly Format validation**
    - **Property 29: Format output is valid JSON**
    - **Property 30: Formatter accepts all four source types**
    - **Validates: Requirements 7.1–7.8**

  - [x] 10.3 Write unit tests for generic query script
    - Test each source type with sample data
    - Test empty result set handling
    - Test unparseable input error
    - Test unknown source type error
    - _Requirements: 7.1–7.9_

- [x] 11. Checkpoint — All skills and formatter complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Wire skills to orchestrator and integrate formatter
  - [x] 12.1 Wire formatter into all skill scripts
    - Update each Athena tool script to pipe output through `format-query-output.py`
    - Update each Vector tool script to pipe output through `format-query-output.py`
    - Update each Graph tool script to pipe output through `format-query-output.py`
    - Update each Catalog tool script to pipe output through `format-query-output.py`
    - Ensure all tool outputs use the LLM-Friendly Format consistently
    - _Requirements: 7.1, 7.2_

  - [x] 12.2 Verify orchestrator routing to all skills
    - Ensure orchestrator.md references all four skill names correctly (`athena-glue`, `vector-db`, `graph-db`, `catalog-mapper`)
    - Verify intent classification keywords map to correct skills
    - _Requirements: 2.1_

  - [x] 12.3 Write property tests for orchestrator routing
    - **Property 3: Orchestrator routes to correct skill by intent**
    - **Property 4: Multi-paradigm delegation covers all relevant skills**
    - **Property 5: Ambiguous intent triggers clarification**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.5**

  - [x] 12.4 Write property test for skill auto-discovery
    - **Property 2: Skill auto-discovery without manifest changes**
    - **Validates: Requirements 1.8**

- [x] 13. SKILLS.md documentation
  - [x] 13.1 Create SKILLS.md
    - Create top-level `SKILLS.md` with overview of the suite
    - Document Athena/Glue skill: tools, configuration, usage examples
    - Document Vector DB skill: tools, configuration, usage examples
    - Document Graph DB skill: tools, configuration, usage examples
    - Document Catalog Mapper: tools, configuration, usage examples
    - Document Generic Query Script: usage and output format
    - Include query best practices: Athena scan cost minimization (partitioning, columnar filtering, LIMIT)
    - Include query best practices: vector/graph performance (batch sizing, index selection, connection pooling)
    - Document access whitelist configuration format
    - Document credential resolver discovery order and configuration
    - _Requirements: 8.1–8.10_

- [x] 14. Test infrastructure and conftest
  - [x] 14.1 Create test infrastructure
    - Create `tests/conftest.py` with shared fixtures (mock AWS clients, mock vector/graph backends, temp catalog files, sample data)
    - Create `tests/unit/` and `tests/property/` directories
    - Ensure Hypothesis is configured with minimum 100 iterations per property test
    - _Requirements: all testing properties_

- [x] 15. Final checkpoint — Full suite complete
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design (Properties 1–30)
- Unit tests validate specific examples and edge cases
- All code is Python; Hypothesis is used for property-based testing
