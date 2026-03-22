# Requirements Document

## Introduction

The Data Analytics Skill Suite is a comprehensive multi-agent plugin for the Claude Code plugin marketplace. It provides Data Scientists with a unified set of agent skills for querying, exploring, and correlating data across structured databases (AWS Athena/Glue), vector stores, and graph databases. The suite follows the standard plugin folder structure and SKILL.md specification, enabling progressive disclosure, context isolation, and secure credential management. A cross-paradigm data catalog mapper and a generic query script standardize outputs into an LLM-friendly format.

## Glossary

- **Plugin**: A packaged extension conforming to the Claude Code plugin marketplace specification, containing a manifest, skills, commands, agents, and hooks.
- **Skill**: A self-contained agent capability defined by a SKILL.md file with YAML frontmatter and markdown instructions, optionally accompanied by scripts, references, and assets.
- **Orchestrator_Agent**: The top-level supervisory agent that routes user requests to the appropriate specialized Skill_Agent based on intent.
- **Skill_Agent**: A specialized sub-agent responsible for a single data paradigm (structured, vector, or graph) or cross-cutting concern (catalog mapping, generic query).
- **Athena_Skill**: The agent skill that interfaces with AWS Athena for SQL query execution and AWS Glue Data Catalog for metadata discovery.
- **Vector_Skill**: The agent skill that interfaces with industry-standard vector databases for embedding-based retrieval, vector search, and metadata filtering.
- **Graph_Skill**: The agent skill that interfaces with graph databases for traversal, relationship discovery, and pattern-matching queries.
- **Catalog_Mapper**: The agent skill that analyzes, records, and tracks relationships and lineage across structured, vector, and graph data assets.
- **Generic_Query_Script**: A script that standardizes query outputs from all supported data sources into a consistent, LLM-friendly format.
- **Access_Whitelist**: A configuration file that enumerates the databases and tables an agent is authorized to access.
- **Credential_Resolver**: The component responsible for automatic AWS credential discovery via IAM roles, environment variables, shared credential files, AWS config files, or named profiles.
- **Plugin_Manifest**: The `.claude-plugin/plugin.json` file describing the plugin name, description, version, and author.
- **SKILL.md**: The required file in each skill directory containing YAML frontmatter metadata and markdown instructions for the agent.
- **Progressive_Disclosure**: The three-level information loading strategy: metadata, full instructions, then supporting resources.
- **LLM_Friendly_Format**: A structured, readable output format optimized for consumption by large language models, using consistent delimiters and schema annotations.

## Requirements

### Requirement 1: Plugin Folder Structure Initialization

**User Story:** As a Data Scientist, I want the plugin to follow the standard Claude Code plugin marketplace folder structure, so that the suite integrates seamlessly with the plugin ecosystem.

#### Acceptance Criteria

1. THE Plugin SHALL contain a `.claude-plugin/plugin.json` Plugin_Manifest file with name, description, version, and author fields.
2. THE Plugin SHALL contain a `.claude-plugin/marketplace.json` file at the repository root.
3. THE Plugin SHALL contain a `skills/` directory with one subdirectory per Skill, each containing a SKILL.md file.
4. THE Plugin SHALL contain a `commands/` directory for slash command definitions.
5. THE Plugin SHALL contain an `agents/` directory for custom agent definitions including the Orchestrator_Agent.
6. THE Plugin SHALL contain a `hooks/` directory for event handler definitions.
7. THE Plugin SHALL contain a `.mcp.json` file for MCP server configuration.
8. WHEN a new Skill is added to the `skills/` directory, THE Plugin SHALL recognize the Skill without requiring changes to the Plugin_Manifest.

### Requirement 2: Multi-Agent Skill Architecture

**User Story:** As a Data Scientist, I want a multi-agent architecture with a supervisory orchestrator and specialized sub-agents, so that each data paradigm is handled by a context-isolated expert.

#### Acceptance Criteria

1. THE Orchestrator_Agent SHALL route incoming user requests to the appropriate Skill_Agent based on the detected intent.
2. THE Orchestrator_Agent SHALL support sequential delegation to multiple Skill_Agents within a single user request when the request spans multiple data paradigms.
3. WHEN a Skill_Agent completes execution, THE Orchestrator_Agent SHALL aggregate the Skill_Agent response into a unified reply to the user.
4. THE Orchestrator_Agent SHALL maintain context isolation between Skill_Agents by forking separate sub-agent contexts for each delegation.
5. WHEN the Orchestrator_Agent cannot determine the appropriate Skill_Agent, THE Orchestrator_Agent SHALL prompt the user for clarification.
6. THE Skill_Agent SKILL.md files SHALL use the `context: fork` frontmatter field to execute as sub-agents with isolated context.
7. THE Skill_Agent SKILL.md files SHALL follow the Progressive_Disclosure pattern with metadata, full instructions, and supporting resources as three distinct levels.

### Requirement 3: AWS Athena and Glue Data Catalog Skill

**User Story:** As a Data Scientist, I want to discover, explore, and query structured datasets in AWS Athena and Glue Data Catalog through an agent skill, so that I can analyze tabular data without leaving the agent environment.

#### Acceptance Criteria

1. THE Athena_Skill SHALL provide a tool to list all databases registered in the AWS Glue Data Catalog.
2. THE Athena_Skill SHALL provide a tool to list all tables within a specified Glue Data Catalog database.
3. THE Athena_Skill SHALL provide a tool to fetch the full schema (column names, data types, partition keys) of a specified table.
4. THE Athena_Skill SHALL provide a tool to preview raw data from a specified table, limited to a configurable maximum number of rows with a default of 100 rows.
5. THE Athena_Skill SHALL provide a tool to execute arbitrary SQL queries against AWS Athena and return the result set.
6. WHEN executing a SQL query, THE Athena_Skill SHALL report the volume of data scanned and the query execution time.
7. THE Athena_Skill SHALL resolve AWS credentials automatically using the Credential_Resolver in the following priority order: IAM roles, environment variables, shared credential file, AWS config file, named profiles.
8. WHEN the Credential_Resolver fails to discover valid credentials, THE Athena_Skill SHALL return a descriptive error message listing the attempted discovery methods.
9. THE Athena_Skill SHALL enforce the Access_Whitelist by rejecting queries that reference databases or tables not present in the whitelist.
10. WHEN a query references a database or table not in the Access_Whitelist, THE Athena_Skill SHALL return an error message identifying the unauthorized resource.
11. THE Athena_Skill SHALL load the Access_Whitelist from a configuration file located at `skills/athena-glue/assets/access-whitelist.json`.
12. IF the Access_Whitelist configuration file is missing or malformed, THEN THE Athena_Skill SHALL refuse all data access operations and return a descriptive error.

### Requirement 4: Vector Database Agent Skill

**User Story:** As a Data Scientist, I want to perform vector search, metadata filtering, and embedding-based retrieval across multiple vector store backends, so that I can leverage semantic similarity search from a unified interface.

#### Acceptance Criteria

1. THE Vector_Skill SHALL support connections to Pinecone, Weaviate, Qdrant, ChromaDB, Milvus, and pgvector backends.
2. THE Vector_Skill SHALL provide a tool to perform vector similarity search given a query embedding and a top-k parameter.
3. THE Vector_Skill SHALL provide a tool to perform metadata filtering on stored vectors using key-value filter expressions.
4. THE Vector_Skill SHALL provide a tool to retrieve vectors by ID from the connected vector store.
5. THE Vector_Skill SHALL provide a tool to list available collections or indices in the connected vector store.
6. WHEN a connection to a specified vector store backend fails, THE Vector_Skill SHALL return a descriptive error message identifying the backend and the failure reason.
7. THE Vector_Skill SHALL load backend connection configuration from `skills/vector-db/assets/vector-config.json`.
8. THE Vector_Skill SHALL normalize result formats from all supported backends into a consistent structure containing: vector ID, score, metadata, and optionally the vector payload.
9. WHEN a user specifies a backend that is not in the supported list, THE Vector_Skill SHALL return an error listing the supported backends.

### Requirement 5: Graph Database Agent Skill

**User Story:** As a Data Scientist, I want to traverse graphs, discover relationships, and execute pattern-matching queries across graph databases, so that I can explore connected data structures through the agent.

#### Acceptance Criteria

1. THE Graph_Skill SHALL support connections to Neo4j (Cypher) and Amazon Neptune/JanusGraph (Gremlin) backends.
2. THE Graph_Skill SHALL provide a tool to execute Cypher queries against Neo4j backends.
3. THE Graph_Skill SHALL provide a tool to execute Gremlin queries against Neptune and JanusGraph backends.
4. THE Graph_Skill SHALL provide a tool to list all node labels and relationship types in the connected graph database.
5. THE Graph_Skill SHALL provide a tool to retrieve the properties and schema of a specified node label or relationship type.
6. THE Graph_Skill SHALL provide a tool to perform graph traversal starting from a specified node, returning connected nodes up to a configurable depth with a default of 3.
7. WHEN a connection to a specified graph database backend fails, THE Graph_Skill SHALL return a descriptive error message identifying the backend and the failure reason.
8. THE Graph_Skill SHALL load backend connection configuration from `skills/graph-db/assets/graph-config.json`.
9. THE Graph_Skill SHALL normalize query results from all supported backends into a consistent structure containing: nodes (with labels and properties) and relationships (with type, source, target, and properties).

### Requirement 6: Data Catalog Relationship Mapper

**User Story:** As a Data Scientist, I want to analyze and track relationships and lineage between structured data, vector indices, and graph nodes, so that I can understand how data assets relate across paradigms.

#### Acceptance Criteria

1. THE Catalog_Mapper SHALL maintain a mapping log that records relationships between Athena tables, vector collections, and graph node labels.
2. THE Catalog_Mapper SHALL provide a tool to register a new cross-paradigm relationship specifying source asset, target asset, relationship type, and an optional description.
3. THE Catalog_Mapper SHALL provide a tool to query all registered relationships for a specified data asset.
4. THE Catalog_Mapper SHALL provide a tool to list all registered data assets and their paradigm type (structured, vector, or graph).
5. THE Catalog_Mapper SHALL persist the mapping log to `skills/catalog-mapper/assets/catalog-map.json`.
6. WHEN a registered data asset is referenced that does not exist in any connected backend, THE Catalog_Mapper SHALL flag the asset as unresolved in the mapping log.
7. THE Catalog_Mapper SHALL provide a tool to generate a lineage report for a specified data asset, showing all upstream and downstream relationships.
8. IF the catalog-map.json file is missing, THEN THE Catalog_Mapper SHALL create a new empty mapping log file.
9. IF the catalog-map.json file is malformed, THEN THE Catalog_Mapper SHALL return a descriptive error and refuse write operations until the file is repaired.

### Requirement 7: Generic Data Query Script

**User Story:** As a Data Scientist, I want query outputs from all data sources standardized into a consistent, LLM-friendly format, so that I can compare and combine results regardless of the source.

#### Acceptance Criteria

1. THE Generic_Query_Script SHALL accept query results from the Athena_Skill, Vector_Skill, Graph_Skill, and Catalog_Mapper.
2. THE Generic_Query_Script SHALL transform all input result sets into the LLM_Friendly_Format.
3. THE LLM_Friendly_Format SHALL include a header section with source type, query timestamp, and result count.
4. THE LLM_Friendly_Format SHALL include a schema section listing field names and data types.
5. THE LLM_Friendly_Format SHALL include a data section with rows represented as key-value pairs.
6. WHEN the input result set is empty, THE Generic_Query_Script SHALL return the header and schema sections with an empty data section.
7. IF the input result set cannot be parsed, THEN THE Generic_Query_Script SHALL return a descriptive error identifying the source and the parsing failure.
8. THE Generic_Query_Script SHALL serialize output as JSON.
9. THE Generic_Query_Script SHALL be invocable as a standalone script from `scripts/format-query-output.py`.

### Requirement 8: SKILLS.md Documentation

**User Story:** As a Data Scientist, I want comprehensive documentation for each agent skill including query best practices, so that I can use the suite effectively and minimize scan costs.

#### Acceptance Criteria

1. THE Plugin SHALL include a top-level `SKILLS.md` file that provides an overview and user guide for each Skill in the suite.
2. THE SKILLS.md file SHALL document the available tools, required configuration, and usage examples for the Athena_Skill.
3. THE SKILLS.md file SHALL document the available tools, required configuration, and usage examples for the Vector_Skill.
4. THE SKILLS.md file SHALL document the available tools, required configuration, and usage examples for the Graph_Skill.
5. THE SKILLS.md file SHALL document the available tools, required configuration, and usage examples for the Catalog_Mapper.
6. THE SKILLS.md file SHALL document the usage and output format of the Generic_Query_Script.
7. THE SKILLS.md file SHALL include a query best practices section with guidance on minimizing scan costs for Athena, including partitioning, columnar filtering, and LIMIT usage.
8. THE SKILLS.md file SHALL include a query best practices section with guidance on maximizing performance for distributed vector and graph databases, including batch sizing, index selection, and connection pooling.
9. THE SKILLS.md file SHALL document the Access_Whitelist configuration format and how to add or remove authorized datasets.
10. THE SKILLS.md file SHALL document the Credential_Resolver discovery order and how to configure each credential method.
