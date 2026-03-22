"""Graph backend adapter interface and factory.

Provides an abstract base class for graph database backends, normalized
result data classes, and a factory function to create adapter instances.

Requirements: 5.1
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class GraphQueryResult:
    """Normalized result returned by graph query execution.

    Attributes:
        nodes: List of node dicts with id, labels, and properties.
        relationships: List of relationship dicts with id, type, source, target, and properties.
        metadata: Optional metadata about the query execution.
    """

    nodes: list[dict[str, Any]] = field(default_factory=list)
    relationships: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] | None = None


@dataclass
class SchemaProperties:
    """Schema properties for a node label or relationship type.

    Attributes:
        label_or_type: The node label or relationship type name.
        properties: List of property dicts with name and type.
    """

    label_or_type: str
    properties: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class GraphTraversalResult:
    """Result of a graph traversal operation.

    Attributes:
        start_node_id: The ID of the starting node.
        depth: The traversal depth used.
        nodes: List of node dicts encountered during traversal.
        relationships: List of relationship dicts encountered during traversal.
    """

    start_node_id: str
    depth: int
    nodes: list[dict[str, Any]] = field(default_factory=list)
    relationships: list[dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

SUPPORTED_BACKENDS: list[str] = [
    "neo4j",
    "neptune-gremlin",
]


class UnsupportedBackendError(Exception):
    """Raised when an unknown graph backend type is requested."""

    def __init__(self, backend_type: str) -> None:
        self.backend_type = backend_type
        self.supported_backends = list(SUPPORTED_BACKENDS)
        super().__init__(
            f"Unsupported graph backend '{backend_type}'. "
            f"Supported backends: {', '.join(SUPPORTED_BACKENDS)}"
        )


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class GraphBackendAdapter(ABC):
    """Abstract interface that every graph backend adapter must implement."""

    @abstractmethod
    def connect(self, config: dict) -> None:
        """Establish connection to the graph database.

        Args:
            config: Backend-specific connection configuration.
        """

    @abstractmethod
    def execute_query(self, query: str) -> GraphQueryResult:
        """Execute a native query (Cypher or Gremlin).

        Args:
            query: The query string to execute.

        Returns:
            GraphQueryResult with nodes and relationships.
        """

    @abstractmethod
    def list_node_labels(self) -> list[str]:
        """List all node labels in the graph.

        Returns:
            List of node label strings.
        """

    @abstractmethod
    def list_relationship_types(self) -> list[str]:
        """List all relationship types in the graph.

        Returns:
            List of relationship type strings.
        """

    @abstractmethod
    def get_properties(self, label_or_type: str) -> SchemaProperties:
        """Get properties and schema for a node label or relationship type.

        Args:
            label_or_type: The node label or relationship type to inspect.

        Returns:
            SchemaProperties with property names and types.
        """

    @abstractmethod
    def traverse(self, start_node_id: str, depth: int = 3) -> GraphTraversalResult:
        """Traverse the graph from a starting node to a configurable depth.

        Args:
            start_node_id: The ID of the node to start traversal from.
            depth: Maximum traversal depth (default: 3).

        Returns:
            GraphTraversalResult with all encountered nodes and relationships.
        """


# ---------------------------------------------------------------------------
# Adapter implementations
# ---------------------------------------------------------------------------

class Neo4jAdapter(GraphBackendAdapter):
    """Neo4j graph backend adapter (Cypher queries, bolt:// connection)."""

    def __init__(self) -> None:
        self._client: Any | None = None
        self._database: str = "neo4j"

    def _ensure_connected(self) -> None:
        if self._client is None:
            raise RuntimeError("Neo4jAdapter is not connected. Call connect() first.")

    def connect(self, config: dict) -> None:
        try:
            from neo4j import GraphDatabase  # type: ignore[import-untyped]
        except ImportError:
            raise ConnectionError(
                "Neo4j: failed to connect — the 'neo4j' package is not installed. "
                "Install it with: pip install neo4j"
            )
        try:
            uri = config.get("uri", "bolt://localhost:7687")
            username = config.get("username", "neo4j")
            password = config.get("password", "")
            self._database = config.get("database", "neo4j")
            driver = GraphDatabase.driver(uri, auth=(username, password))
            driver.verify_connectivity()
            self._client = driver
        except Exception as exc:
            raise ConnectionError(f"Neo4j: failed to connect — {exc}") from exc

    def _run_cypher(self, query: str) -> list[Any]:
        """Execute a Cypher query and return raw records."""
        with self._client.session(database=self._database) as session:
            result = session.run(query)
            return list(result)

    @staticmethod
    def _normalize_node(node: Any) -> dict[str, Any]:
        """Normalize a neo4j Node to a plain dict."""
        return {
            "id": node.element_id if hasattr(node, "element_id") else str(node.id),
            "labels": list(node.labels),
            "properties": dict(node),
        }

    @staticmethod
    def _normalize_relationship(rel: Any) -> dict[str, Any]:
        """Normalize a neo4j Relationship to a plain dict."""
        return {
            "id": rel.element_id if hasattr(rel, "element_id") else str(rel.id),
            "type": rel.type,
            "source": rel.start_node.element_id if hasattr(rel.start_node, "element_id") else str(rel.start_node.id),
            "target": rel.end_node.element_id if hasattr(rel.end_node, "element_id") else str(rel.end_node.id),
            "properties": dict(rel),
        }

    def execute_query(self, query: str) -> GraphQueryResult:
        self._ensure_connected()
        # Import neo4j graph types lazily; fall back to duck-typing if unavailable
        try:
            import neo4j.graph as _neo4j_graph  # type: ignore[import-untyped]
            _Node = _neo4j_graph.Node
            _Relationship = _neo4j_graph.Relationship
            _Path = getattr(_neo4j_graph, "Path", None)
        except ImportError:
            _Node = None
            _Relationship = None
            _Path = None

        records = self._run_cypher(query)
        nodes: list[dict[str, Any]] = []
        relationships: list[dict[str, Any]] = []
        seen_node_ids: set[str] = set()
        seen_rel_ids: set[str] = set()

        def _is_node(v: Any) -> bool:
            if _Node is not None:
                return isinstance(v, _Node)
            return hasattr(v, "labels") and hasattr(v, "element_id")

        def _is_relationship(v: Any) -> bool:
            if _Relationship is not None:
                return isinstance(v, _Relationship)
            return hasattr(v, "type") and hasattr(v, "start_node") and hasattr(v, "end_node")

        def _is_path(v: Any) -> bool:
            if _Path is not None:
                return isinstance(v, _Path)
            return hasattr(v, "nodes") and hasattr(v, "relationships")

        def _collect(value: Any) -> None:
            if _is_node(value):
                nid = value.element_id if hasattr(value, "element_id") else str(value.id)
                if nid not in seen_node_ids:
                    seen_node_ids.add(nid)
                    nodes.append(self._normalize_node(value))
            elif _is_relationship(value):
                rid = value.element_id if hasattr(value, "element_id") else str(value.id)
                if rid not in seen_rel_ids:
                    seen_rel_ids.add(rid)
                    relationships.append(self._normalize_relationship(value))
                _collect(value.start_node)
                _collect(value.end_node)
            elif _is_path(value):
                for node in value.nodes:
                    _collect(node)
                for rel in value.relationships:
                    _collect(rel)

        for record in records:
            for value in record.values():
                _collect(value)

        return GraphQueryResult(nodes=nodes, relationships=relationships)

    def list_node_labels(self) -> list[str]:
        self._ensure_connected()
        records = self._run_cypher("CALL db.labels() YIELD label RETURN label")
        return [record["label"] for record in records]

    def list_relationship_types(self) -> list[str]:
        self._ensure_connected()
        records = self._run_cypher(
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
        )
        return [record["relationshipType"] for record in records]

    def get_properties(self, label_or_type: str) -> SchemaProperties:
        self._ensure_connected()
        # Try APOC first, fall back to MATCH
        try:
            records = self._run_cypher("CALL apoc.meta.schema() YIELD value")
            if records:
                schema_value = records[0]["value"]
                props_raw = schema_value.get(label_or_type, {}).get("properties", {})
                properties = [
                    {"name": k, "type": v.get("type", "unknown") if isinstance(v, dict) else str(v)}
                    for k, v in props_raw.items()
                ]
                return SchemaProperties(label_or_type=label_or_type, properties=properties)
        except Exception:
            pass
        # Fallback: sample a node with that label
        records = self._run_cypher(
            f"MATCH (n:`{label_or_type}`) RETURN keys(n) LIMIT 1"
        )
        if records:
            keys = records[0]["keys(n)"]
            properties = [{"name": k, "type": "unknown"} for k in keys]
        else:
            properties = []
        return SchemaProperties(label_or_type=label_or_type, properties=properties)

    def traverse(self, start_node_id: str, depth: int = 3) -> GraphTraversalResult:
        self._ensure_connected()
        query = (
            f"MATCH path = (start)-[*1..{depth}]-(end) "
            f"WHERE id(start) = {start_node_id} RETURN path"
        )
        result = self.execute_query(query)
        return GraphTraversalResult(
            start_node_id=str(start_node_id),
            depth=depth,
            nodes=result.nodes,
            relationships=result.relationships,
        )


class NeptuneGremlinAdapter(GraphBackendAdapter):
    """Neptune/Gremlin graph backend adapter (wss:// connection, optional IAM auth)."""

    def __init__(self) -> None:
        self._client: Any | None = None

    def _ensure_connected(self) -> None:
        if self._client is None:
            raise RuntimeError("NeptuneGremlinAdapter is not connected. Call connect() first.")

    def connect(self, config: dict) -> None:
        try:
            from gremlin_python.driver import client as gremlin_client  # type: ignore[import-untyped]
        except ImportError:
            raise ConnectionError(
                "Neptune/Gremlin: failed to connect — the 'gremlinpython' package is not installed. "
                "Install it with: pip install gremlinpython"
            )
        try:
            uri = config.get("uri", "wss://localhost:8182/gremlin")
            iam_auth = config.get("iam_auth", False)
            region = config.get("region")

            if iam_auth and region:
                # IAM auth requires request signing; use the AWS SigV4 transport
                try:
                    from gremlin_python.driver.aiohttp.transport import AiohttpTransport  # type: ignore[import-untyped]
                    import boto3  # type: ignore[import-untyped]
                    from botocore.auth import SigV4Auth  # type: ignore[import-untyped]
                    from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
                    from botocore.credentials import Credentials  # type: ignore[import-untyped]
                except ImportError:
                    pass  # Fall through to plain connection

            self._client = gremlin_client.Client(uri, "g")
        except Exception as exc:
            raise ConnectionError(f"Neptune/Gremlin: failed to connect — {exc}") from exc

    def _submit(self, query: str) -> list[Any]:
        """Submit a Gremlin query and return the result list."""
        result_set = self._client.submit(query)
        return result_set.all().result()

    @staticmethod
    def _normalize_gremlin_vertex(vertex: Any) -> dict[str, Any] | None:
        """Attempt to normalize a Gremlin vertex dict to our node format."""
        if not isinstance(vertex, dict):
            return None
        # Gremlin vertex maps have 'id', 'label', and optionally 'properties'
        vid = vertex.get("id", "")
        label = vertex.get("label", "")
        raw_props = vertex.get("properties", {})
        # Properties in Gremlin are lists of value maps: {"key": [{"id":..., "value":...}]}
        properties: dict[str, Any] = {}
        if isinstance(raw_props, dict):
            for k, v in raw_props.items():
                if isinstance(v, list) and v:
                    properties[k] = v[0].get("value", v[0]) if isinstance(v[0], dict) else v[0]
                else:
                    properties[k] = v
        return {
            "id": str(vid),
            "labels": [label] if label else [],
            "properties": properties,
        }

    @staticmethod
    def _normalize_gremlin_edge(edge: Any) -> dict[str, Any] | None:
        """Attempt to normalize a Gremlin edge dict to our relationship format."""
        if not isinstance(edge, dict):
            return None
        eid = edge.get("id", "")
        label = edge.get("label", "")
        out_v = edge.get("outV", edge.get("outVLabel", ""))
        in_v = edge.get("inV", edge.get("inVLabel", ""))
        properties = {k: v for k, v in edge.items()
                      if k not in ("id", "label", "outV", "inV", "outVLabel", "inVLabel", "type")}
        return {
            "id": str(eid),
            "type": label,
            "source": str(out_v),
            "target": str(in_v),
            "properties": properties,
        }

    def _normalize_results(self, raw: list[Any]) -> GraphQueryResult:
        """Best-effort normalization of Gremlin results to GraphQueryResult."""
        nodes: list[dict[str, Any]] = []
        relationships: list[dict[str, Any]] = []

        def _process(item: Any) -> None:
            if isinstance(item, dict):
                item_type = item.get("type", "")
                if item_type == "vertex" or "label" in item and "outV" not in item and "inV" not in item:
                    node = self._normalize_gremlin_vertex(item)
                    if node:
                        nodes.append(node)
                elif item_type == "edge" or ("outV" in item or "inV" in item):
                    rel = self._normalize_gremlin_edge(item)
                    if rel:
                        relationships.append(rel)
                else:
                    # Could be a path or valueMap — try to extract nested items
                    for v in item.values():
                        if isinstance(v, (list, dict)):
                            _process(v)
            elif isinstance(item, list):
                for sub in item:
                    _process(sub)
            elif isinstance(item, str):
                # Plain string result (e.g. from label queries) — skip here
                pass

        for item in raw:
            _process(item)

        return GraphQueryResult(nodes=nodes, relationships=relationships)

    def execute_query(self, query: str) -> GraphQueryResult:
        self._ensure_connected()
        raw = self._submit(query)
        return self._normalize_results(raw)

    def list_node_labels(self) -> list[str]:
        self._ensure_connected()
        raw = self._submit("g.V().label().dedup().toList()")
        return [str(item) for item in raw if isinstance(item, str)]

    def list_relationship_types(self) -> list[str]:
        self._ensure_connected()
        raw = self._submit("g.E().label().dedup().toList()")
        return [str(item) for item in raw if isinstance(item, str)]

    def get_properties(self, label_or_type: str) -> SchemaProperties:
        self._ensure_connected()
        raw = self._submit(
            f"g.V().hasLabel('{label_or_type}').limit(1).valueMap(true).toList()"
        )
        properties: list[dict[str, Any]] = []
        if raw and isinstance(raw[0], dict):
            for k, v in raw[0].items():
                if k in ("id", "label", "T.id", "T.label"):
                    continue
                properties.append({"name": str(k), "type": type(v).__name__})
        return SchemaProperties(label_or_type=label_or_type, properties=properties)

    def traverse(self, start_node_id: str, depth: int = 3) -> GraphTraversalResult:
        self._ensure_connected()
        raw = self._submit(
            f"g.V('{start_node_id}').repeat(both()).times({depth}).path().toList()"
        )
        result = self._normalize_results(raw)
        return GraphTraversalResult(
            start_node_id=str(start_node_id),
            depth=depth,
            nodes=result.nodes,
            relationships=result.relationships,
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_BACKEND_MAP: dict[str, type[GraphBackendAdapter]] = {
    "neo4j": Neo4jAdapter,
    "neptune-gremlin": NeptuneGremlinAdapter,
}


def create_adapter(backend_type: str, config: dict) -> GraphBackendAdapter:
    """Create the appropriate graph backend adapter.

    Args:
        backend_type: One of the supported backend identifiers.
        config: Backend-specific connection configuration.

    Returns:
        An instance of the requested GraphBackendAdapter subclass.

    Raises:
        UnsupportedBackendError: If *backend_type* is not in the supported set.
    """
    adapter_cls = _BACKEND_MAP.get(backend_type)
    if adapter_cls is None:
        raise UnsupportedBackendError(backend_type)
    return adapter_cls()
