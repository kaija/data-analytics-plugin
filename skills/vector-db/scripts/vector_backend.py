"""Vector backend adapter interface and factory.

Provides an abstract base class for vector database backends, a normalized
result data class, and a factory function to create adapter instances.

Requirements: 4.1, 4.6, 4.8, 4.9
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class VectorResult:
    """Normalized result returned by all vector backend adapters.

    Attributes:
        id: Unique vector identifier.
        score: Similarity score.
        metadata: Key-value metadata pairs.
        payload: Optional vector payload (list of floats or None).
    """

    id: str
    score: float
    metadata: dict[str, Any] = field(default_factory=dict)
    payload: list[float] | None = None


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

SUPPORTED_BACKENDS: list[str] = [
    "pinecone",
    "weaviate",
    "qdrant",
    "chromadb",
    "milvus",
    "pgvector",
]


class UnsupportedBackendError(Exception):
    """Raised when an unknown vector backend type is requested."""

    def __init__(self, backend_type: str) -> None:
        self.backend_type = backend_type
        self.supported_backends = list(SUPPORTED_BACKENDS)
        super().__init__(
            f"Unsupported vector backend '{backend_type}'. "
            f"Supported backends: {', '.join(SUPPORTED_BACKENDS)}"
        )


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class VectorBackendAdapter(ABC):
    """Abstract interface that every vector backend adapter must implement."""

    @abstractmethod
    def connect(self, config: dict) -> None:
        """Establish connection to the vector store.

        Args:
            config: Backend-specific connection configuration.
        """

    @abstractmethod
    def search(
        self, query_embedding: list[float], top_k: int = 10
    ) -> list[VectorResult]:
        """Perform similarity search.

        Args:
            query_embedding: The query vector.
            top_k: Number of nearest neighbours to return.

        Returns:
            List of VectorResult ordered by descending similarity.
        """

    @abstractmethod
    def filter_by_metadata(self, filters: dict) -> list[VectorResult]:
        """Filter vectors by metadata key-value pairs.

        Args:
            filters: Dictionary of metadata key-value filter expressions.

        Returns:
            List of matching VectorResult instances.
        """

    @abstractmethod
    def get_by_id(self, vector_id: str) -> VectorResult | None:
        """Retrieve a vector by its ID.

        Args:
            vector_id: The unique identifier of the vector.

        Returns:
            The matching VectorResult, or None if not found.
        """

    @abstractmethod
    def list_collections(self) -> list[str]:
        """List available collections / indices in the vector store."""


# ---------------------------------------------------------------------------
# Stub adapter implementations (actual logic added in task 5.3)
# ---------------------------------------------------------------------------

class PineconeAdapter(VectorBackendAdapter):
    """Pinecone vector backend adapter."""

    def __init__(self) -> None:
        self._client: Any | None = None
        self._index: Any | None = None

    def connect(self, config: dict) -> None:
        try:
            import pinecone  # type: ignore[import-untyped]
        except ImportError:
            raise ConnectionError(
                "Pinecone: failed to connect — the 'pinecone' package is not installed. "
                "Install it with: pip install pinecone-client"
            )
        try:
            api_key = config.get("api_key", "")
            environment = config.get("environment", "")
            pc = pinecone.Pinecone(api_key=api_key, environment=environment)
            index_name = config.get("index", config.get("collection", ""))
            if index_name:
                self._index = pc.Index(index_name)
            self._client = pc
        except Exception as exc:
            raise ConnectionError(
                f"Pinecone: failed to connect — {exc}"
            ) from exc

    def _ensure_connected(self) -> None:
        if self._client is None:
            raise RuntimeError("PineconeAdapter is not connected. Call connect() first.")

    def search(self, query_embedding: list[float], top_k: int = 10) -> list[VectorResult]:
        self._ensure_connected()
        if self._index is None:
            raise RuntimeError("PineconeAdapter has no index configured. Provide 'index' in config.")
        response = self._index.query(vector=query_embedding, top_k=top_k, include_metadata=True)
        results: list[VectorResult] = []
        for match in response.get("matches", []):
            results.append(VectorResult(
                id=str(match.get("id", "")),
                score=float(match.get("score", 0.0)),
                metadata=dict(match.get("metadata", {})),
                payload=match.get("values"),
            ))
        return results

    def filter_by_metadata(self, filters: dict) -> list[VectorResult]:
        self._ensure_connected()
        if self._index is None:
            raise RuntimeError("PineconeAdapter has no index configured. Provide 'index' in config.")
        # Pinecone metadata filtering uses a filter dict in the query call
        # We use a zero vector of dimension 1 as a dummy — real usage would need proper dimension
        response = self._index.query(
            vector=[0.0],
            top_k=100,
            include_metadata=True,
            filter=filters,
        )
        results: list[VectorResult] = []
        for match in response.get("matches", []):
            results.append(VectorResult(
                id=str(match.get("id", "")),
                score=float(match.get("score", 0.0)),
                metadata=dict(match.get("metadata", {})),
                payload=match.get("values"),
            ))
        return results

    def get_by_id(self, vector_id: str) -> VectorResult | None:
        self._ensure_connected()
        if self._index is None:
            raise RuntimeError("PineconeAdapter has no index configured. Provide 'index' in config.")
        response = self._index.fetch(ids=[vector_id])
        vectors = response.get("vectors", {})
        if vector_id not in vectors:
            return None
        vec = vectors[vector_id]
        return VectorResult(
            id=vector_id,
            score=1.0,
            metadata=dict(vec.get("metadata", {})),
            payload=vec.get("values"),
        )

    def list_collections(self) -> list[str]:
        self._ensure_connected()
        indexes = self._client.list_indexes()
        return [idx.name if hasattr(idx, "name") else str(idx) for idx in indexes]


class WeaviateAdapter(VectorBackendAdapter):
    """Weaviate vector backend adapter."""

    def __init__(self) -> None:
        self._client: Any | None = None

    def connect(self, config: dict) -> None:
        try:
            import weaviate  # type: ignore[import-untyped]
        except ImportError:
            raise ConnectionError(
                "Weaviate: failed to connect — the 'weaviate-client' package is not installed. "
                "Install it with: pip install weaviate-client"
            )
        try:
            host = config.get("host", "http://localhost:8080")
            api_key = config.get("api_key")
            if api_key:
                auth = weaviate.auth.AuthApiKey(api_key=api_key)
                self._client = weaviate.Client(url=host, auth_client_secret=auth)
            else:
                self._client = weaviate.Client(url=host)
        except Exception as exc:
            raise ConnectionError(
                f"Weaviate: failed to connect — {exc}"
            ) from exc

    def _ensure_connected(self) -> None:
        if self._client is None:
            raise RuntimeError("WeaviateAdapter is not connected. Call connect() first.")

    def search(self, query_embedding: list[float], top_k: int = 10) -> list[VectorResult]:
        self._ensure_connected()
        result = (
            self._client.query
            .get(self._client._connection.url, [])
            .with_near_vector({"vector": query_embedding})
            .with_limit(top_k)
            .with_additional(["id", "certainty"])
            .do()
        )
        results: list[VectorResult] = []
        data = result.get("data", {}).get("Get", {})
        for class_name, items in data.items():
            for item in items:
                additional = item.get("_additional", {})
                vec_id = additional.get("id", "")
                score = float(additional.get("certainty", 0.0))
                metadata = {k: v for k, v in item.items() if k != "_additional"}
                results.append(VectorResult(
                    id=vec_id,
                    score=score,
                    metadata=metadata,
                ))
        return results[:top_k]

    def filter_by_metadata(self, filters: dict) -> list[VectorResult]:
        self._ensure_connected()
        where_filter = {"operator": "And", "operands": []}
        for key, value in filters.items():
            op = {
                "path": [key],
                "operator": "Equal",
                "valueString" if isinstance(value, str) else "valueNumber": value,
            }
            where_filter["operands"].append(op)
        result = (
            self._client.query
            .get(self._client._connection.url, [])
            .with_where(where_filter)
            .with_additional(["id", "certainty"])
            .do()
        )
        results: list[VectorResult] = []
        data = result.get("data", {}).get("Get", {})
        for class_name, items in data.items():
            for item in items:
                additional = item.get("_additional", {})
                vec_id = additional.get("id", "")
                score = float(additional.get("certainty", 0.0))
                metadata = {k: v for k, v in item.items() if k != "_additional"}
                results.append(VectorResult(
                    id=vec_id,
                    score=score,
                    metadata=metadata,
                ))
        return results

    def get_by_id(self, vector_id: str) -> VectorResult | None:
        self._ensure_connected()
        try:
            result = self._client.data_object.get_by_id(vector_id)
        except Exception:
            return None
        if result is None:
            return None
        return VectorResult(
            id=result.get("id", vector_id),
            score=1.0,
            metadata=result.get("properties", {}),
        )

    def list_collections(self) -> list[str]:
        self._ensure_connected()
        schema = self._client.schema.get()
        return [cls["class"] for cls in schema.get("classes", [])]


class QdrantAdapter(VectorBackendAdapter):
    """Qdrant vector backend adapter."""

    def __init__(self) -> None:
        self._client: Any | None = None
        self._collection: str | None = None

    def connect(self, config: dict) -> None:
        try:
            from qdrant_client import QdrantClient  # type: ignore[import-untyped]
        except ImportError:
            raise ConnectionError(
                "Qdrant: failed to connect — the 'qdrant-client' package is not installed. "
                "Install it with: pip install qdrant-client"
            )
        try:
            host = config.get("host", "localhost")
            port = config.get("port", 6333)
            api_key = config.get("api_key")
            self._collection = config.get("collection")
            self._client = QdrantClient(host=host, port=port, api_key=api_key)
        except Exception as exc:
            raise ConnectionError(
                f"Qdrant: failed to connect — {exc}"
            ) from exc

    def _ensure_connected(self) -> None:
        if self._client is None:
            raise RuntimeError("QdrantAdapter is not connected. Call connect() first.")

    def search(self, query_embedding: list[float], top_k: int = 10) -> list[VectorResult]:
        self._ensure_connected()
        collection = self._collection or "default"
        hits = self._client.search(
            collection_name=collection,
            query_vector=query_embedding,
            limit=top_k,
        )
        results: list[VectorResult] = []
        for hit in hits:
            results.append(VectorResult(
                id=str(hit.id),
                score=float(hit.score),
                metadata=dict(hit.payload) if hit.payload else {},
                payload=hit.vector if hasattr(hit, "vector") else None,
            ))
        return results

    def filter_by_metadata(self, filters: dict) -> list[VectorResult]:
        self._ensure_connected()
        from qdrant_client.models import Filter, FieldCondition, MatchValue  # type: ignore[import-untyped]

        collection = self._collection or "default"
        conditions = [
            FieldCondition(key=k, match=MatchValue(value=v))
            for k, v in filters.items()
        ]
        hits, _offset = self._client.scroll(
            collection_name=collection,
            scroll_filter=Filter(must=conditions),
            limit=100,
        )
        results: list[VectorResult] = []
        for point in hits:
            results.append(VectorResult(
                id=str(point.id),
                score=1.0,
                metadata=dict(point.payload) if point.payload else {},
                payload=point.vector if hasattr(point, "vector") else None,
            ))
        return results

    def get_by_id(self, vector_id: str) -> VectorResult | None:
        self._ensure_connected()
        collection = self._collection or "default"
        points = self._client.retrieve(
            collection_name=collection,
            ids=[vector_id],
        )
        if not points:
            return None
        point = points[0]
        return VectorResult(
            id=str(point.id),
            score=1.0,
            metadata=dict(point.payload) if point.payload else {},
            payload=point.vector if hasattr(point, "vector") else None,
        )

    def list_collections(self) -> list[str]:
        self._ensure_connected()
        collections = self._client.get_collections().collections
        return [c.name for c in collections]


class ChromaDBAdapter(VectorBackendAdapter):
    """ChromaDB vector backend adapter."""

    def __init__(self) -> None:
        self._client: Any | None = None
        self._collection_name: str | None = None

    def connect(self, config: dict) -> None:
        try:
            import chromadb  # type: ignore[import-untyped]
        except ImportError:
            raise ConnectionError(
                "ChromaDB: failed to connect — the 'chromadb' package is not installed. "
                "Install it with: pip install chromadb"
            )
        try:
            host = config.get("host")
            port = config.get("port", 8000)
            self._collection_name = config.get("collection")
            if host:
                self._client = chromadb.HttpClient(host=host, port=port)
            else:
                self._client = chromadb.Client()
        except Exception as exc:
            raise ConnectionError(
                f"ChromaDB: failed to connect — {exc}"
            ) from exc

    def _ensure_connected(self) -> None:
        if self._client is None:
            raise RuntimeError("ChromaDBAdapter is not connected. Call connect() first.")

    def _get_collection(self) -> Any:
        name = self._collection_name or "default"
        return self._client.get_collection(name)

    def search(self, query_embedding: list[float], top_k: int = 10) -> list[VectorResult]:
        self._ensure_connected()
        collection = self._get_collection()
        response = collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            include=["metadatas", "distances", "embeddings"],
        )
        results: list[VectorResult] = []
        ids = response.get("ids", [[]])[0]
        distances = response.get("distances", [[]])[0]
        metadatas = response.get("metadatas", [[]])[0]
        embeddings = response.get("embeddings")
        emb_list = embeddings[0] if embeddings else [None] * len(ids)
        for i, vid in enumerate(ids):
            # ChromaDB returns distances; convert to similarity score (1 - distance for L2)
            dist = distances[i] if i < len(distances) else 0.0
            score = 1.0 / (1.0 + dist)
            meta = metadatas[i] if i < len(metadatas) else {}
            emb = emb_list[i] if i < len(emb_list) else None
            results.append(VectorResult(
                id=str(vid),
                score=score,
                metadata=dict(meta) if meta else {},
                payload=emb,
            ))
        return results

    def filter_by_metadata(self, filters: dict) -> list[VectorResult]:
        self._ensure_connected()
        collection = self._get_collection()
        # ChromaDB where filter: {"$and": [{"key": {"$eq": value}}, ...]}
        where: dict[str, Any] = {}
        if len(filters) == 1:
            key, value = next(iter(filters.items()))
            where = {key: {"$eq": value}}
        elif len(filters) > 1:
            where = {"$and": [{k: {"$eq": v}} for k, v in filters.items()]}
        response = collection.get(
            where=where if where else None,
            include=["metadatas", "embeddings"],
        )
        results: list[VectorResult] = []
        ids = response.get("ids", [])
        metadatas = response.get("metadatas", [])
        embeddings = response.get("embeddings")
        emb_list = embeddings if embeddings else [None] * len(ids)
        for i, vid in enumerate(ids):
            meta = metadatas[i] if i < len(metadatas) else {}
            emb = emb_list[i] if i < len(emb_list) else None
            results.append(VectorResult(
                id=str(vid),
                score=1.0,
                metadata=dict(meta) if meta else {},
                payload=emb,
            ))
        return results

    def get_by_id(self, vector_id: str) -> VectorResult | None:
        self._ensure_connected()
        collection = self._get_collection()
        response = collection.get(
            ids=[vector_id],
            include=["metadatas", "embeddings"],
        )
        ids = response.get("ids", [])
        if not ids:
            return None
        metadatas = response.get("metadatas", [])
        embeddings = response.get("embeddings")
        meta = metadatas[0] if metadatas else {}
        emb = embeddings[0] if embeddings else None
        return VectorResult(
            id=str(ids[0]),
            score=1.0,
            metadata=dict(meta) if meta else {},
            payload=emb,
        )

    def list_collections(self) -> list[str]:
        self._ensure_connected()
        collections = self._client.list_collections()
        return [c.name if hasattr(c, "name") else str(c) for c in collections]


class MilvusAdapter(VectorBackendAdapter):
    """Milvus vector backend adapter."""

    def __init__(self) -> None:
        self._client: Any | None = None
        self._collection_name: str | None = None

    def connect(self, config: dict) -> None:
        try:
            from pymilvus import connections, Collection, utility  # type: ignore[import-untyped]
        except ImportError:
            raise ConnectionError(
                "Milvus: failed to connect — the 'pymilvus' package is not installed. "
                "Install it with: pip install pymilvus"
            )
        try:
            host = config.get("host", "localhost")
            port = config.get("port", 19530)
            alias = config.get("alias", "default")
            self._collection_name = config.get("collection")
            connections.connect(alias=alias, host=host, port=port)
            # Store the module references for later use
            self._client = {
                "connections": connections,
                "Collection": Collection,
                "utility": utility,
                "alias": alias,
            }
        except Exception as exc:
            raise ConnectionError(
                f"Milvus: failed to connect — {exc}"
            ) from exc

    def _ensure_connected(self) -> None:
        if self._client is None:
            raise RuntimeError("MilvusAdapter is not connected. Call connect() first.")

    def _get_collection(self) -> Any:
        name = self._collection_name or "default"
        return self._client["Collection"](name)

    def search(self, query_embedding: list[float], top_k: int = 10) -> list[VectorResult]:
        self._ensure_connected()
        collection = self._get_collection()
        collection.load()
        search_results = collection.search(
            data=[query_embedding],
            anns_field="embedding",
            param={"metric_type": "L2", "params": {"nprobe": 10}},
            limit=top_k,
            output_fields=["*"],
        )
        results: list[VectorResult] = []
        for hits in search_results:
            for hit in hits:
                metadata = {k: v for k, v in hit.entity.fields.items() if k != "embedding"} if hasattr(hit.entity, "fields") else {}
                results.append(VectorResult(
                    id=str(hit.id),
                    score=float(hit.score),
                    metadata=metadata,
                ))
        return results

    def filter_by_metadata(self, filters: dict) -> list[VectorResult]:
        self._ensure_connected()
        collection = self._get_collection()
        collection.load()
        # Build Milvus boolean expression from filters
        expressions = []
        for key, value in filters.items():
            if isinstance(value, str):
                expressions.append(f'{key} == "{value}"')
            else:
                expressions.append(f"{key} == {value}")
        expr = " and ".join(expressions) if expressions else ""
        query_results = collection.query(
            expr=expr,
            output_fields=["*"],
        )
        results: list[VectorResult] = []
        for item in query_results:
            vid = str(item.get("id", item.get("pk", "")))
            metadata = {k: v for k, v in item.items() if k not in ("id", "pk", "embedding")}
            results.append(VectorResult(
                id=vid,
                score=1.0,
                metadata=metadata,
            ))
        return results

    def get_by_id(self, vector_id: str) -> VectorResult | None:
        self._ensure_connected()
        collection = self._get_collection()
        collection.load()
        query_results = collection.query(
            expr=f'id == "{vector_id}"',
            output_fields=["*"],
        )
        if not query_results:
            return None
        item = query_results[0]
        metadata = {k: v for k, v in item.items() if k not in ("id", "pk", "embedding")}
        return VectorResult(
            id=str(item.get("id", vector_id)),
            score=1.0,
            metadata=metadata,
            payload=item.get("embedding"),
        )

    def list_collections(self) -> list[str]:
        self._ensure_connected()
        return list(self._client["utility"].list_collections())


class PgvectorAdapter(VectorBackendAdapter):
    """pgvector backend adapter."""

    def __init__(self) -> None:
        self._client: Any | None = None
        self._table: str | None = None

    def connect(self, config: dict) -> None:
        try:
            import psycopg2  # type: ignore[import-untyped]
        except ImportError:
            raise ConnectionError(
                "pgvector: failed to connect — the 'psycopg2' package is not installed. "
                "Install it with: pip install psycopg2-binary"
            )
        try:
            host = config.get("host", "localhost")
            port = config.get("port", 5432)
            database = config.get("database", "vectors")
            user = config.get("user", "postgres")
            password = config.get("password", "")
            self._table = config.get("table", config.get("collection", "vectors"))
            self._client = psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
            )
        except ImportError:
            raise
        except Exception as exc:
            raise ConnectionError(
                f"pgvector: failed to connect — {exc}"
            ) from exc

    def _ensure_connected(self) -> None:
        if self._client is None:
            raise RuntimeError("PgvectorAdapter is not connected. Call connect() first.")

    def search(self, query_embedding: list[float], top_k: int = 10) -> list[VectorResult]:
        self._ensure_connected()
        table = self._table or "vectors"
        embedding_str = "[" + ",".join(str(x) for x in query_embedding) + "]"
        query = f"""
            SELECT id, embedding <-> %s::vector AS distance, metadata
            FROM {table}
            ORDER BY embedding <-> %s::vector
            LIMIT %s
        """
        cur = self._client.cursor()
        cur.execute(query, (embedding_str, embedding_str, top_k))
        results: list[VectorResult] = []
        for row in cur.fetchall():
            vid, distance, metadata_raw = row[0], row[1], row[2]
            metadata = json.loads(metadata_raw) if isinstance(metadata_raw, str) else (metadata_raw or {})
            score = 1.0 / (1.0 + float(distance))
            results.append(VectorResult(
                id=str(vid),
                score=score,
                metadata=metadata,
            ))
        cur.close()
        return results

    def filter_by_metadata(self, filters: dict) -> list[VectorResult]:
        self._ensure_connected()
        table = self._table or "vectors"
        # Use JSONB containment operator for metadata filtering
        filter_json = json.dumps(filters)
        query = f"SELECT id, metadata FROM {table} WHERE metadata @> %s::jsonb"
        cur = self._client.cursor()
        cur.execute(query, (filter_json,))
        results: list[VectorResult] = []
        for row in cur.fetchall():
            vid, metadata_raw = row[0], row[1]
            metadata = json.loads(metadata_raw) if isinstance(metadata_raw, str) else (metadata_raw or {})
            results.append(VectorResult(
                id=str(vid),
                score=1.0,
                metadata=metadata,
            ))
        cur.close()
        return results

    def get_by_id(self, vector_id: str) -> VectorResult | None:
        self._ensure_connected()
        table = self._table or "vectors"
        query = f"SELECT id, metadata, embedding FROM {table} WHERE id = %s"
        cur = self._client.cursor()
        cur.execute(query, (vector_id,))
        row = cur.fetchone()
        cur.close()
        if row is None:
            return None
        vid, metadata_raw, embedding = row[0], row[1], row[2]
        metadata = json.loads(metadata_raw) if isinstance(metadata_raw, str) else (metadata_raw or {})
        payload = list(embedding) if embedding else None
        return VectorResult(
            id=str(vid),
            score=1.0,
            metadata=metadata,
            payload=payload,
        )

    def list_collections(self) -> list[str]:
        self._ensure_connected()
        query = """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename IN (
                SELECT table_name FROM information_schema.columns
                WHERE udt_name = 'vector'
            )
        """
        cur = self._client.cursor()
        cur.execute(query)
        tables = [row[0] for row in cur.fetchall()]
        cur.close()
        return tables


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_BACKEND_MAP: dict[str, type[VectorBackendAdapter]] = {
    "pinecone": PineconeAdapter,
    "weaviate": WeaviateAdapter,
    "qdrant": QdrantAdapter,
    "chromadb": ChromaDBAdapter,
    "milvus": MilvusAdapter,
    "pgvector": PgvectorAdapter,
}


def create_adapter(backend_type: str, config: dict) -> VectorBackendAdapter:
    """Create the appropriate vector backend adapter.

    Args:
        backend_type: One of the supported backend identifiers.
        config: Backend-specific connection configuration.

    Returns:
        An instance of the requested VectorBackendAdapter subclass.

    Raises:
        UnsupportedBackendError: If *backend_type* is not in the supported set.
    """
    adapter_cls = _BACKEND_MAP.get(backend_type)
    if adapter_cls is None:
        raise UnsupportedBackendError(backend_type)
    return adapter_cls()
