"""
Pinecone client — handles connection, upserting JD vectors, and similarity search.
Uses the Pinecone Python SDK v5+ (serverless).
"""

import hashlib
from pinecone import Pinecone, ServerlessSpec

from config import PineconeConfig
from jd_parser import JDMetadata
from embedding_service import EmbeddingService


class PineconeClient:
    """Manages Pinecone index operations for Job Descriptions."""

    def __init__(self, config: PineconeConfig, embedding_service: EmbeddingService):
        self.config = config
        self.embedding_service = embedding_service
        self._pc = Pinecone(api_key=config.api_key)
        self._index = None

    # ------------------------------------------------------------------
    # Index management
    # ------------------------------------------------------------------
    def ensure_index_exists(self, dimension: int) -> None:
        """
        Check if the index exists; if not, provide instructions.
        We assume the user creates the index manually (as requested).
        """
        existing = [idx.name for idx in self._pc.list_indexes()]

        if self.config.index_name in existing:
            print(f"✅ Pinecone index '{self.config.index_name}' found.")
        else:
            print(
                f"❌ Index '{self.config.index_name}' not found in your Pinecone account."
            )
            print(f"   Existing indexes: {existing or 'None'}")
            print(f"\n   👉 Please create it in the Pinecone console with:")
            print(f"      • Name:      {self.config.index_name}")
            print(f"      • Dimension: {dimension}")
            print(f"      • Metric:    cosine")
            print(f"      • Cloud:     {self.config.cloud}")
            print(f"      • Region:    {self.config.region}")
            raise RuntimeError(
                f"Index '{self.config.index_name}' does not exist. Create it first."
            )

    @property
    def index(self):
        """Get a reference to the Pinecone index."""
        if self._index is None:
            self._index = self._pc.Index(self.config.index_name)
        return self._index

    # ------------------------------------------------------------------
    # Upsert JDs
    # ------------------------------------------------------------------
    @staticmethod
    def _generate_vector_id(filename: str) -> str:
        """Generate a deterministic ID from the filename (for idempotent upserts)."""
        return f"jd_{hashlib.md5(filename.encode()).hexdigest()[:16]}"

    def upsert_jds(
        self, jd_metadata_list: list[JDMetadata], batch_size: int = 50
    ) -> int:
        """
        Embed and upsert JD documents into Pinecone.

        Each JD is stored as a single vector with its full text embedded
        and structured metadata attached.

        Args:
            jd_metadata_list: Parsed JD metadata objects.
            batch_size: Number of vectors per upsert batch.

        Returns:
            Number of vectors upserted.
        """
        if not jd_metadata_list:
            print("⚠️  No JDs to upsert.")
            return 0

        print(f"📐 Generating embeddings for {len(jd_metadata_list)} JD(s)...")

        # Prepare texts for embedding — use full raw text for best semantic matching
        texts = [jd.raw_text for jd in jd_metadata_list]
        embeddings = self.embedding_service.embed_texts(texts)

        # Build upsert vectors
        vectors = []
        for jd, embedding in zip(jd_metadata_list, embeddings):
            vector_id = self._generate_vector_id(jd.filename)
            metadata = jd.to_pinecone_metadata()
            # Also store a truncated version of raw text for retrieval context
            metadata["text_preview"] = jd.raw_text[
                :1000
            ]  # Pinecone metadata limit ~40KB
            vectors.append(
                {
                    "id": vector_id,
                    "values": embedding,
                    "metadata": metadata,
                }
            )

        # Batch upsert
        total_upserted = 0
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i : i + batch_size]
            self.index.upsert(vectors=batch)
            total_upserted += len(batch)
            print(f"   📤 Upserted batch {i // batch_size + 1}: {len(batch)} vector(s)")

        print(
            f"\n✅ Total upserted: {total_upserted} vector(s) into '{self.config.index_name}'."
        )

        # Show index stats
        stats = self.index.describe_index_stats()
        print(f"📊 Index stats: {stats.total_vector_count} total vector(s) in index.\n")

        return total_upserted

    # ------------------------------------------------------------------
    # Query (for resume matching — will be expanded later)
    # ------------------------------------------------------------------
    def query_similar(self, query_text: str, top_k: int = 5) -> list[dict]:
        """
        Query the index with a text (e.g., resume content) and return
        the top-k most similar JDs.

        Args:
            query_text: The text to search with (resume content).
            top_k: Number of results to return.

        Returns:
            List of dicts with keys: id, score, metadata
        """
        query_embedding = self.embedding_service.embed_text(query_text)

        results = self.index.query(
            vector=query_embedding,
            top_k=top_k,
            include_metadata=True,
        )

        matches = []
        for match in results.get("matches", []):
            matches.append(
                {
                    "id": match["id"],
                    "score": round(match["score"], 4),
                    "metadata": match.get("metadata", {}),
                }
            )

        return matches
