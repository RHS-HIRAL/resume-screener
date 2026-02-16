"""
Embedding module — generates vector embeddings using HuggingFace
sentence-transformers via LangChain.
"""

from langchain_huggingface import HuggingFaceEmbeddings
from config import EmbeddingConfig


class EmbeddingService:
    """Wrapper around LangChain's HuggingFace embeddings."""

    def __init__(self, config: EmbeddingConfig):
        self.config = config
        self._model: HuggingFaceEmbeddings | None = None

    @property
    def model(self) -> HuggingFaceEmbeddings:
        """Lazy-load the embedding model."""
        if self._model is None:
            print(f"🤖 Loading embedding model: {self.config.model_name}")
            self._model = HuggingFaceEmbeddings(
                model_name=self.config.model_name,
                model_kwargs={"device": "cpu"},
                encode_kwargs={
                    "normalize_embeddings": True,  # cosine similarity works better normalised
                    "batch_size": 32,
                },
            )
            print("✅ Embedding model loaded.\n")
        return self._model

    def embed_text(self, text: str) -> list[float]:
        """Embed a single text string."""
        return self.model.embed_query(text)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of text strings."""
        return self.model.embed_documents(texts)
