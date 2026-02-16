"""
Configuration module — loads settings from .env file.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class SharePointConfig:
    client_id: str = field(default_factory=lambda: os.getenv("AZURE_CLIENT_ID", ""))
    client_secret: str = field(
        default_factory=lambda: os.getenv("AZURE_CLIENT_SECRET", "")
    )
    tenant_id: str = field(default_factory=lambda: os.getenv("AZURE_TENANT_ID", ""))
    site_name: str = field(
        default_factory=lambda: os.getenv("SHAREPOINT_SITE_NAME", "ResumeScreener")
    )
    domain: str = field(
        default_factory=lambda: os.getenv(
            "SHAREPOINT_SITE_DOMAIN", "si2techvad.sharepoint.com"
        )
    )
    folder_path: str = field(
        default_factory=lambda: os.getenv("SHAREPOINT_JD_FOLDER", "JobDescriptions")
    )

    @property
    def authority(self) -> str:
        return f"https://login.microsoftonline.com/{self.tenant_id}"

    @property
    def scope(self) -> list[str]:
        return ["https://graph.microsoft.com/.default"]


@dataclass
class PineconeConfig:
    api_key: str = field(default_factory=lambda: os.getenv("PINECONE_API_KEY", ""))
    index_name: str = field(
        default_factory=lambda: os.getenv("PINECONE_INDEX_NAME", "job-descriptions")
    )
    cloud: str = field(default_factory=lambda: os.getenv("PINECONE_CLOUD", "aws"))
    region: str = field(
        default_factory=lambda: os.getenv("PINECONE_REGION", "us-east-1")
    )


@dataclass
class EmbeddingConfig:
    model_name: str = field(
        default_factory=lambda: os.getenv(
            "EMBEDDING_MODEL_NAME", "sentence-transformers/all-mpnet-base-v2"
        )
    )
    dimension: int = field(
        default_factory=lambda: int(os.getenv("EMBEDDING_DIMENSION", "768"))
    )


@dataclass
class AppConfig:
    sharepoint: SharePointConfig = field(default_factory=SharePointConfig)
    pinecone: PineconeConfig = field(default_factory=PineconeConfig)
    embedding: EmbeddingConfig = field(default_factory=EmbeddingConfig)


def get_config() -> AppConfig:
    """Returns the application configuration."""
    config = AppConfig()

    # Validate critical fields
    missing = []
    if not config.sharepoint.client_id:
        missing.append("AZURE_CLIENT_ID")
    if not config.sharepoint.client_secret:
        missing.append("AZURE_CLIENT_SECRET")
    if not config.sharepoint.tenant_id:
        missing.append("AZURE_TENANT_ID")
    if not config.pinecone.api_key:
        missing.append("PINECONE_API_KEY")

    if missing:
        print(f"⚠️  Warning: Missing environment variables: {', '.join(missing)}")
        print("   Copy .env.example → .env and fill in your credentials.\n")

    return config
