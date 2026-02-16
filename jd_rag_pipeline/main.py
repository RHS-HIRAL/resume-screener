"""
Main Pipeline — Orchestrates the full JD ingestion workflow:

    1. Authenticate with SharePoint via Microsoft Graph API
    2. Download PDF job descriptions from the configured folder
    3. Extract text from each PDF
    4. Parse structured metadata (title, location, experience, etc.)
    5. Generate embeddings using HuggingFace (all-mpnet-base-v2)
    6. Upsert vectors + metadata into Pinecone

Usage:
    python main.py                  # Run full pipeline
    python main.py --query "..."    # Test similarity search with a text query
"""

import argparse
import sys
import json

from config import get_config
from sharepoint_client import SharePointClient
from pdf_extractor import extract_texts_from_pdfs
from jd_parser import parse_all_jds
from embedding_service import EmbeddingService
from pinecone_client import PineconeClient


def run_ingestion_pipeline():
    """Run the full JD ingestion pipeline."""
    print("=" * 65)
    print("  JD Ingestion Pipeline — SharePoint → Pinecone")
    print("=" * 65, "\n")

    # --- Load Config ---
    config = get_config()

    # --- Step 1: Download PDFs from SharePoint ---
    print("📂 STEP 1: Downloading JDs from SharePoint...\n")
    sp_client = SharePointClient(config.sharepoint, download_dir="./downloads")
    pdf_paths = sp_client.download_pdfs()

    if not pdf_paths:
        print("❌ No PDFs found. Exiting.")
        sys.exit(1)

    # --- Step 2: Extract text from PDFs ---
    print("\n📖 STEP 2: Extracting text from PDFs...\n")
    extracted_docs = extract_texts_from_pdfs(pdf_paths)

    # --- Step 3: Parse metadata ---
    print("\n🔍 STEP 3: Parsing JD metadata...\n")
    jd_metadata_list = parse_all_jds(extracted_docs)

    if not jd_metadata_list:
        print("❌ No valid JDs extracted. Exiting.")
        sys.exit(1)

    # Print a sample of parsed metadata
    print("\n📋 Sample parsed metadata (first JD):")
    sample = jd_metadata_list[0].to_pinecone_metadata()
    for key, value in sample.items():
        if key != "text_preview":
            display_val = (
                value[:100] + "..."
                if isinstance(value, str) and len(value) > 100
                else value
            )
            print(f"   {key:25s} → {display_val}")
    print()

    # --- Step 4: Embed and upsert into Pinecone ---
    print("📤 STEP 4: Embedding & upserting into Pinecone...\n")
    embedding_service = EmbeddingService(config.embedding)
    pc_client = PineconeClient(config.pinecone, embedding_service)
    pc_client.ensure_index_exists(dimension=config.embedding.dimension)
    count = pc_client.upsert_jds(jd_metadata_list)

    # --- Done ---
    print("=" * 65)
    print(f"  ✅ Pipeline complete! {count} JD(s) ingested into Pinecone.")
    print("=" * 65)


def run_query(query_text: str, top_k: int = 5):
    """Test similarity search against stored JDs."""
    print("=" * 65)
    print("  Similarity Search — Query vs. Stored JDs")
    print("=" * 65, "\n")

    config = get_config()
    embedding_service = EmbeddingService(config.embedding)
    pc_client = PineconeClient(config.pinecone, embedding_service)

    print(f"🔎 Querying with text ({len(query_text)} chars), top_k={top_k}...\n")
    results = pc_client.query_similar(query_text, top_k=top_k)

    if not results:
        print("No matching JDs found.")
        return

    for i, match in enumerate(results, 1):
        meta = match["metadata"]
        print(f"{'─' * 50}")
        print(f"  Match #{i}")
        print(f"  Score:       {match['score']}")
        print(f"  Filename:    {meta.get('filename', 'N/A')}")
        print(f"  Job Title:   {meta.get('job_title', 'N/A')}")
        print(f"  Location:    {meta.get('location', 'N/A')}")
        print(f"  Experience:  {meta.get('experience', 'N/A')}")
        print(f"  Department:  {meta.get('department', 'N/A')}")
        print(f"  Skills:      {meta.get('skills', 'N/A')[:100]}...")
    print(f"{'─' * 50}\n")


# ------------------------------------------------------------------
# CLI Entry Point
# ------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="JD RAG Pipeline — SharePoint to Pinecone"
    )
    parser.add_argument(
        "--query",
        "-q",
        type=str,
        default=None,
        help="Run a similarity search query against stored JDs.",
    )
    parser.add_argument(
        "--top-k",
        "-k",
        type=int,
        default=5,
        help="Number of top results to return (default: 5).",
    )
    args = parser.parse_args()

    if args.query:
        run_query(args.query, top_k=args.top_k)
    else:
        run_ingestion_pipeline()
