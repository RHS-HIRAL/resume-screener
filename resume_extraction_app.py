"""
resume_extraction_app.py
────────────────────────
Streamlit UI for batch resume extraction via Ollama.

Workflow:
  1. Authenticate with SharePoint and list job-role subfolders under Resumes/.
  2. User selects a JD folder → app lists all PDFs in it.
  3. User clicks "Extract All" → each PDF is downloaded, parsed by Ollama,
     and the structured JSON is saved to extracted_json_resumes/<role>/<file>.json.
  4. A _manifest.json aggregates metadata per role for quick downstream use.
"""

import json
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import streamlit as st

# ── Ensure project root is on sys.path so imports work ───────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from resume_screener_pipeline.pipeline import SharePointResumeFetcher
from ollama_resume_extractor import (
    extract_text_from_pdf,
    extract_text_from_docx,
    process_single,
    DEFAULT_MODEL,
    EXTRACTORS,
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════
RESULTS_DIR = PROJECT_ROOT / "extracted_json_resumes"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

TMP_DIR = PROJECT_ROOT / "tmp_resumes"
TMP_DIR.mkdir(parents=True, exist_ok=True)

SHAREPOINT_RESUME_BASE = os.getenv("SHAREPOINT_BASE_FOLDER", "Resumes")


# ═══════════════════════════════════════════════════════════════════════════════
# SHAREPOINT HELPERS  (thin wrappers around SharePointResumeFetcher)
# ═══════════════════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner="Authenticating with SharePoint…")
def get_sp_client() -> SharePointResumeFetcher:
    """Create and cache a SharePoint client for the session."""
    return SharePointResumeFetcher()


def list_job_role_folders(sp: SharePointResumeFetcher) -> list[dict]:
    """
    List subfolders under Resumes/ on SharePoint.
    Returns list of dicts with 'name' and 'id' keys for each subfolder.
    """
    items = sp._list_files(SHAREPOINT_RESUME_BASE)
    folders = [
        {"name": item["name"], "id": item.get("id", "")}
        for item in items
        if "folder" in item  # Graph API marks folders with a 'folder' key
    ]
    return sorted(folders, key=lambda f: f["name"])


def list_resumes_in_folder(sp: SharePointResumeFetcher, folder_name: str) -> list[dict]:
    """
    List PDF/DOCX files inside a specific job-role subfolder on SharePoint.
    Returns enriched file metadata dicts.
    """
    folder_path = f"{SHAREPOINT_RESUME_BASE}/{folder_name}"
    items = sp._list_files(folder_path)
    resume_files = []
    for item in items:
        if "file" not in item:
            continue
        name = item.get("name", "")
        if not name.lower().endswith((".pdf", ".docx")):
            continue
        resume_files.append({
            "name": name,
            "id": item.get("id", ""),
            "size": item.get("size", 0),
            "download_url": item.get("@microsoft.graph.downloadUrl", ""),
            "last_modified": item.get("lastModifiedDateTime", ""),
        })
    return sorted(resume_files, key=lambda f: f["name"])


def download_resume(sp: SharePointResumeFetcher, file_info: dict) -> Path:
    """Download a single resume PDF to the local tmp directory."""
    return sp._download_file(file_info["download_url"], file_info["name"])


# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT / MANIFEST HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def get_output_dir(role_name: str) -> Path:
    """Get or create the output directory for a given job role."""
    out = RESULTS_DIR / role_name
    out.mkdir(parents=True, exist_ok=True)
    return out


def get_output_path(role_name: str, filename: str) -> Path:
    """Get the JSON output path for a specific resume."""
    stem = Path(filename).stem
    return get_output_dir(role_name) / f"{stem}.json"


def is_already_extracted(role_name: str, filename: str) -> bool:
    """Check if a resume has already been extracted."""
    return get_output_path(role_name, filename).exists()


def save_extraction(role_name: str, filename: str, data: dict) -> Path:
    """Save a single extraction result as a JSON file."""
    out_path = get_output_path(role_name, filename)
    out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_path


def load_extraction(role_name: str, filename: str) -> dict | None:
    """Load a previously saved extraction result."""
    path = get_output_path(role_name, filename)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def save_manifest(role_name: str, file_results: list[dict], model: str, mode: str):
    """Write a _manifest.json summarising the extraction run."""
    out_dir = get_output_dir(role_name)
    manifest = {
        "job_role": role_name,
        "last_updated": datetime.now().isoformat(),
        "model_used": model,
        "extraction_mode": mode,
        "total_resumes": len(file_results),
        "successful": sum(1 for r in file_results if r["status"] == "success"),
        "failed": sum(1 for r in file_results if r["status"] == "failed"),
        "skipped": sum(1 for r in file_results if r["status"] == "skipped"),
        "files": file_results,
    }
    manifest_path = out_dir / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest


def load_manifest(role_name: str) -> dict | None:
    """Load an existing manifest for a role."""
    path = get_output_dir(role_name) / "_manifest.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMLIT UI
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(
        page_title="Resume Extractor",
        page_icon="📄",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # ── Custom CSS ────────────────────────────────────────────────────────
    st.markdown("""
    <style>
        /* Main header gradient */
        .main-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 1.5rem 2rem;
            border-radius: 12px;
            margin-bottom: 1.5rem;
        }
        .main-header h1 {
            color: #ffffff !important;
            margin: 0;
            font-size: 1.8rem;
        }
        .main-header p {
            color: rgba(255, 255, 255, 0.90) !important;
            margin: 0.3rem 0 0 0;
            font-size: 0.95rem;
        }

        /* Stats cards — theme-aware */
        .stat-card {
            background: rgba(102, 126, 234, 0.08);
            border: 1px solid rgba(102, 126, 234, 0.25);
            border-radius: 10px;
            padding: 1rem 1.2rem;
            text-align: center;
        }
        .stat-card .stat-number {
            font-size: 2rem;
            font-weight: 700;
            color: #667eea;
            margin: 0;
        }
        .stat-card .stat-label {
            font-size: 0.85rem;
            opacity: 0.75;
            margin: 0.2rem 0 0 0;
        }

        /* File list items — theme-aware */
        .file-item {
            border: 1px solid rgba(128, 128, 128, 0.25);
            border-radius: 8px;
            padding: 0.75rem 1rem;
            margin: 0.4rem 0;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        .file-item:hover {
            border-color: rgba(102, 126, 234, 0.5);
            box-shadow: 0 2px 8px rgba(102, 126, 234, 0.15);
        }
        .file-item .file-name {
            font-weight: 600;
        }
        .file-item .file-size {
            opacity: 0.6;
            margin-left: 0.8rem;
            font-size: 0.85rem;
        }

        /* Status badges */
        .badge-success {
            background: rgba(40, 167, 69, 0.15);
            color: #28a745;
            padding: 0.2rem 0.6rem;
            border-radius: 12px;
            font-size: 0.8rem;
            font-weight: 600;
            white-space: nowrap;
        }
        .badge-pending {
            background: rgba(255, 193, 7, 0.15);
            color: #e6a800;
            padding: 0.2rem 0.6rem;
            border-radius: 12px;
            font-size: 0.8rem;
            font-weight: 600;
            white-space: nowrap;
        }
        .badge-error {
            background: rgba(220, 53, 69, 0.15);
            color: #dc3545;
            padding: 0.2rem 0.6rem;
            border-radius: 12px;
            font-size: 0.8rem;
            font-weight: 600;
            white-space: nowrap;
        }
    </style>
    """, unsafe_allow_html=True)

    # ── Header ────────────────────────────────────────────────────────────
    st.markdown("""
    <div class="main-header">
        <h1>📄 Resume Extraction Pipeline</h1>
        <p>Extract structured candidate data from SharePoint resumes using Ollama LLM</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Sidebar ───────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ Configuration")

        # Model and mode selectors
        model = st.selectbox(
            "🤖 Ollama Model",
            options=["llama3.2", "gemma:7b"],
            index=0,
            help="Choose the Ollama model for extraction",
        )
        mode = st.selectbox(
            "🔧 Extraction Mode",
            options=list(EXTRACTORS.keys()),
            index=0,
            help="'structured' enforces JSON schema; 'function_call' uses tool-calling API",
        )

        st.divider()

        force_re_extract = st.toggle(
            "🔄 Force Re-extract",
            value=False,
            help="Overwrite existing JSON outputs",
        )

        st.divider()
        st.markdown("### 📂 SharePoint Connection")
        st.caption(f"Base folder: `{SHAREPOINT_RESUME_BASE}/`")

    # ── Initialise SharePoint client ──────────────────────────────────────
    try:
        sp = get_sp_client()
    except Exception as e:
        st.error(f"❌ Failed to connect to SharePoint: {e}")
        st.info("Check your `.env` file for correct Azure / SharePoint credentials.")
        st.stop()

    # ── Load job role folders ─────────────────────────────────────────────
    with st.spinner("Loading job role folders from SharePoint…"):
        try:
            folders = list_job_role_folders(sp)
        except Exception as e:
            st.error(f"❌ Could not list folders: {e}")
            st.stop()

    if not folders:
        st.warning("No subfolders found under `Resumes/` on SharePoint.")
        st.info("Expected structure: `Resumes/JD_<job-title>/` containing PDF résumés.")
        st.stop()

    # ── Folder selector ───────────────────────────────────────────────────
    folder_names = [f["name"] for f in folders]
    selected_folder = st.selectbox(
        "📁 Select Job Role Folder",
        options=folder_names,
        help="Choose a job-role folder from SharePoint to process",
    )

    if not selected_folder:
        st.stop()

    # ── List resumes in folder ────────────────────────────────────────────
    with st.spinner(f"Loading résumés from `{selected_folder}`…"):
        try:
            resume_files = list_resumes_in_folder(sp, selected_folder)
        except Exception as e:
            st.error(f"❌ Could not list files in `{selected_folder}`: {e}")
            st.stop()

    if not resume_files:
        st.warning(f"No PDF/DOCX files found in `{selected_folder}`.")
        st.stop()

    # ── Stats cards ───────────────────────────────────────────────────────
    total = len(resume_files)
    already_done = sum(1 for f in resume_files if is_already_extracted(selected_folder, f["name"]))
    pending = total - already_done

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number">{total}</div>
            <div class="stat-label">Total Résumés</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number">{already_done}</div>
            <div class="stat-label">Already Extracted</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number">{pending}</div>
            <div class="stat-label">Pending</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # ── File listing ──────────────────────────────────────────────────────
    st.subheader(f"📋 Résumés in `{selected_folder}`")

    for f in resume_files:
        size_kb = f["size"] / 1024
        extracted = is_already_extracted(selected_folder, f["name"])
        badge = (
            '<span class="badge-success">✅ Extracted</span>'
            if extracted
            else '<span class="badge-pending">⏳ Pending</span>'
        )
        st.markdown(f"""
        <div class="file-item">
            <div>
                <span class="file-name">📄 {f['name']}</span>
                <span class="file-size">{size_kb:.1f} KB</span>
            </div>
            <div>{badge}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # EXTRACTION
    # ══════════════════════════════════════════════════════════════════════
    files_to_process = (
        resume_files
        if force_re_extract
        else [f for f in resume_files if not is_already_extracted(selected_folder, f["name"])]
    )

    col_btn1, col_btn2 = st.columns([1, 1])

    with col_btn1:
        extract_clicked = st.button(
            f"🚀 Extract {'All' if force_re_extract else 'Pending'} ({len(files_to_process)} files)",
            type="primary",
            disabled=len(files_to_process) == 0,
            use_container_width=True,
        )

    with col_btn2:
        # Download combined JSON for already-extracted results
        all_results = []
        for f in resume_files:
            data = load_extraction(selected_folder, f["name"])
            if data:
                all_results.append(data)

        if all_results:
            combined_json = json.dumps(all_results, indent=2, ensure_ascii=False)
            st.download_button(
                label=f"⬇️ Download All Extracted ({len(all_results)} files)",
                data=combined_json,
                file_name=f"{selected_folder}_extractions.json",
                mime="application/json",
                use_container_width=True,
            )

    if extract_clicked and files_to_process:
        st.markdown("### 🔄 Extraction Progress")
        progress_bar = st.progress(0, text="Starting extraction…")
        status_container = st.container()

        file_results = []

        for idx, file_info in enumerate(files_to_process):
            fname = file_info["name"]
            progress = (idx + 1) / len(files_to_process)
            progress_bar.progress(progress, text=f"Processing {fname} ({idx+1}/{len(files_to_process)})")

            with status_container:
                with st.expander(f"📄 {fname}", expanded=(idx == len(files_to_process) - 1)):
                    try:
                        # Step 1: Download from SharePoint
                        st.caption("⬇️ Downloading from SharePoint…")
                        local_path = download_resume(sp, file_info)
                        st.caption(f"✅ Downloaded to `{local_path}`")

                        # Step 2: Extract via Ollama
                        st.caption(f"🧠 Extracting with `{model}` ({mode} mode)…")
                        start_time = time.time()
                        result = process_single(
                            str(local_path),
                            jd_text=None,
                            model=model,
                            mode=mode,
                        )
                        elapsed = time.time() - start_time

                        # Enrich result with additional metadata
                        result["_job_role"] = selected_folder
                        result["_sharepoint_item_id"] = file_info.get("id", "")
                        result["_extraction_model"] = model
                        result["_extraction_mode"] = mode
                        result["_extraction_duration_sec"] = round(elapsed, 2)

                        if "error" in result:
                            st.error(f"❌ Error: {result['error']}")
                            file_results.append({
                                "filename": fname,
                                "status": "failed",
                                "error": result["error"],
                                "extracted_at": datetime.now().isoformat(),
                                "output_file": None,
                            })
                        else:
                            # Step 3: Save JSON
                            out_path = save_extraction(selected_folder, fname, result)
                            st.success(f"✅ Extracted in {elapsed:.1f}s → `{out_path.name}`")
                            st.json(result)
                            file_results.append({
                                "filename": fname,
                                "status": "success",
                                "extracted_at": datetime.now().isoformat(),
                                "output_file": out_path.name,
                                "duration_sec": round(elapsed, 2),
                            })

                    except Exception as e:
                        st.error(f"❌ Exception: {e}")
                        file_results.append({
                            "filename": fname,
                            "status": "failed",
                            "error": str(e),
                            "extracted_at": datetime.now().isoformat(),
                            "output_file": None,
                        })

        # Include skipped files in the manifest
        processed_names = {r["filename"] for r in file_results}
        for f in resume_files:
            if f["name"] not in processed_names:
                existing = load_extraction(selected_folder, f["name"])
                file_results.append({
                    "filename": f["name"],
                    "status": "skipped" if existing else "pending",
                    "extracted_at": existing.get("_processed_at", "") if existing else "",
                    "output_file": get_output_path(selected_folder, f["name"]).name if existing else None,
                })

        # Save manifest
        manifest = save_manifest(selected_folder, file_results, model, mode)
        progress_bar.progress(1.0, text="✅ Extraction complete!")

        # Clean up temp directory
        try:
            if TMP_DIR.exists():
                shutil.rmtree(TMP_DIR)
            TMP_DIR.mkdir(parents=True, exist_ok=True)
            st.toast("🧹 Cleaned up temporary files")
        except OSError as e:
            st.warning(f"Could not clean up temp dir: {e}")

        st.markdown("---")
        st.subheader("📊 Extraction Summary")
        scol1, scol2, scol3 = st.columns(3)
        with scol1:
            st.metric("✅ Successful", manifest["successful"])
        with scol2:
            st.metric("❌ Failed", manifest["failed"])
        with scol3:
            st.metric("⏭️ Skipped", manifest["skipped"])

        with st.expander("📋 View Manifest"):
            st.json(manifest)



    # ══════════════════════════════════════════════════════════════════════
    # VIEW EXISTING RESULTS
    # ══════════════════════════════════════════════════════════════════════
    existing_manifest = load_manifest(selected_folder)
    if existing_manifest and not extract_clicked:
        st.markdown("---")
        st.subheader("📊 Previous Extraction Results")

        st.caption(
            f"Last run: {existing_manifest.get('last_updated', 'N/A')}  |  "
            f"Model: `{existing_manifest.get('model_used', 'N/A')}`  |  "
            f"Mode: `{existing_manifest.get('extraction_mode', 'N/A')}`"
        )

        scol1, scol2, scol3 = st.columns(3)
        with scol1:
            st.metric("✅ Successful", existing_manifest.get("successful", 0))
        with scol2:
            st.metric("❌ Failed", existing_manifest.get("failed", 0))
        with scol3:
            st.metric("⏭️ Skipped", existing_manifest.get("skipped", 0))

        st.markdown("#### Individual Results")
        for f in resume_files:
            data = load_extraction(selected_folder, f["name"])
            if data:
                with st.expander(f"📄 {f['name']}"):
                    st.json(data)
            else:
                with st.expander(f"📄 {f['name']} — ⏳ Not yet extracted"):
                    st.info("This resume has not been extracted yet.")


if __name__ == "__main__":
    main()
