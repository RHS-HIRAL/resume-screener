"""
resume_extraction_script.py (Headless)
──────────────────────────────────────
Extracts text from all resumes in SharePoint and uploads them to:
  Text Files/Resumes/<JobRole>/<Filename>.txt

Workflow:
  1. Authenticate with SharePoint.
  2. Iterate through all job-role subfolders in Resumes/.
  3. For each resume (PDF/DOCX):
     - Download to temp.
     - Extract text.
     - Upload text to SharePoint at `Text Files/Resumes/<Role>/<Filename>.txt`.
"""

import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
import requests
import urllib.parse

# ── Ensure project root is on sys.path so imports work ───────────────────────
# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).resolve().parent
# If script is in 'experiment_files', project root is one level up
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import the fetcher class for auth and helper methods
from resume_screener_pipeline.old_pipeline import SharePointResumeFetcher

from ollama_resume_extractor import (
    extract_text_from_pdf,
    extract_text_from_docx,
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SHAREPOINT_RESUME_BASE = os.getenv("SHAREPOINT_BASE_FOLDER", "Resumes")
TARGET_BASE_FOLDER = "Text Files/Resumes"

TMP_DIR = PROJECT_ROOT / "tmp_resumes_headless"
TMP_DIR.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def get_sp_client() -> SharePointResumeFetcher:
    """Create a SharePoint client."""
    return SharePointResumeFetcher()


def check_file_exists(sp: SharePointResumeFetcher, remote_path: str) -> bool:
    """Check if a file exists on SharePoint at the given path."""
    _, drive_id = sp._ensure_site_drive()
    encoded_path = urllib.parse.quote(remote_path)
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{encoded_path}"
    
    try:
        resp = requests.get(url, headers=sp._headers, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


def upload_to_sharepoint(sp: SharePointResumeFetcher, local_path: Path, remote_path: str):
    """
    Upload a file to SharePoint at the specified remote path.
    Example remote_path: "Text Files/Resumes/JD_Role/resume.txt"
    """
    if check_file_exists(sp, remote_path):
        print(f"      ⏭️  Skipping (already exists): {remote_path}")
        return

    # Ensure we have the drive ID
    _, drive_id = sp._ensure_site_drive()

    encoded_path = urllib.parse.quote(remote_path)
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{encoded_path}:/content"

    print(f"   ⬆️  Uploading to: {remote_path}")
    
    with open(local_path, "rb") as f:
        resp = requests.put(url, headers=sp._headers, data=f, timeout=60)
    
    if resp.status_code not in (200, 201):
        print(f"   ❌ Upload failed: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
    
    print("      ✅ Upload successful")


def list_job_role_folders(sp: SharePointResumeFetcher) -> list[dict]:
    items = sp._list_files(SHAREPOINT_RESUME_BASE)
    folders = [
        {"name": item["name"], "id": item.get("id", "")}
        for item in items
        if "folder" in item
    ]
    return sorted(folders, key=lambda f: f["name"])


def list_resumes_in_folder(sp: SharePointResumeFetcher, folder_name: str) -> list[dict]:
    folder_path = f"{SHAREPOINT_RESUME_BASE}/{folder_name}"
    try:
        items = sp._list_files(folder_path)
    except Exception as e:
        print(f"   ⚠️ Could not list files in {folder_name}: {e}")
        return []

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
            "download_url": item.get("@microsoft.graph.downloadUrl", ""),
        })
    return sorted(resume_files, key=lambda f: f["name"])


def extract_raw_text(local_path: Path) -> str:
    suffix = local_path.suffix.lower()
    if suffix == ".pdf":
        return extract_text_from_pdf(str(local_path))
    elif suffix == ".docx":
        return extract_text_from_docx(str(local_path))
    else:
        return ""


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    print("🚀 Starting Resume Text Extraction & Upload Service...")
    
    try:
        sp = get_sp_client()
        sp._get_token() # Force auth
    except Exception as e:
        print(f"❌ SharePoint Auth Failed: {e}")
        return

    print("✅ Authenticated with SharePoint.")

    # 1. List all job role folders
    print(f"📂 Listing folders in '{SHAREPOINT_RESUME_BASE}'...")
    folders = list_job_role_folders(sp)
    print(f"   Found {len(folders)} folders.")

    total_uploaded = 0
    total_failed = 0
    total_skipped = 0

    for folder in folders:
        role_name = folder["name"]
        print(f"\n─────────────────────────────────────────────────────────────")
        print(f"📁 Processing Role: {role_name}")
        print(f"─────────────────────────────────────────────────────────────")

        # List resumes in this folder
        resumes = list_resumes_in_folder(sp, role_name)
        if not resumes:
            print("   (No resumes found)")
            continue
            
        print(f"   Found {len(resumes)} resumes.")

        for res in resumes:
            fname = res["name"]
            download_url = res["download_url"]
            
            # Prepare paths
            # Structure: Text Files/Resumes/<Role>/<Filename>.txt
            txt_filename = Path(fname).stem + ".txt"
            remote_path = f"{TARGET_BASE_FOLDER}/{role_name}/{txt_filename}"
            local_pdf_path = TMP_DIR / fname
            local_txt_path = TMP_DIR / txt_filename

            # Check if file exists remotely BEFORE downloading/processing
            if check_file_exists(sp, remote_path):
                print(f"   ⏭️  Skipping (already exists): {fname}")
                total_skipped += 1
                continue

            try:
                # A. Download
                # Using fetcher's internal method directly to save to our tmp dir
                # or just use requests since we have the URL
                print(f"   📄 Processing: {fname}")
                
                # Use sp._download_file logic but customized path
                # sp._download_file puts it in DOWNLOAD_DIR, let's copy or download directly
                with requests.get(download_url, stream=True) as r:
                    r.raise_for_status()
                    with open(local_pdf_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                
                # B. Extract Text
                text = extract_raw_text(local_pdf_path)
                if not text or len(text.strip()) < 10:
                    print(f"      ⚠️ Text extraction empty or too short. Skipping.")
                    total_failed += 1
                    continue
                
                # C. Save Text Locally
                local_txt_path.write_text(text, encoding="utf-8")
                
                # D. Upload to SharePoint
                upload_to_sharepoint(sp, local_txt_path, remote_path)
                total_uploaded += 1
                
                # Cleanup immediate files to save space
                local_pdf_path.unlink(missing_ok=True)
                local_txt_path.unlink(missing_ok=True)

            except Exception as e:
                print(f"      ❌ Failed: {e}")
                total_failed += 1

    print(f"\n🎉 Done! Uploaded: {total_uploaded}, Skipped: {total_skipped}, Failed: {total_failed}")
    
    # Final cleanup
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR)


if __name__ == "__main__":
    main()