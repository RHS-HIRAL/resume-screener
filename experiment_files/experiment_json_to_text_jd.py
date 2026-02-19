"""
experiment_json_to_text_jd.py
─────────────────────────────
Converts local JSON job descriptions to text files and uploads them to SharePoint.

Workflow:
  1. Authenticate with SharePoint.
  2. Iterate through local JSON files in the specified folder.
  3. Convert JSON to plain text.
  4. Upload text to SharePoint at `Text Files/JobDescriptions/<Job_ID>.txt`.
  5. Set the 'Title' metadata on the uploaded file.
"""

import json
import os
import sys
import argparse
from pathlib import Path
import requests
import urllib.parse

# ── Ensure project root is on sys.path so imports work ───────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from resume_screener_pipeline.old_pipeline import SharePointResumeFetcher

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
TARGET_JD_FOLDER = "Text Files/JobDescriptions"

METADATA_FIELDS = {
    "title": "Job Title",
    "location": "Location",
    "job_type": "Job Type",
    "department": "Department",
    "shifts": "Shifts",
    "experience": "Experience Required",
}


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT CONVERSION HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def bullets_to_prose(bullets: list[str]) -> str:
    cleaned = [b.strip(" .,") for b in bullets if b.strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0] + "."
    return ", ".join(cleaned[:-1]) + ", and " + cleaned[-1] + "."


def strip_html(text: str) -> str:
    import re
    return re.sub(r"<[^>]+>", "", text).strip()


def section_to_prose(section: dict) -> str:
    parts = []
    heading = section.get("heading", "").strip()
    paragraphs = [strip_html(p) for p in section.get("paragraphs", []) if p.strip()]
    bullets = [strip_html(b) for b in section.get("bullets", []) if b.strip()]

    if paragraphs:
        parts.append(" ".join(paragraphs))
    if bullets:
        parts.append(bullets_to_prose(bullets))

    if not parts:
        return ""

    body = " ".join(parts)
    return f"{heading}: {body}" if heading else body


def json_to_text(data: dict) -> str:
    lines = []
    
    # Metadata block
    meta_parts = []
    for field, label in METADATA_FIELDS.items():
        value = data.get(field, "").strip()
        if value:
            meta_parts.append(f"{label}: {value}")
    if meta_parts:
        lines.append(". ".join(meta_parts) + ".")

    required = data.get("required_skills", [])
    good_to_have = data.get("good_to_have_skills", [])

    if required:
        lines.append("Required Skills: " + bullets_to_prose(required))
    if good_to_have:
        lines.append("Good to Have Skills: " + bullets_to_prose(good_to_have))

    for section in data.get("sections", []):
        heading = section.get("heading", "")
        if heading in ("Must Have Skills", "Good to Have Skills"):
            continue
        prose = section_to_prose(section)
        if prose:
            lines.append(prose)

    return "\n\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# SHAREPOINT HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def get_sp_client() -> SharePointResumeFetcher:
    return SharePointResumeFetcher()


def check_file_exists(sp: SharePointResumeFetcher, remote_path: str) -> bool:
    """Check if a file exists on SharePoint at the given path."""
    _, drive_id = sp._ensure_site_drive()
    encoded_path = urllib.parse.quote(remote_path)
    # The path should be relative to root
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{encoded_path}"
    
    try:
        resp = requests.get(url, headers=sp._headers, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


def upload_jd_file(sp: SharePointResumeFetcher, content: str, filename: str) -> dict:
    """
    Upload text content to `Text Files/JobDescriptions/<filename>`.
    Returns the JSON response from Microsoft Graph (including item ID).
    """
    _, drive_id = sp._ensure_site_drive()
    
    remote_path = f"{TARGET_JD_FOLDER}/{filename}"
    encoded_path = urllib.parse.quote(remote_path)
    # For some reason, if we use colon syntax for path, we need to be careful.
    # Pattern: /drives/{drive-id}/root:/{path}:/content
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{encoded_path}:/content"
    
    print(f"   ⬆️  Uploading: {filename}")
    resp = requests.put(url, headers=sp._headers, data=content.encode("utf-8"), timeout=60)
    
    if resp.status_code not in (200, 201):
        print(f"      ❌ Upload failed: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
        
    return resp.json()


def set_jd_metadata(sp: SharePointResumeFetcher, item_id: str, title: str):
    """
    Patch the list item to set the 'Title' field (or 'Job Title' if custom col).
    """
    _, drive_id = sp._ensure_site_drive()
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/listItem/fields"
    
    # We try both standard 'Title' and potential custom 'JobTitle' or 'JDTitle'
    # Just setting 'Title' usually works for the main display name in SP lists.
    # If the user has a specific column, we might need to know its internal name.
    # Based on old_pipeline, it reads 'JDTitle' or 'Title'.
    # We will set 'Title' as it's the default, and maybe 'JDTitle' if it exists.
    
    fields = {
        "Title": title,
        "JDTitle": title  # Attempt to set custom column too if it matches
    }
    
    resp = requests.patch(url, headers=sp._headers, json=fields, timeout=30)
    if resp.status_code == 200:
        print(f"      ✅ Metadata set: Title='{title}'")
    else:
        # If JDTitle failed, maybe it doesn't exist. Try just Title.
        if "JDTitle" in resp.text:
             print("      ⚠️ Failed setting 'JDTitle', retrying with just 'Title'...")
             fields = {"Title": title}
             resp = requests.patch(url, headers=sp._headers, json=fields, timeout=30)
             if resp.status_code == 200:
                 print(f"      ✅ Metadata set: Title='{title}'")
             else:
                 print(f"      ⚠️ Metadata patch failed: {resp.status_code} - {resp.text}")
        else:
             print(f"      ⚠️ Metadata patch failed: {resp.status_code} - {resp.text}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def process_folder(input_folder: str):
    print("🚀 Starting JD Text Upload Service...")
    
    try:
        sp = get_sp_client()
        sp._get_token()
    except Exception as e:
        print(f"❌ SharePoint Auth Failed: {e}")
        return

    input_path = Path(input_folder)
    json_files = list(input_path.glob("*.json"))
    
    if not json_files:
        print(f"❌ No JSON files found in '{input_folder}'.")
        return

    print(f"📂 Found {len(json_files)} JSON files in '{input_folder}'.")
    
    success_count = 0
    fail_count = 0
    skip_count = 0

    for json_file in json_files:
        try:
            # 1. Read JSON
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Use file stem (likely Job ID or similar) as filename
            # User requirement: "job id as its name" => usually stem is ID.
            txt_filename = f"{json_file.stem}.txt"
            remote_path = f"{TARGET_JD_FOLDER}/{txt_filename}"
            
            # Check if file exists remotely
            if check_file_exists(sp, remote_path):
                print(f"      ⏭️  Skipping (already exists): {txt_filename}")
                skip_count += 1
                continue
            
            # 2. Extract Text
            text = json_to_text(data)
            job_title = data.get("title", data.get("job_title", "Unknown Role"))
            
            # 3. Upload
            resp_json = upload_jd_file(sp, text, txt_filename)
            item_id = resp_json.get("id")
            
            # 4. Set Metadata
            if item_id:
                set_jd_metadata(sp, item_id, job_title)
            
            success_count += 1

        except Exception as e:
            print(f"   ❌ Failed to process {json_file.name}: {e}")
            fail_count += 1

    print(f"\n🎉 Done! Uploaded: {success_count}, Skipped: {skip_count}, Failed: {fail_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload JD text files to SharePoint.")
    parser.add_argument("input_folder", help="Path to folder containing JSON files")
    args = parser.parse_args()

    process_folder(args.input_folder)