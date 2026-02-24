"""
Resume Screener — Streamlit UI
Dynamically select resume + JD files, send to FastAPI, display & save results.
"""

import io
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()  # pick up AZURE_* and SHAREPOINT_* from .env

# ── SharePoint text-file folder paths (overridable via .env) ─────────────────
SP_TEXT_RESUMES_FOLDER = os.getenv("SHAREPOINT_TEXT_RESUMES_FOLDER", "Text Files/Resumes")
SP_TEXT_JD_FOLDER      = os.getenv("SHAREPOINT_TEXT_JD_FOLDER",      "Text Files/JobDescriptions")

# ── Optional SharePoint / MSAL integration ────────────────────────────────────
try:
    import msal
    HAS_MSAL = True
except ImportError:
    HAS_MSAL = False

# ── Optional text-extraction libraries ───────────────────────────────────────
try:
    import PyPDF2 as pdf_lib
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

try:
    from docx import Document as DocxDocument
    HAS_DOCX = True
except ImportError:
    HAS_DOCX = False

# ═══════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Resume Screener",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════════════
# GLOBAL STYLES
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;600&display=swap');

/* ── Reset & base ── */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* ── App background ── */
.stApp {
    background: #0d0f14;
    background-image:
        radial-gradient(ellipse at 20% 10%, rgba(99,102,241,0.12) 0%, transparent 60%),
        radial-gradient(ellipse at 80% 80%, rgba(16,185,129,0.08) 0%, transparent 50%);
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #13151c !important;
    border-right: 1px solid rgba(255,255,255,0.06) !important;
}
[data-testid="stSidebar"] * {
    color: #c8ccd8 !important;
}

/* ── Cards ── */
.rs-card {
    background: #161a24;
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 14px;
    padding: 1.6rem 1.8rem;
    margin-bottom: 1.2rem;
    box-shadow: 0 4px 30px rgba(0,0,0,0.3);
    transition: border-color 0.2s;
}
.rs-card:hover {
    border-color: rgba(99,102,241,0.3);
}

/* ── Section titles ── */
.rs-title {
    font-family: 'DM Serif Display', serif;
    font-size: 2.6rem;
    color: #f0f2fa;
    letter-spacing: -0.02em;
    line-height: 1.1;
    margin: 0 0 0.3rem 0;
}
.rs-subtitle {
    font-size: 0.95rem;
    color: #6b7280;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    font-weight: 500;
    margin: 0 0 2rem 0;
}
.rs-section-header {
    font-family: 'DM Serif Display', serif;
    font-size: 1.35rem;
    color: #e8eaf2;
    margin: 0 0 0.9rem 0;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

/* ── Score ring ── */
.score-ring-wrap {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 1.5rem 0;
}
.score-number {
    font-family: 'DM Serif Display', serif;
    font-size: 4.5rem;
    line-height: 1;
    font-weight: 400;
    background: linear-gradient(135deg, #818cf8, #34d399);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}
.score-label {
    font-size: 0.75rem;
    color: #6b7280;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-weight: 600;
    margin-top: 0.3rem;
}

/* ── Match badges ── */
.badge {
    display: inline-block;
    padding: 0.22rem 0.75rem;
    border-radius: 999px;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
}
.badge-match       { background: rgba(16,185,129,0.15); color: #34d399; border: 1px solid rgba(16,185,129,0.3); }
.badge-partial     { background: rgba(251,191,36,0.13); color: #fbbf24; border: 1px solid rgba(251,191,36,0.3); }
.badge-no-match    { background: rgba(239,68,68,0.12);  color: #f87171; border: 1px solid rgba(239,68,68,0.3); }

/* ── Parameter card ── */
.param-card {
    background: #1a1f2e;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px;
    padding: 1rem 1.2rem;
    margin-bottom: 0.7rem;
}
.param-title {
    font-size: 0.78rem;
    color: #8b90a0;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-weight: 600;
    margin-bottom: 0.4rem;
}
.param-summary {
    font-size: 0.9rem;
    color: #c8ccd8;
    line-height: 1.5;
}

/* ── Profile card ── */
.profile-field {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    padding: 0.55rem 0;
    border-bottom: 1px solid rgba(255,255,255,0.05);
}
.profile-field:last-child { border-bottom: none; }
.pf-label { font-size: 0.75rem; color: #6b7280; text-transform: uppercase; letter-spacing: 0.08em; font-weight: 600; }
.pf-value { font-size: 0.9rem; color: #dde1ee; text-align: right; max-width: 65%; word-break: break-word; }

/* ── Status / info pills ── */
.info-pill {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    background: rgba(99,102,241,0.1);
    border: 1px solid rgba(99,102,241,0.2);
    border-radius: 999px;
    padding: 0.3rem 0.9rem;
    font-size: 0.8rem;
    color: #818cf8;
    font-weight: 500;
    margin: 0.2rem 0.2rem 0.2rem 0;
}

/* ── JSON viewer ── */
.json-viewer {
    background: #0f1117;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 10px;
    padding: 1.2rem;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    color: #a8b2c8;
    overflow-x: auto;
    max-height: 400px;
    overflow-y: auto;
    white-space: pre;
}

/* ── Streamlit overrides ── */
.stButton>button {
    background: linear-gradient(135deg, #6366f1, #4f46e5) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 0.6rem 1.8rem !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    letter-spacing: 0.02em !important;
    transition: all 0.2s !important;
    box-shadow: 0 4px 15px rgba(99,102,241,0.25) !important;
}
.stButton>button:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 6px 20px rgba(99,102,241,0.4) !important;
}
.stTextInput>div>div>input,
.stTextArea>div>div>textarea,
.stSelectbox>div>div>div {
    background: #1a1f2e !important;
    color: #dde1ee !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 8px !important;
    font-family: 'DM Sans', sans-serif !important;
}
.stFileUploader>div {
    background: #161a24 !important;
    border: 1.5px dashed rgba(99,102,241,0.3) !important;
    border-radius: 12px !important;
}
div[data-testid="stFileUploaderDropzone"] {
    background: #161a24 !important;
}
label { color: #8b90a0 !important; font-size: 0.8rem !important; font-weight: 600 !important; text-transform: uppercase !important; letter-spacing: 0.08em !important; }
.stMarkdown p { color: #c8ccd8; }
.stProgress > div > div { background: linear-gradient(90deg, #6366f1, #34d399) !important; }
hr { border-color: rgba(255,255,255,0.07) !important; }

/* ── Divider ── */
.rs-divider {
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(99,102,241,0.4), transparent);
    margin: 1.5rem 0;
}

/* Alert boxes */
.stAlert { border-radius: 10px !important; }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# HELPERS — Text Extraction
# ═══════════════════════════════════════════════════════════════════════════

def extract_text_from_pdf(file_bytes: bytes) -> str:
    if not HAS_PYPDF:
        st.error("PyPDF2 not installed. Run: pip install PyPDF2")
        return ""
    try:
        reader = pdf_lib.PdfReader(io.BytesIO(file_bytes))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as e:
        st.error(f"PDF extraction failed: {e}")
        return ""


def extract_text_from_docx(file_bytes: bytes) -> str:
    if not HAS_DOCX:
        st.error("python-docx not installed. Run: pip install python-docx")
        return ""
    try:
        doc = DocxDocument(io.BytesIO(file_bytes))
        parts = [p.text for p in doc.paragraphs]
        for table in doc.tables:
            for row in table.rows:
                parts.append(" ".join(cell.text for cell in row.cells))
        return "\n".join(parts)
    except Exception as e:
        st.error(f"DOCX extraction failed: {e}")
        return ""


def extract_text(uploaded_file) -> str:
    """Extract text from an uploaded file (PDF, DOCX, TXT)."""
    if uploaded_file is None:
        return ""
    raw = uploaded_file.read()
    name = uploaded_file.name.lower()
    if name.endswith(".pdf"):
        return extract_text_from_pdf(raw)
    elif name.endswith(".docx"):
        return extract_text_from_docx(raw)
    elif name.endswith(".txt"):
        return raw.decode("utf-8", errors="replace")
    else:
        st.warning(f"Unsupported file type: {uploaded_file.name}")
        return ""


def extract_text_from_path(path: str) -> str:
    """Extract text from a file on disk."""
    p = Path(path)
    if not p.exists():
        return ""
    raw = p.read_bytes()
    if path.lower().endswith(".pdf"):
        return extract_text_from_pdf(raw)
    elif path.lower().endswith(".docx"):
        return extract_text_from_docx(raw)
    elif path.lower().endswith(".txt"):
        return raw.decode("utf-8", errors="replace")
    return ""


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS — API Call
# ═══════════════════════════════════════════════════════════════════════════

def call_api(api_url: str, resume_text: str, jd_text: str) -> dict | None:
    endpoint = api_url.rstrip("/") + "/api/v1/analyze-resume"
    payload = {"resume_text": resume_text, "jd_text": jd_text}
    try:
        resp = requests.post(endpoint, json=payload, timeout=120)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error(f"❌ Cannot connect to API at **{endpoint}**. Make sure your FastAPI server is running.")
        return None
    except requests.exceptions.Timeout:
        st.error("❌ Request timed out (120s). The model may be taking too long.")
        return None
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API error {e.response.status_code}: {e.response.text[:400]}")
        return None
    except Exception as e:
        st.error(f"❌ Unexpected error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS — Save Result
# ═══════════════════════════════════════════════════════════════════════════

def save_result(result: dict, role: str, full_name: str) -> Path:
    """Save JSON result to extracted_json_output/<role>/<fullname>_<datetime>.json"""
    safe_role = re.sub(r"[^\w\-]", "_", role.strip()) or "General"
    safe_name = re.sub(r"[^\w\-]", "_", full_name.strip()) or "Unknown"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    out_dir = Path("extracted_json_output") / safe_role
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{safe_name}_{ts}.json"
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_path


# ═══════════════════════════════════════════════════════════════════════════
# SHAREPOINT — MatchScore Updater
# ═══════════════════════════════════════════════════════════════════════════

class SharePointMatchScoreUpdater:
    """
    Finds a resume file already uploaded to SharePoint (by filename) and
    writes the MatchScore rounded integer into the 'MatchScore' column.

    Authentication uses the same Azure AD client-credentials flow as app.py.
    All settings are read from the sidebar/session_state at call time.
    """

    GRAPH_BASE = "https://graph.microsoft.com/v1.0"
    SCOPES = ["https://graph.microsoft.com/.default"]

    def __init__(self, tenant_id: str, client_id: str, client_secret: str,
                 site_domain: str, site_path: str, drive_name: str):
        if not HAS_MSAL:
            raise RuntimeError("msal is not installed. Run: pip install msal")
        self._msal_app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
        )
        self.site_domain = site_domain
        self.site_path = site_path.strip("/")
        self.drive_name = drive_name
        self._site_id: str | None = None
        self._drive_id: str | None = None

    # ── Auth ────────────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        result = self._msal_app.acquire_token_silent(self.SCOPES, account=None)
        if not result:
            result = self._msal_app.acquire_token_for_client(scopes=self.SCOPES)
        if "access_token" not in result:
            raise RuntimeError(result.get("error_description", "Token acquisition failed"))
        return {
            "Authorization": f"Bearer {result['access_token']}",
            "Content-Type": "application/json",
        }

    # ── Site / Drive resolution ──────────────────────────────────────────────

    def _get_site_id(self) -> str:
        if self._site_id:
            return self._site_id
        url = f"{self.GRAPH_BASE}/sites/{self.site_domain}:/{self.site_path}"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        self._site_id = resp.json()["id"]
        return self._site_id

    def _get_drive_id(self) -> str:
        if self._drive_id:
            return self._drive_id
        site_id = self._get_site_id()
        url = f"{self.GRAPH_BASE}/sites/{site_id}/drives"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        drives = resp.json().get("value", [])
        for d in drives:
            if d["name"].lower() == self.drive_name.lower():
                self._drive_id = d["id"]
                return self._drive_id
        # Fallback: use first available drive
        if drives:
            self._drive_id = drives[0]["id"]
            return self._drive_id
        raise RuntimeError(f"No drives found on SharePoint site '{self.site_domain}/{self.site_path}'")

    # ── Folder browsing ──────────────────────────────────────────────────────

    def _list_folder_children(self, folder_path: str) -> list[dict]:
        """
        Return direct children of a drive folder given its relative path
        (e.g. 'Text Files/Resumes').  Handles pagination automatically.
        Each item dict contains: id, name, is_folder.
        """
        from urllib.parse import quote as _quote
        drive_id    = self._get_drive_id()
        encoded     = _quote(folder_path.strip("/"), safe="/")
        url: str | None = (
            f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{encoded}:/children"
            "?$select=id,name,file,folder&$top=999"
        )
        items: list[dict] = []
        while url:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            if not resp.ok:
                break
            data  = resp.json()
            items.extend(data.get("value", []))
            url   = data.get("@odata.nextLink")
        return items

    def list_resumes_grouped(self) -> dict[str, list[dict]]:
        """
        List all .txt resume files inside SP_TEXT_RESUMES_FOLDER,
        grouped by immediate subfolder name.

        Returns: { subfolder_name: [{id, name}, ...], ... }
        """
        subfolders = [
            item for item in self._list_folder_children(SP_TEXT_RESUMES_FOLDER)
            if "folder" in item
        ]
        groups: dict[str, list[dict]] = {}
        for sf in subfolders:
            sf_path = f"{SP_TEXT_RESUMES_FOLDER}/{sf['name']}"
            files = [
                {"id": f["id"], "name": f["name"]}
                for f in self._list_folder_children(sf_path)
                if "file" in f and f["name"].lower().endswith(".txt")
            ]
            if files:
                groups[sf["name"]] = files
        return groups

    def list_jd_files(self) -> list[dict]:
        """
        List all .txt JD files inside SP_TEXT_JD_FOLDER (flat, no subfolders).
        Returns: [{id, name}, ...]
        """
        return [
            {"id": f["id"], "name": f["name"]}
            for f in self._list_folder_children(SP_TEXT_JD_FOLDER)
            if "file" in f and f["name"].lower().endswith(".txt")
        ]

    def download_text_content(self, item_id: str) -> str:
        """
        Download and return the UTF-8 text content of a drive item by ID.
        Follows the redirect that Graph API issues for /content requests.
        """
        drive_id = self._get_drive_id()
        url      = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"
        resp     = requests.get(url, headers=self._headers(), timeout=60, allow_redirects=True)
        resp.raise_for_status()
        return resp.content.decode("utf-8", errors="replace")

    # ── File lookup ──────────────────────────────────────────────────────────

    def find_matching_items(self, filename: str, role_hint: str = "") -> list[dict]:
        """
        Search the Resumes/ folder of the drive for files whose name exactly
        matches `filename` (case-insensitive).

        Returns a list of dicts: [{id, name, path}, ...].
        If role_hint is given and multiple matches exist, items whose path
        contains any word from role_hint are ranked first.
        """
        drive_id = self._get_drive_id()
        # Use stem of filename as search term to cast a wide enough net
        stem = Path(filename).stem
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root/search(q='{stem}')"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        if not resp.ok:
            return []

        matches = []
        for item in resp.json().get("value", []):
            if "folder" in item:
                continue
            if item.get("name", "").lower() != filename.lower():
                continue
            parent_path = (
                item.get("parentReference", {}).get("path", "") or ""
            )
            matches.append({
                "id":   item["id"],
                "name": item["name"],
                "path": parent_path,
            })

        if len(matches) <= 1 or not role_hint:
            return matches

        # ── Role-hint ranking: prefer items whose path contains role tokens ──
        role_tokens = [t.lower() for t in re.split(r"[\W_]+", role_hint) if len(t) > 2]
        def _score(m: dict) -> int:
            p = m["path"].lower()
            return sum(1 for t in role_tokens if t in p)

        ranked = sorted(matches, key=_score, reverse=True)
        top_score = _score(ranked[0])

        # If the top-scoring item is unambiguous, return just that one
        top_group = [m for m in ranked if _score(m) == top_score]
        return top_group if len(top_group) == 1 else ranked

    # ── Public methods ───────────────────────────────────────────────────────

    def fetch_match_score(
        self,
        filename: str,
        role_hint: str = "",
    ) -> tuple[str | None, int | None]:
        """
        Locate `filename` on SharePoint and read its current MatchScore field.

        Returns:
            (item_id, existing_score)
            - item_id is None when the file is not found or multiple matches remain
              ambiguous (caller should fall through to push_match_score instead).
            - existing_score is None when the column is empty / unset / zero.
        """
        candidates = self.find_matching_items(filename, role_hint=role_hint)

        if not candidates or len(candidates) > 1:
            # Not found, or ambiguous — caller handles via push_match_score
            return None, None

        item_id  = candidates[0]["id"]
        drive_id = self._get_drive_id()
        url      = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}/listItem/fields"
        resp     = requests.get(url, headers=self._headers(), timeout=30)
        if not resp.ok:
            return item_id, None

        raw = resp.json().get("MatchScore")
        if raw is None or str(raw).strip() in ("", "0", "None"):
            return item_id, None

        try:
            return item_id, int(round(float(raw)))
        except (TypeError, ValueError):
            return item_id, None

    def push_match_score(
        self,
        filename: str,
        score: int,
        role_hint: str = "",
        confirmed_item_id: str = "",
    ) -> tuple[str, str, list[dict]]:
        """
        Locate `filename` on SharePoint and PATCH MatchScore = score.

        Returns a 3-tuple:
            ("OK",           message,        [])            – success
            ("NOT_FOUND",    message,        [])            – file not found
            ("NEEDS_CONFIRM",message,        [candidates])  – multiple matches, user must pick
            ("ERROR",        message,        [])            – HTTP / other error

        Pass `confirmed_item_id` (from a previous NEEDS_CONFIRM response) to
        skip the search and write directly to that item.
        """
        drive_id = self._get_drive_id()

        # ── Resolve item_id ──────────────────────────────────────────────────
        if confirmed_item_id:
            item_id = confirmed_item_id
        else:
            candidates = self.find_matching_items(filename, role_hint=role_hint)

            if not candidates:
                return (
                    "NOT_FOUND",
                    f"File **{filename}** was not found in the SharePoint drive '{self.drive_name}'.",
                    [],
                )

            if len(candidates) > 1:
                return (
                    "NEEDS_CONFIRM",
                    (
                        f"**{len(candidates)} files** named `{filename}` were found in SharePoint. "
                        "Please select the correct one below and click **Confirm & Push**."
                    ),
                    candidates,
                )

            item_id = candidates[0]["id"]

        # ── PATCH ────────────────────────────────────────────────────────────
        url  = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}/listItem/fields"
        resp = requests.patch(
            url,
            headers=self._headers(),
            json={"MatchScore": score},
            timeout=30,
        )
        if resp.status_code == 200:
            return (
                "OK",
                f"MatchScore = **{score}** successfully written to `{filename}` on SharePoint.",
                [],
            )
        return (
            "ERROR",
            (
                f"SharePoint returned HTTP {resp.status_code} while updating `{filename}`: "
                f"{resp.text[:300]}"
            ),
            [],
        )


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS — Scan local folder for files
# ═══════════════════════════════════════════════════════════════════════════

def _make_sp_updater() -> "SharePointMatchScoreUpdater":
    """Convenience factory — builds an updater from the current sp_config in session_state."""
    cfg = st.session_state.get("sp_config", {})
    return SharePointMatchScoreUpdater(
        tenant_id     = cfg["tenant_id"],
        client_id     = cfg["client_id"],
        client_secret = cfg["client_secret"],
        site_domain   = cfg["site_domain"],
        site_path     = cfg["site_path"],
        drive_name    = cfg["drive_name"],
    )


def _extract_job_id_from_subfolder(subfolder: str) -> str:
    """
    Extract the leading job-ID token from a subfolder name.
    e.g.  '5101_Trainee_Accountant'  → '5101'
          '3250_Network_L3_Engineer' → '3250'
    Returns '' when no leading token is found.
    """
    m = re.match(r"^(\w+?)_", subfolder)
    return m.group(1) if m else ""


def _find_jd_for_job_id(job_id: str, jd_files: list[dict]) -> dict | None:
    """
    Return the first JD file whose name contains `job_id` as a delimited token.
    Matches patterns like:
      JD_5101_automation-engineer-it-infra.txt
      JD_5101.txt
    Returns None when no match is found.
    """
    if not job_id:
        return None
    pattern = re.compile(
        rf"(?:^|[_\-]){re.escape(job_id)}(?:[_\-\.])", re.IGNORECASE
    )
    for f in jd_files:
        if pattern.search(f["name"]):
            return f
    return None


def _clear_session_for_new_resume() -> None:
    """
    Called when the user picks a different resume subfolder or file.
    Clears loaded texts, analysis results, and all push-related state.
    """
    for key in [
        "active_resume_text",
        "active_jd_text",
        "last_result",
        "last_resume_filename",
        "auto_push_status",
        "sp_confirm_candidates",
        "sp_confirm_score",
        "sp_confirm_filename",
    ]:
        st.session_state.pop(key, None)


def scan_folder(folder: str, extensions=(".pdf", ".docx", ".txt")) -> list[str]:
    p = Path(folder)
    if not p.exists():
        return []
    return sorted(
        str(f) for f in p.rglob("*")
        if f.is_file() and f.suffix.lower() in extensions
    )


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS — Render result
# ═══════════════════════════════════════════════════════════════════════════

def badge_html(status: str) -> str:
    s = status.lower()
    if "no match" in s or "no_match" in s:
        cls = "badge-no-match"
        label = "No Match"
    elif "partial" in s:
        cls = "badge-partial"
        label = "Partial Match"
    else:
        cls = "badge-match"
        label = "Match"
    return f'<span class="badge {cls}">{label}</span>'


def render_result(result: dict, role: str, resume_filename: str = ""):
    match_data = result.get("function_1_resume_jd_matching", {})
    extract_data = result.get("function_2_resume_data_extraction", {})

    score = match_data.get("overall_match_score", 0)
    personal = extract_data.get("personal_information", {})
    employment = extract_data.get("current_employment", {})
    career = extract_data.get("career_metrics", {})
    socials = extract_data.get("social_profiles", {})
    education_list = extract_data.get("education_history", [])
    summary = extract_data.get("professional_summary", "")

    full_name = personal.get("full_name", "Unknown")

    # ── Top row: Score + Personal Info ──────────────────────────────────
    col_score, col_info, col_career = st.columns([1, 2, 1.5])

    with col_score:
        # Score color
        if score >= 75:
            color_start, color_end = "#34d399", "#10b981"
        elif score >= 50:
            color_start, color_end = "#fbbf24", "#f59e0b"
        else:
            color_start, color_end = "#f87171", "#ef4444"

        st.markdown(f"""
        <div class="rs-card" style="text-align:center; padding:2rem 1rem;">
            <div class="score-label">Overall Match</div>
            <div class="score-number" style="background:linear-gradient(135deg,{color_start},{color_end});-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;">
                {score}
            </div>
            <div class="score-label" style="margin-top:0.5rem;">out of 100</div>
        </div>
        """, unsafe_allow_html=True)

    with col_info:
        st.markdown(f"""
        <div class="rs-card">
            <div class="rs-section-header">👤 {full_name}</div>
            <div class="profile-field"><span class="pf-label">Role</span><span class="pf-value">{employment.get("current_job_title","—")}</span></div>
            <div class="profile-field"><span class="pf-label">Company</span><span class="pf-value">{employment.get("current_organization","—")}</span></div>
            <div class="profile-field"><span class="pf-label">Email</span><span class="pf-value">{personal.get("email","—")}</span></div>
            <div class="profile-field"><span class="pf-label">Phone</span><span class="pf-value">{personal.get("phone","—")}</span></div>
            <div class="profile-field"><span class="pf-label">Location</span><span class="pf-value">{personal.get("location","—")}</span></div>
        </div>
        """, unsafe_allow_html=True)

    with col_career:
        linkedin = socials.get("linkedin", "")
        github = socials.get("github", "")
        portfolio = socials.get("portfolio", "")
        edu_str = "—"
        if education_list:
            top = education_list[0]
            edu_str = f"{top.get('degree','')}<br><span style='font-size:0.78rem;color:#6b7280'>{top.get('institution','')}</span>"

        link_pills = ""
        if linkedin and linkedin != "N/A":
            link_pills += f'<span class="info-pill">🔗 LinkedIn</span>'
        if github and github != "N/A":
            link_pills += f'<span class="info-pill">🐙 GitHub</span>'
        if portfolio and portfolio != "N/A":
            link_pills += f'<span class="info-pill">🌐 Portfolio</span>'

        st.markdown(f"""
        <div class="rs-card">
            <div class="rs-section-header">📈 Career</div>
            <div class="profile-field"><span class="pf-label">Experience</span><span class="pf-value">{career.get("total_experience_in_years","—")} yrs</span></div>
            <div class="profile-field"><span class="pf-label">Total Jobs</span><span class="pf-value">{career.get("total_jobs","—")}</span></div>
            <div class="profile-field"><span class="pf-label">Education</span><span class="pf-value">{edu_str}</span></div>
            <div style="margin-top:0.8rem">{link_pills if link_pills else '<span style="color:#4b5563;font-size:0.8rem">No social links found</span>'}</div>
        </div>
        """, unsafe_allow_html=True)

    # ── Professional Summary ─────────────────────────────────────────────
    if summary:
        st.markdown(f"""
        <div class="rs-card">
            <div class="rs-section-header">📝 Professional Summary</div>
            <p style="color:#a8b2c8;line-height:1.7;font-size:0.92rem;margin:0">{summary}</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('<div class="rs-divider"></div>', unsafe_allow_html=True)

    # ── JD Parameter Matching ────────────────────────────────────────────
    st.markdown('<div class="rs-section-header">🎯 JD Match Parameters</div>', unsafe_allow_html=True)

    param_keys = [
        ("experience",                "⏱ Experience"),
        ("education",                 "🎓 Education"),
        ("location",                  "📍 Location"),
        ("project_history_relevance", "🏗 Project History"),
        ("tools_used",                "🔧 Tools Used"),
        ("certifications",            "🏅 Certifications"),
    ]

    cols = st.columns(2)
    for i, (key, label) in enumerate(param_keys):
        param = match_data.get(key, {})
        status = param.get("status", "Unknown")
        summary_text = param.get("summary", "No details available.")
        with cols[i % 2]:
            st.markdown(f"""
            <div class="param-card">
                <div class="param-title">{label}</div>
                <div style="margin-bottom:0.5rem">{badge_html(status)}</div>
                <div class="param-summary">{summary_text}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown('<div class="rs-divider"></div>', unsafe_allow_html=True)

    # ── Raw JSON ─────────────────────────────────────────────────────────
    with st.expander("🗂 Raw JSON Response", expanded=False):
        st.markdown(f'<div class="json-viewer">{json.dumps(result, indent=2, ensure_ascii=False)}</div>', unsafe_allow_html=True)

    # ── Save / Download / Push to SharePoint ─────────────────────────────
    st.markdown("")
    col_save, col_dl, col_sp = st.columns([2, 1, 2])
    with col_save:
        if st.button("💾  Save to extracted_json_output/", type="primary"):
            saved_path = save_result(result, role, full_name)
            st.success(f"✅ Saved → `{saved_path}`")

    with col_dl:
        json_bytes = json.dumps(result, indent=2, ensure_ascii=False).encode("utf-8")
        safe_name = re.sub(r"[^\w\-]", "_", full_name)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="⬇  Download JSON",
            data=json_bytes,
            file_name=f"{safe_name}_{ts}.json",
            mime="application/json",
        )

    with col_sp:
        # ── SharePoint MatchScore push ────────────────────────────────────
        sp_cfg     = st.session_state.get("sp_config", {})
        sp_enabled = sp_cfg.get("enabled", False)

        # Auto-detect: if the loaded file is a .txt, the SharePoint PDF
        # shares the same stem — swap the extension automatically.
        _raw_fn = resume_filename or ""
        if _raw_fn.lower().endswith(".txt"):
            _default_fn = Path(_raw_fn).stem + ".pdf"
        else:
            _default_fn = _raw_fn  # already .pdf, or blank for paste-text case

        sp_filename = st.text_input(
            "SharePoint PDF filename",
            value=_default_fn,
            placeholder="e.g. John_Doe_5101_2024-05-10.pdf",
            key="sp_filename_override",
            help=(
                "Target PDF filename in SharePoint Resumes/ folder. "
                "Auto-derived from the loaded .txt file; edit manually if needed."
            ),
        ).strip()

        # ── Show auto-push outcome ────────────────────────────────────────
        _auto_status = st.session_state.get("auto_push_status")
        if _auto_status:
            _kind, _amsg = _auto_status
            if _kind == "ok":
                st.success(f"🤖 Auto-pushed: {_amsg}")
            elif _kind == "needs_confirm":
                st.warning(f"🤖 Auto-push needs confirmation ↓")
            elif _kind == "error":
                st.warning(f"🤖 Auto-push failed — use button below")
            elif _kind == "skipped":
                st.info(f"🤖 {_amsg}")

        push_ready   = sp_enabled and bool(sp_filename)
        # Label changes based on whether auto-push already succeeded
        _already_pushed = _auto_status and _auto_status[0] == "ok"
        push_label   = (
            "📤  Re-push MatchScore"
            if _already_pushed
            else "📤  Push MatchScore to SharePoint"
        )
        push_tooltip = (
            "Push this candidate's MatchScore to their SharePoint resume PDF"
            if push_ready
            else (
                "SharePoint not configured — check .env variables"
                if not sp_enabled
                else "Enter the SharePoint filename above to enable"
            )
        )

        if st.button(
            push_label,
            disabled=not push_ready,
            use_container_width=True,
            key="sp_push_btn",
            help=push_tooltip,
        ):
            # Clear any previous confirmation state on a fresh push attempt
            st.session_state.pop("sp_confirm_candidates", None)
            st.session_state.pop("sp_confirm_score",      None)
            st.session_state.pop("sp_confirm_filename",   None)

            rounded_score = round(score)
            role_hint     = st.session_state.get("role_name", "")
            try:
                with st.spinner(f"Searching SharePoint for `{sp_filename}`…"):
                    status, msg, candidates = _make_sp_updater().push_match_score(
                        sp_filename, rounded_score, role_hint=role_hint
                    )

                if status == "OK":
                    st.success(msg)
                elif status == "NEEDS_CONFIRM":
                    # Stash candidates so the confirmation widget below can render
                    st.session_state["sp_confirm_candidates"] = candidates
                    st.session_state["sp_confirm_score"]      = rounded_score
                    st.session_state["sp_confirm_filename"]   = sp_filename
                    st.warning(msg)
                else:  # NOT_FOUND or ERROR
                    st.error(msg)
            except Exception as exc:
                st.error(f"❌ SharePoint error: {exc}")

        # ── Confirmation widget (shown when NEEDS_CONFIRM was returned) ───
        candidates = st.session_state.get("sp_confirm_candidates")
        if candidates and sp_enabled:
            confirm_score    = st.session_state.get("sp_confirm_score", round(score))
            confirm_filename = st.session_state.get("sp_confirm_filename", sp_filename)

            # Build human-readable labels: filename + parent folder path
            labels = [
                f"{c['name']}  ·  {c['path'].split('root:')[-1] if 'root:' in c['path'] else c['path']}"
                for c in candidates
            ]
            chosen_label = st.selectbox(
                "Select the correct file:",
                options=labels,
                key="sp_confirm_select",
            )
            chosen_idx  = labels.index(chosen_label)
            chosen_item = candidates[chosen_idx]

            if st.button(
                f"✅  Confirm & Push MatchScore = {confirm_score}",
                use_container_width=True,
                key="sp_confirm_btn",
            ):
                try:
                    with st.spinner(f"Writing MatchScore = {confirm_score}…"):
                        status, msg, _ = _make_sp_updater().push_match_score(
                            confirm_filename,
                            confirm_score,
                            confirmed_item_id=chosen_item["id"],
                        )
                    if status == "OK":
                        st.success(msg)
                        # Clear confirmation state after successful push
                        st.session_state.pop("sp_confirm_candidates", None)
                        st.session_state.pop("sp_confirm_score",      None)
                        st.session_state.pop("sp_confirm_filename",   None)
                    else:
                        st.error(msg)
                except Exception as exc:
                    st.error(f"❌ SharePoint error: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("""
    <div style="padding: 0.5rem 0 1.5rem 0;">
        <div style="font-family:'DM Serif Display',serif; font-size:1.4rem; color:#e8eaf2; line-height:1.1;">Resume<br>Screener</div>
        <div style="font-size:0.7rem; color:#4b5563; text-transform:uppercase; letter-spacing:0.12em; font-weight:600; margin-top:0.3rem;">AI-Powered Analysis</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<span style="font-size:0.75rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;">⚙ Configuration</span>', unsafe_allow_html=True)

    api_url = st.text_input(
        "FastAPI Server URL",
        value=st.session_state.get("api_url", "http://localhost:8000"),
        placeholder="http://localhost:8000",
        key="api_url_input",
        help="Base URL of your FastAPI server",
    )
    st.session_state["api_url"] = api_url

    role_name = st.text_input(
        "Role / Position Name",
        value=st.session_state.get("role_name", ""),
        placeholder="e.g. 3250_Network_L3_Engineer",
        key="role_name_input",
        help="Used for output folder organisation",
    )
    st.session_state["role_name"] = role_name

    st.markdown("---")
    st.markdown("""
    <div style="font-size:0.75rem; color:#374151; line-height:1.6;">
        <div style="color:#6b7280; font-weight:600; margin-bottom:0.4rem;">HOW IT WORKS</div>
        1. Select a <b>Resume</b> from SharePoint<br>
        2. Select a <b>Job Description</b> from SharePoint<br>
        3. Sent to FastAPI <code style="color:#818cf8">/analyze-resume</code><br>
        4. Results displayed &amp; saved<br>
        5. Push <b>MatchScore</b> back to SharePoint
    </div>
    """, unsafe_allow_html=True)

    # Dependencies check
    st.markdown("---")
    st.markdown('<span style="font-size:0.75rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;">📦 Dependencies</span>', unsafe_allow_html=True)
    dep_col1, dep_col2 = st.columns(2)
    with dep_col1:
        st.markdown(f"{'✅' if HAS_PYPDF else '❌'} PyPDF2")
        st.markdown(f"{'✅' if HAS_DOCX else '❌'} python-docx")

    # ── SharePoint Integration ───────────────────────────────────────────
    # Credentials are loaded silently from .env — no input fields needed.
    st.markdown("---")
    st.markdown('<span style="font-size:0.75rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;">🔗 SharePoint Integration</span>', unsafe_allow_html=True)

    _sp_tenant   = os.getenv("AZURE_TENANT_ID", "")
    _sp_client   = os.getenv("AZURE_CLIENT_ID", "")
    _sp_secret   = os.getenv("AZURE_CLIENT_SECRET", "")
    _sp_domain   = os.getenv("SHAREPOINT_SITE_DOMAIN", "")
    _sp_sitepath = os.getenv("SHAREPOINT_SITE_PATH", "")
    _sp_drive    = os.getenv("SHAREPOINT_DRIVE_NAME", "Documents")

    _sp_ready = all([_sp_tenant, _sp_client, _sp_secret, _sp_domain, _sp_sitepath, _sp_drive])

    if _sp_ready and HAS_MSAL:
        st.markdown(
            f'<div style="font-size:0.82rem;color:#34d399;">✅ Connected to <b>{_sp_domain}</b></div>'
            f'<div style="font-size:0.75rem;color:#4b5563;margin-top:0.2rem;">Drive: {_sp_drive} · {_sp_sitepath}</div>',
            unsafe_allow_html=True,
        )
    elif not HAS_MSAL:
        st.warning("⚠️ `msal` not installed — run `pip install msal`")
    else:
        st.warning("⚠️ One or more AZURE_* / SHAREPOINT_* env vars are missing in .env")

    # Build sp_config once per render cycle so render_result() can read it
    st.session_state["sp_config"] = {
        "enabled":       _sp_ready and HAS_MSAL,
        "tenant_id":     _sp_tenant,
        "client_id":     _sp_client,
        "client_secret": _sp_secret,
        "site_domain":   _sp_domain,
        "site_path":     _sp_sitepath,
        "drive_name":    _sp_drive,
    }


# ═══════════════════════════════════════════════════════════════════════════
# MAIN PAGE
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("""
<div style="padding: 2rem 0 0.5rem 0;">
    <div class="rs-title">Resume Screener</div>
    <div class="rs-subtitle">AI-Powered Resume × Job Description Analysis</div>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# INPUT SECTION — SharePoint File Browser
# ═══════════════════════════════════════════════════════════════════════════

sp_cfg     = st.session_state.get("sp_config", {})
sp_enabled = sp_cfg.get("enabled", False)

# ── Auto-fetch file lists on first render (cached in session_state) ──────────
if sp_enabled and "sp_resume_groups" not in st.session_state:
    with st.spinner("📂 Loading files from SharePoint…"):
        try:
            _b = _make_sp_updater()
            st.session_state["sp_resume_groups"] = _b.list_resumes_grouped()
            st.session_state["sp_jd_files"]      = _b.list_jd_files()
        except Exception as _load_err:
            st.session_state["sp_resume_groups"] = {}
            st.session_state["sp_jd_files"]      = []
            st.error(f"❌ Could not load SharePoint files: {_load_err}")

# ── Refresh button ────────────────────────────────────────────────────────────
_ref_col, _ = st.columns([1, 5])
with _ref_col:
    if st.button("🔄 Refresh", key="sp_refresh", help="Re-fetch file lists from SharePoint"):
        for _k in [k for k in st.session_state if
                   k in ("sp_resume_groups", "sp_jd_files") or k.startswith("sp_content_")]:
            del st.session_state[_k]
        st.rerun()

# ── Initialise text from last successful download (survives rerenders) ────────
resume_text = st.session_state.get("active_resume_text", "")
jd_text     = st.session_state.get("active_jd_text",     "")

# ── Two-column file selectors ─────────────────────────────────────────────────
col_r, col_j = st.columns(2)

with col_r:
    st.markdown('<div class="rs-card">', unsafe_allow_html=True)
    st.markdown('<div class="rs-section-header">📄 Resume</div>', unsafe_allow_html=True)

    if not sp_enabled:
        st.warning("SharePoint not connected — check .env variables in the sidebar.")
    else:
        _resume_groups = st.session_state.get("sp_resume_groups", {})
        if not _resume_groups:
            st.info(f"No subfolders found in `{SP_TEXT_RESUMES_FOLDER}`.")
        else:
            # Step 1 — pick job-role subfolder
            _sf_opts = ["— Select job role —"] + sorted(_resume_groups.keys())
            _chosen_sf = st.selectbox(
                "Job Role / Subfolder",
                options=_sf_opts,
                key="sp_sf_select",
                label_visibility="collapsed",
            )

            if _chosen_sf and _chosen_sf != "— Select job role —":
                # ── Detect subfolder change → clear previous results ──────────
                if st.session_state.get("_prev_sf") != _chosen_sf:
                    _clear_session_for_new_resume()
                    st.session_state["_prev_sf"]    = _chosen_sf
                    st.session_state["_prev_rf"]    = None
                    # ── Auto-select matching JD by job ID ────────────────────
                    _job_id       = _extract_job_id_from_subfolder(_chosen_sf)
                    _auto_jd      = _find_jd_for_job_id(
                        _job_id, st.session_state.get("sp_jd_files", [])
                    )
                    if _auto_jd:
                        st.session_state["sp_jd_select"] = _auto_jd["name"]

                # Auto-populate role_name from the chosen subfolder
                st.session_state["role_name"] = _chosen_sf

                # Step 2 — pick file within subfolder
                _sf_files  = _resume_groups[_chosen_sf]
                _file_opts = ["— Select resume —"] + [f["name"] for f in _sf_files]
                _chosen_rf = st.selectbox(
                    "Resume File",
                    options=_file_opts,
                    key="sp_resume_select",
                    label_visibility="collapsed",
                )

                # ── Detect file change → clear previous results ───────────────
                if _chosen_rf != "— Select resume —":
                    if st.session_state.get("_prev_rf") != _chosen_rf:
                        _clear_session_for_new_resume()
                        st.session_state["_prev_rf"] = _chosen_rf
                    _r_item    = next(f for f in _sf_files if f["name"] == _chosen_rf)
                    _r_cache   = f"sp_content_{_r_item['id']}"

                    # Download once, cache by item ID
                    if _r_cache not in st.session_state:
                        with st.spinner(f"Downloading `{_chosen_rf}` from SharePoint…"):
                            try:
                                st.session_state[_r_cache] = _make_sp_updater().download_text_content(_r_item["id"])
                            except Exception as _dl_err:
                                st.session_state[_r_cache] = ""
                                st.error(f"❌ Download failed: {_dl_err}")

                    _r_content = st.session_state.get(_r_cache, "")
                    if _r_content:
                        resume_text = _r_content
                        st.session_state["active_resume_text"] = _r_content
                        st.session_state["resume_filename"]    = _chosen_rf
                        st.success(f"✅ {_chosen_rf} — **{len(_r_content.split()):,}** words")
                        with st.expander("Preview", expanded=False):
                            st.text_area("", value=_r_content[:1500] + ("…" if len(_r_content) > 1500 else ""),
                                         height=160, key="preview_resume", label_visibility="collapsed")
                    else:
                        st.warning("File downloaded but appears empty.")

    st.markdown("</div>", unsafe_allow_html=True)

with col_j:
    st.markdown('<div class="rs-card">', unsafe_allow_html=True)
    st.markdown('<div class="rs-section-header">📋 Job Description</div>', unsafe_allow_html=True)

    if not sp_enabled:
        st.warning("SharePoint not connected — check .env variables in the sidebar.")
    else:
        _jd_files = st.session_state.get("sp_jd_files", [])
        if not _jd_files:
            st.info(f"No .txt files found in `{SP_TEXT_JD_FOLDER}`.")
        else:
            _jd_opts    = ["— Select JD —"] + [f["name"] for f in _jd_files]
            _chosen_jdf = st.selectbox(
                "Job Description",
                options=_jd_opts,
                key="sp_jd_select",
                label_visibility="collapsed",
            )

            if _chosen_jdf and _chosen_jdf != "— Select JD —":
                _jd_item  = next(f for f in _jd_files if f["name"] == _chosen_jdf)
                _jd_cache = f"sp_content_{_jd_item['id']}"

                # Download once, cache by item ID
                if _jd_cache not in st.session_state:
                    with st.spinner(f"Downloading `{_chosen_jdf}` from SharePoint…"):
                        try:
                            st.session_state[_jd_cache] = _make_sp_updater().download_text_content(_jd_item["id"])
                        except Exception as _dl_err:
                            st.session_state[_jd_cache] = ""
                            st.error(f"❌ Download failed: {_dl_err}")

                _jd_content = st.session_state.get(_jd_cache, "")
                if _jd_content:
                    jd_text = _jd_content
                    st.session_state["active_jd_text"] = _jd_content
                    st.success(f"✅ {_chosen_jdf} — **{len(_jd_content.split()):,}** words")
                    with st.expander("Preview", expanded=False):
                        st.text_area("", value=_jd_content[:1500] + ("…" if len(_jd_content) > 1500 else ""),
                                     height=160, key="preview_jd", label_visibility="collapsed")
                else:
                    st.warning("File downloaded but appears empty.")

    st.markdown("</div>", unsafe_allow_html=True)


# ── Paste text override (fallback — clears SP selection for that slot) ────────
with st.expander("✏️  Paste Text Directly (override SharePoint selection)", expanded=False):
    col_pt1, col_pt2 = st.columns(2)
    with col_pt1:
        paste_resume = st.text_area("Paste Resume Text", height=200, key="paste_resume",
                                    placeholder="Paste raw resume text here to override SharePoint selection…")
        if paste_resume.strip():
            resume_text = paste_resume
            st.session_state["active_resume_text"] = paste_resume
            st.session_state["resume_filename"]    = ""   # no filename for pasted text
    with col_pt2:
        paste_jd = st.text_area("Paste JD Text", height=200, key="paste_jd",
                                placeholder="Paste raw JD text here to override SharePoint selection…")
        if paste_jd.strip():
            jd_text = paste_jd
            st.session_state["active_jd_text"] = paste_jd


# ═══════════════════════════════════════════════════════════════════════════
# STATUS BAR
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("")
status_col1, status_col2, status_col3, status_col4 = st.columns(4)
with status_col1:
    icon = "✅" if resume_text else "⭕"
    words = len(resume_text.split()) if resume_text else 0
    st.markdown(f"""<div style="text-align:center;background:#161a24;border:1px solid rgba(255,255,255,0.07);border-radius:10px;padding:0.8rem;">
        <div style="font-size:1.3rem">{icon}</div>
        <div style="font-size:0.7rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;margin-top:0.2rem">Resume</div>
        <div style="font-size:0.85rem;color:#c8ccd8;">{words:,} words</div>
    </div>""", unsafe_allow_html=True)

with status_col2:
    icon = "✅" if jd_text else "⭕"
    words = len(jd_text.split()) if jd_text else 0
    st.markdown(f"""<div style="text-align:center;background:#161a24;border:1px solid rgba(255,255,255,0.07);border-radius:10px;padding:0.8rem;">
        <div style="font-size:1.3rem">{icon}</div>
        <div style="font-size:0.7rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;margin-top:0.2rem">Job Description</div>
        <div style="font-size:0.85rem;color:#c8ccd8;">{words:,} words</div>
    </div>""", unsafe_allow_html=True)

with status_col3:
    role_display = st.session_state.get("role_name", "") or "Not set"
    st.markdown(f"""<div style="text-align:center;background:#161a24;border:1px solid rgba(255,255,255,0.07);border-radius:10px;padding:0.8rem;">
        <div style="font-size:1.3rem">{'✅' if st.session_state.get('role_name') else '⭕'}</div>
        <div style="font-size:0.7rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;margin-top:0.2rem">Role</div>
        <div style="font-size:0.78rem;color:#c8ccd8;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{role_display}">{role_display[:22]}{'…' if len(role_display)>22 else ''}</div>
    </div>""", unsafe_allow_html=True)

with status_col4:
    api_ready = bool(st.session_state.get("api_url", "").strip())
    st.markdown(f"""<div style="text-align:center;background:#161a24;border:1px solid rgba(255,255,255,0.07);border-radius:10px;padding:0.8rem;">
        <div style="font-size:1.3rem">{'✅' if api_ready else '⭕'}</div>
        <div style="font-size:0.7rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;margin-top:0.2rem">API Server</div>
        <div style="font-size:0.78rem;color:#c8ccd8;">{st.session_state.get("api_url","—")[:25]}</div>
    </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# ANALYSE BUTTON
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("")
ready = bool(resume_text.strip()) and bool(jd_text.strip())

if not ready:
    st.info("⬆  Load a **Resume** and a **Job Description** to enable analysis.")

col_btn, col_clear = st.columns([3, 1])
with col_btn:
    analyse_clicked = st.button(
        "🔍  Analyse Resume",
        disabled=not ready,
        use_container_width=True,
        type="primary",
    )
with col_clear:
    if st.button("🗑  Clear Results", use_container_width=True):
        if "last_result" in st.session_state:
            del st.session_state["last_result"]
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

if analyse_clicked and ready:
    api_server = st.session_state.get("api_url", "http://localhost:8000").strip()
    if not api_server:
        st.error("Please enter a FastAPI Server URL in the sidebar.")
    else:
        with st.spinner("🤖 Sending to AI analysis pipeline… this may take up to 60 seconds"):
            prog = st.progress(0, text="Connecting to API…")
            time.sleep(0.3)
            prog.progress(20, text="Sending resume + JD…")

            result = call_api(api_server, resume_text, jd_text)

            prog.progress(80, text="Processing response…")
            time.sleep(0.2)
            prog.progress(100, text="Done!")
            time.sleep(0.3)
            prog.empty()

        if result:
            st.session_state["last_result"] = result
            # Persist the resume filename so the SP push button knows what to update
            st.session_state["last_resume_filename"] = st.session_state.get("resume_filename", "")

            # Auto-save
            role = st.session_state.get("role_name", "General").strip() or "General"
            full_name = (
                result
                .get("function_2_resume_data_extraction", {})
                .get("personal_information", {})
                .get("full_name", "Unknown")
            )
            try:
                saved_path = save_result(result, role, full_name)
                st.toast(f"✅ Auto-saved to {saved_path}", icon="💾")
            except Exception as e:
                st.toast(f"⚠ Could not auto-save: {e}", icon="⚠️")

            # ── Auto-push MatchScore to SharePoint ──────────────────────────
            _sp_cfg      = st.session_state.get("sp_config", {})
            _sp_enabled  = _sp_cfg.get("enabled", False)
            _resume_fn   = st.session_state.get("resume_filename", "")
            # .txt → .pdf swap for the PDF column target
            if _resume_fn.lower().endswith(".txt"):
                _pdf_fn = Path(_resume_fn).stem + ".pdf"
            else:
                _pdf_fn = _resume_fn

            if _sp_enabled and _pdf_fn:
                _raw_score = (
                    result
                    .get("function_1_resume_jd_matching", {})
                    .get("overall_match_score", 0)
                )
                _new_score = round(_raw_score)
                _role_hint = st.session_state.get("role_name", "")
                try:
                    _updater   = _make_sp_updater()
                    with st.spinner("Checking existing MatchScore on SharePoint…"):
                        _item_id, _old_score = _updater.fetch_match_score(
                            _pdf_fn, role_hint=_role_hint
                        )

                    if _old_score is not None:
                        # ── Existing score found — prompt user to choose ──────
                        _avg_score = round((_old_score + _new_score) / 2)
                        st.session_state["score_comparison"] = {
                            "old":      _old_score,
                            "new":      _new_score,
                            "avg":      _avg_score,
                            "item_id":  _item_id,
                            "filename": _pdf_fn,
                        }
                        st.session_state["auto_push_status"] = (
                            "score_conflict",
                            f"Existing score **{_old_score}** found — see prompt below.",
                        )
                        st.toast(
                            f"⚠️ Previous MatchScore {_old_score} found — choose below",
                            icon="⚠️",
                        )
                    else:
                        # ── No prior score — push immediately ─────────────────
                        with st.spinner(f"Pushing MatchScore = {_new_score}…"):
                            _status, _msg, _candidates = _updater.push_match_score(
                                _pdf_fn, _new_score,
                                role_hint=_role_hint,
                                confirmed_item_id=_item_id or "",
                            )
                        if _status == "OK":
                            st.session_state["auto_push_status"] = ("ok", _msg)
                            st.toast(
                                f"📤 MatchScore = {_new_score} pushed to SharePoint",
                                icon="✅",
                            )
                        elif _status == "NEEDS_CONFIRM":
                            st.session_state["auto_push_status"]      = ("needs_confirm", _msg)
                            st.session_state["sp_confirm_candidates"] = _candidates
                            st.session_state["sp_confirm_score"]      = _new_score
                            st.session_state["sp_confirm_filename"]   = _pdf_fn
                            st.toast(
                                "⚠️ Multiple files found — confirm in the results panel",
                                icon="⚠️",
                            )
                        else:
                            st.session_state["auto_push_status"] = ("error", _msg)
                            st.toast("⚠️ Auto-push failed — use manual button below", icon="⚠️")
                except Exception as _push_err:
                    st.session_state["auto_push_status"] = ("error", str(_push_err))
                    st.toast("⚠️ Auto-push error — use manual button below", icon="⚠️")
            elif _sp_enabled and not _pdf_fn:
                st.session_state["auto_push_status"] = (
                    "skipped", "No filename detected — use manual push below."
                )


# ═══════════════════════════════════════════════════════════════════════════
# RESULTS DISPLAY
# ═══════════════════════════════════════════════════════════════════════════

if "last_result" in st.session_state:
    st.markdown('<div class="rs-divider"></div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="display:flex;align-items:center;gap:0.7rem;margin-bottom:1.5rem;">
        <div style="font-family:'DM Serif Display',serif;font-size:1.6rem;color:#e8eaf2;">Analysis Results</div>
        <span class="info-pill">✓ Complete</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Score Comparison Dialog ───────────────────────────────────────────────
    _sc = st.session_state.get("score_comparison")
    if _sc:
        _old = _sc["old"]
        _new = _sc["new"]
        _avg = _sc["avg"]
        _sc_item_id  = _sc["item_id"]
        _sc_filename = _sc["filename"]

        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg,#1e2235,#161a24);
            border: 1px solid #f59e0b;
            border-left: 4px solid #f59e0b;
            border-radius: 12px;
            padding: 1.4rem 1.6rem 1.2rem;
            margin-bottom: 1.5rem;
        ">
            <div style="font-size:0.72rem;color:#f59e0b;text-transform:uppercase;
                        letter-spacing:0.12em;font-weight:700;margin-bottom:0.7rem;">
                ⚠ Existing MatchScore Found on SharePoint
            </div>
            <div style="font-size:0.88rem;color:#9ca3af;margin-bottom:1rem;">
                A score already exists for <code style="color:#c8ccd8">{_sc_filename}</code>.
                Choose which score to save:
            </div>
            <div style="display:flex;gap:1.5rem;flex-wrap:wrap;margin-bottom:0.4rem;">
                <div style="text-align:center;min-width:90px;">
                    <div style="font-size:2rem;font-weight:700;color:#6b7280;">{_old}</div>
                    <div style="font-size:0.7rem;color:#6b7280;text-transform:uppercase;
                                letter-spacing:0.1em;margin-top:0.2rem;">Previous</div>
                </div>
                <div style="text-align:center;min-width:90px;">
                    <div style="font-size:2rem;font-weight:700;color:#818cf8;">{_new}</div>
                    <div style="font-size:0.7rem;color:#818cf8;text-transform:uppercase;
                                letter-spacing:0.1em;margin-top:0.2rem;">New</div>
                </div>
                <div style="text-align:center;min-width:90px;">
                    <div style="font-size:2rem;font-weight:700;color:#34d399;">{_avg}</div>
                    <div style="font-size:0.7rem;color:#34d399;text-transform:uppercase;
                                letter-spacing:0.1em;margin-top:0.2rem;">Average</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        _btn_col1, _btn_col2, _btn_col3, _btn_col4 = st.columns([1, 1, 1, 1])

        def _do_score_push(chosen: int) -> None:
            """Push `chosen` score, clear dialog, show toast."""
            try:
                _s, _m, _ = _make_sp_updater().push_match_score(
                    _sc_filename, chosen,
                    confirmed_item_id=_sc_item_id or "",
                )
                if _s == "OK":
                    st.session_state["auto_push_status"] = ("ok", _m)
                    st.toast(f"📤 MatchScore = {chosen} saved to SharePoint", icon="✅")
                else:
                    st.session_state["auto_push_status"] = ("error", _m)
                    st.toast(f"❌ Push failed: {_m[:80]}", icon="❌")
            except Exception as _e:
                st.session_state["auto_push_status"] = ("error", str(_e))
                st.toast(f"❌ Push error: {_e}", icon="❌")
            st.session_state.pop("score_comparison", None)

        with _btn_col1:
            if st.button(
                f"Keep Previous  ({_old})",
                use_container_width=True,
                key="sc_keep_old",
                help=f"Write the previous score ({_old}) back to SharePoint — no change in value",
            ):
                _do_score_push(_old)
                st.rerun()

        with _btn_col2:
            if st.button(
                f"Use New  ({_new})",
                use_container_width=True,
                type="primary",
                key="sc_use_new",
                help=f"Overwrite with the new score ({_new})",
            ):
                _do_score_push(_new)
                st.rerun()

        with _btn_col3:
            if st.button(
                f"Use Average  ({_avg})",
                use_container_width=True,
                key="sc_use_avg",
                help=f"Save the rounded average of both scores ({_old} + {_new}) ÷ 2 = {_avg}",
            ):
                _do_score_push(_avg)
                st.rerun()

        with _btn_col4:
            if st.button(
                "Dismiss",
                use_container_width=True,
                key="sc_dismiss",
                help="Don't update SharePoint right now — use the manual push button below",
            ):
                st.session_state.pop("score_comparison", None)
                st.session_state["auto_push_status"] = (
                    "skipped", "Score comparison dismissed — use manual push button below."
                )
                st.rerun()
    # ── end score comparison dialog ───────────────────────────────────────────

    render_result(
        st.session_state["last_result"],
        st.session_state.get("role_name", "General"),
        resume_filename=st.session_state.get("last_resume_filename", ""),
    )


# ═══════════════════════════════════════════════════════════════════════════
# SAVED RESULTS BROWSER
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("")
with st.expander("📁  Browse Saved Results (extracted_json_output/)", expanded=False):
    out_base = Path("extracted_json_output")
    if not out_base.exists():
        st.info("No saved results yet.")
    else:
        saved_files = sorted(out_base.rglob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not saved_files:
            st.info("No saved results yet.")
        else:
            st.caption(f"Found **{len(saved_files)}** saved result(s).")
            for sf in saved_files[:30]:  # Show latest 30
                rel = sf.relative_to(out_base)
                cols = st.columns([4, 1, 1])
                with cols[0]:
                    st.markdown(f"📄 `{rel}`")
                with cols[1]:
                    mtime = datetime.fromtimestamp(sf.stat().st_mtime).strftime("%m/%d %H:%M")
                    st.caption(mtime)
                with cols[2]:
                    try:
                        data = json.loads(sf.read_text(encoding="utf-8"))
                        if st.button("View", key=f"view_{sf}"):
                            st.session_state["last_result"] = data
                            st.rerun()
                    except Exception:
                        st.caption("parse error")