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
# HELPERS — Scan local folder for files
# ═══════════════════════════════════════════════════════════════════════════

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


def render_result(result: dict, role: str):
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

    # ── Save ─────────────────────────────────────────────────────────────
    st.markdown("")
    col_save, col_dl = st.columns([2, 1])
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
    st.markdown('<span style="font-size:0.75rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;">📁 Local Folder Scan</span>', unsafe_allow_html=True)
    st.caption("Optionally point to local folders to browse files")

    resume_folder = st.text_input("Resume Folder Path", value="", placeholder="e.g. extracted_txt_resumes/")
    jd_folder = st.text_input("JD Folder Path", value="", placeholder="e.g. extracted_txt_jd/")

    st.markdown("---")
    st.markdown("""
    <div style="font-size:0.75rem; color:#374151; line-height:1.6;">
        <div style="color:#6b7280; font-weight:600; margin-bottom:0.4rem;">HOW IT WORKS</div>
        1. Upload or select Resume & JD<br>
        2. Text is extracted automatically<br>
        3. Sent to FastAPI <code style="color:#818cf8">/analyze-resume</code><br>
        4. Results displayed & saved
    </div>
    """, unsafe_allow_html=True)

    # Dependencies check
    st.markdown("---")
    st.markdown('<span style="font-size:0.75rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;">📦 Dependencies</span>', unsafe_allow_html=True)
    dep_col1, dep_col2 = st.columns(2)
    with dep_col1:
        st.markdown(f"{'✅' if HAS_PYPDF else '❌'} PyPDF2")
        st.markdown(f"{'✅' if HAS_DOCX else '❌'} python-docx")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN PAGE
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("""
<div style="padding: 2rem 0 0.5rem 0;">
    <div class="rs-title">Resume Screener</div>
    <div class="rs-subtitle">AI-Powered Resume × Job Description Analysis</div>
</div>
""", unsafe_allow_html=True)

# ── Scan local folders if configured ────────────────────────────────────────
local_resume_files = scan_folder(st.session_state.get("api_url_input", ""))  # placeholder
if resume_folder:
    local_resume_files = scan_folder(resume_folder)
if jd_folder:
    local_jd_files = scan_folder(jd_folder)
else:
    local_jd_files = []

# ═══════════════════════════════════════════════════════════════════════════
# INPUT SECTION — Two columns
# ═══════════════════════════════════════════════════════════════════════════

tab_upload, tab_local = st.tabs(["📤  Upload Files", "📂  Browse Local Files"])

resume_text = ""
jd_text = ""

with tab_upload:
    col_r, col_j = st.columns(2)

    with col_r:
        st.markdown('<div class="rs-card">', unsafe_allow_html=True)
        st.markdown('<div class="rs-section-header">📄 Resume</div>', unsafe_allow_html=True)
        uploaded_resume = st.file_uploader(
            "Upload Resume",
            type=["pdf", "docx", "txt"],
            key="upload_resume",
            label_visibility="collapsed",
        )
        if uploaded_resume:
            with st.spinner("Extracting resume text…"):
                resume_text = extract_text(uploaded_resume)
            if resume_text:
                st.success(f"✅ Extracted **{len(resume_text.split())}** words")
                with st.expander("Preview", expanded=False):
                    st.text_area("", value=resume_text[:1500] + ("…" if len(resume_text) > 1500 else ""), height=180, key="preview_resume", label_visibility="collapsed")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_j:
        st.markdown('<div class="rs-card">', unsafe_allow_html=True)
        st.markdown('<div class="rs-section-header">📋 Job Description</div>', unsafe_allow_html=True)
        uploaded_jd = st.file_uploader(
            "Upload Job Description",
            type=["pdf", "docx", "txt"],
            key="upload_jd",
            label_visibility="collapsed",
        )
        if uploaded_jd:
            with st.spinner("Extracting JD text…"):
                jd_text = extract_text(uploaded_jd)
            if jd_text:
                st.success(f"✅ Extracted **{len(jd_text.split())}** words")
                with st.expander("Preview", expanded=False):
                    st.text_area("", value=jd_text[:1500] + ("…" if len(jd_text) > 1500 else ""), height=180, key="preview_jd", label_visibility="collapsed")
        st.markdown("</div>", unsafe_allow_html=True)

with tab_local:
    col_rl, col_jl = st.columns(2)

    with col_rl:
        st.markdown('<div class="rs-card">', unsafe_allow_html=True)
        st.markdown('<div class="rs-section-header">📄 Resume — Local File</div>', unsafe_allow_html=True)
        if local_resume_files:
            chosen_resume = st.selectbox(
                "Select Resume",
                options=["— Select a file —"] + local_resume_files,
                key="local_resume_select",
                label_visibility="collapsed",
            )
            if chosen_resume != "— Select a file —":
                with st.spinner("Reading…"):
                    resume_text = extract_text_from_path(chosen_resume)
                if resume_text:
                    st.success(f"✅ {Path(chosen_resume).name} — **{len(resume_text.split())}** words")
        else:
            st.info("Set a **Resume Folder Path** in the sidebar to browse local files.")
        
        # Manual path entry fallback
        manual_resume = st.text_input("Or enter file path manually", key="manual_resume", placeholder="/path/to/resume.pdf")
        if manual_resume and Path(manual_resume).exists():
            with st.spinner("Reading…"):
                resume_text = extract_text_from_path(manual_resume)
            if resume_text:
                st.success(f"✅ {Path(manual_resume).name} — **{len(resume_text.split())}** words")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_jl:
        st.markdown('<div class="rs-card">', unsafe_allow_html=True)
        st.markdown('<div class="rs-section-header">📋 Job Description — Local File</div>', unsafe_allow_html=True)
        if local_jd_files:
            chosen_jd = st.selectbox(
                "Select JD",
                options=["— Select a file —"] + local_jd_files,
                key="local_jd_select",
                label_visibility="collapsed",
            )
            if chosen_jd != "— Select a file —":
                with st.spinner("Reading…"):
                    jd_text = extract_text_from_path(chosen_jd)
                if jd_text:
                    st.success(f"✅ {Path(chosen_jd).name} — **{len(jd_text.split())}** words")
        else:
            st.info("Set a **JD Folder Path** in the sidebar to browse local files.")

        manual_jd = st.text_input("Or enter file path manually", key="manual_jd", placeholder="/path/to/jd.pdf")
        if manual_jd and Path(manual_jd).exists():
            with st.spinner("Reading…"):
                jd_text = extract_text_from_path(manual_jd)
            if jd_text:
                st.success(f"✅ {Path(manual_jd).name} — **{len(jd_text.split())}** words")
        st.markdown("</div>", unsafe_allow_html=True)


# ── Paste text directly (override) ──────────────────────────────────────────
with st.expander("✏️  Paste Text Directly (override extracted text)", expanded=False):
    col_pt1, col_pt2 = st.columns(2)
    with col_pt1:
        paste_resume = st.text_area("Paste Resume Text", height=200, key="paste_resume", placeholder="Paste raw resume text here to override file extraction…")
        if paste_resume.strip():
            resume_text = paste_resume
    with col_pt2:
        paste_jd = st.text_area("Paste JD Text", height=200, key="paste_jd", placeholder="Paste raw JD text here to override file extraction…")
        if paste_jd.strip():
            jd_text = paste_jd


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

    render_result(
        st.session_state["last_result"],
        st.session_state.get("role_name", "General"),
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