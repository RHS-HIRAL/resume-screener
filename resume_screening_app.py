"""
resume_screening_app.py
───────────────────────
Streamlit UI for the Resume Screening Pipeline.

Workflow:
  1. Load local JD JSONs from extracted_json_jd/ (pre-extracted structured fields).
  2. Load extracted résumé JSONs from extracted_json_resumes/.
  3. User selects a role + JD → "Run Screening" scores each résumé.
  4. Results displayed as a ranked scoreboard with expandable per-candidate details.
"""

import json
import os
import sys
import shutil
import time
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd

# ── Ensure project root is on sys.path ────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from resume_screener_pipeline.pipeline import (
    WeightedScorer,
    resume_json_to_text,
    RESULTS_DIR,
    PARSED_JD_DIR,
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

SCREENING_RESULTS_DIR = PROJECT_ROOT / "screening_results"
SCREENING_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# JD JSON files directory (output of new_app.py pipeline)
JD_JSON_DIR = PROJECT_ROOT / "extracted_json_jd"


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def get_extracted_roles() -> list[str]:
    """List job role folders that have extracted JSONs in extracted_json_resumes/."""
    if not RESULTS_DIR.exists():
        return []
    roles = []
    for d in sorted(RESULTS_DIR.iterdir()):
        if d.is_dir():
            jsons = [f for f in d.glob("*.json") if not f.name.startswith("_")]
            if jsons:
                roles.append(d.name)
    return roles


def count_resume_jsons(role_name: str) -> int:
    """Count non-manifest JSON files for a role."""
    role_dir = RESULTS_DIR / role_name
    if not role_dir.exists():
        return 0
    return len([f for f in role_dir.glob("*.json") if not f.name.startswith("_")])


def get_available_jds() -> list[dict]:
    """List all JD JSON files with their title and filename."""
    if not JD_JSON_DIR.exists():
        return []
    jds = []
    for jf in sorted(JD_JSON_DIR.glob("JD_*.json")):
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
            jds.append({
                "filename": jf.name,
                "title": data.get("title", jf.stem),
                "location": data.get("location", ""),
                "department": data.get("department", ""),
                "experience": data.get("experience", ""),
                "slug": data.get("slug", ""),
            })
        except Exception:
            jds.append({"filename": jf.name, "title": jf.stem, "location": "", "department": "", "experience": "", "slug": ""})
    return jds


def load_jd_json(filename: str) -> dict:
    """Load a JD JSON file by filename."""
    jd_path = JD_JSON_DIR / filename
    return json.loads(jd_path.read_text(encoding="utf-8"))


def load_resume_jsons(role_name: str) -> list[dict]:
    """
    Load all resume JSONs for a role from extracted_json_resumes/.
    Returns list of {"id", "text", "metadata", "raw_json"} dicts.
    """
    role_dir = RESULTS_DIR / role_name
    if not role_dir.exists():
        return []
    docs = []
    for jf in sorted(role_dir.glob("*.json")):
        if jf.name.startswith("_"):
            continue
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
            text = resume_json_to_text(data)
            if text and len(text.strip()) > 30:
                docs.append({
                    "id": jf.stem,
                    "text": text,
                    "metadata": {
                        "filename": jf.name,
                        "source_pdf": data.get("_source_file", ""),
                        "full_name": data.get("full_name", ""),
                    },
                    "raw_json": data,
                })
        except Exception:
            pass
    return docs


def load_resume_jsons_preview(role_name: str) -> list[dict]:
    """Load resume JSONs with basic info for preview."""
    role_dir = RESULTS_DIR / role_name
    previews = []
    for jf in sorted(role_dir.glob("*.json")):
        if jf.name.startswith("_"):
            continue
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
            previews.append({
                "filename": jf.name,
                "full_name": data.get("full_name", "Unknown"),
                "current_title": data.get("current_job_title", "N/A"),
                "source_pdf": data.get("_source_file", ""),
                "skills_count": len(data.get("explicit_skillset", [])),
                "jobs_count": len(data.get("job_history", [])),
            })
        except Exception:
            previews.append({
                "filename": jf.name,
                "full_name": "Error reading file",
                "current_title": "N/A",
                "source_pdf": "",
                "skills_count": 0,
                "jobs_count": 0,
            })
    return previews


def save_screening_results(role_name: str, results: list[dict], jd_title: str) -> Path:
    """Save screening results to a JSON file."""
    out_dir = SCREENING_RESULTS_DIR / role_name
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"screening_{ts}.json"
    payload = {
        "role": role_name,
        "jd_title": jd_title,
        "screened_at": datetime.now().isoformat(),
        "total_candidates": len(results),
        "results": results,
    }
    out_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_file


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMLIT UI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(
        page_title="Resume Screener",
        page_icon="🎯",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # ── Custom CSS ────────────────────────────────────────────────────────
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }

        /* Main header gradient */
        .main-header {
            background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
            padding: 2rem 2.5rem;
            border-radius: 16px;
            margin-bottom: 1.5rem;
            box-shadow: 0 8px 32px rgba(48, 43, 99, 0.3);
        }
        .main-header h1 {
            color: #ffffff !important;
            margin: 0;
            font-size: 2rem;
            font-weight: 800;
            letter-spacing: -0.5px;
        }
        .main-header p {
            color: rgba(255, 255, 255, 0.80) !important;
            margin: 0.4rem 0 0 0;
            font-size: 0.95rem;
            font-weight: 300;
        }

        /* Stats cards */
        .stat-card {
            background: linear-gradient(135deg, rgba(102, 126, 234, 0.08) 0%, rgba(118, 75, 162, 0.08) 100%);
            border: 1px solid rgba(102, 126, 234, 0.20);
            border-radius: 12px;
            padding: 1.2rem 1.4rem;
            text-align: center;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        .stat-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 16px rgba(102, 126, 234, 0.15);
        }
        .stat-card .stat-number {
            font-size: 2.2rem;
            font-weight: 800;
            background: linear-gradient(135deg, #667eea, #764ba2);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin: 0;
        }
        .stat-card .stat-label {
            font-size: 0.82rem;
            font-weight: 500;
            opacity: 0.65;
            margin: 0.3rem 0 0 0;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        /* Score table */
        .score-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 2px 12px rgba(0,0,0,0.06);
        }
        .score-table thead th {
            background: linear-gradient(135deg, #302b63, #24243e);
            color: #ffffff;
            padding: 0.9rem 1rem;
            font-size: 0.82rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.6px;
            text-align: left;
            border: none;
        }
        .score-table thead th:first-child {
            text-align: center;
            width: 60px;
        }
        .score-table tbody tr {
            transition: background-color 0.15s;
        }
        .score-table tbody tr:hover {
            background-color: rgba(102, 126, 234, 0.06);
        }
        .score-table tbody td {
            padding: 0.75rem 1rem;
            font-size: 0.9rem;
            border-bottom: 1px solid rgba(128, 128, 128, 0.12);
        }
        .score-table tbody td:first-child {
            text-align: center;
            font-weight: 700;
            color: #667eea;
        }

        /* Rank badges */
        .rank-badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 32px;
            height: 32px;
            border-radius: 8px;
            font-weight: 700;
            font-size: 0.85rem;
        }
        .rank-1 { background: linear-gradient(135deg, #FFD700, #FFA500); color: #000; }
        .rank-2 { background: linear-gradient(135deg, #C0C0C0, #A0A0A0); color: #000; }
        .rank-3 { background: linear-gradient(135deg, #CD7F32, #B8860B); color: #fff; }
        .rank-other { background: rgba(102, 126, 234, 0.12); color: #667eea; }

        /* Score bars */
        .score-bar-container {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .score-bar {
            flex: 1;
            height: 8px;
            background: rgba(128, 128, 128, 0.12);
            border-radius: 4px;
            overflow: hidden;
        }
        .score-bar-fill {
            height: 100%;
            border-radius: 4px;
            transition: width 0.6s ease;
        }
        .score-bar-fill.hybrid { background: linear-gradient(90deg, #667eea, #764ba2); }
        .score-bar-fill.vector { background: linear-gradient(90deg, #11998e, #38ef7d); }
        .score-bar-fill.bm25   { background: linear-gradient(90deg, #f093fb, #f5576c); }
        .score-value {
            min-width: 48px;
            font-weight: 600;
            font-size: 0.85rem;
            text-align: right;
        }

        /* File list items */
        .resume-card {
            border: 1px solid rgba(128, 128, 128, 0.18);
            border-radius: 10px;
            padding: 0.65rem 1rem;
            margin: 0.3rem 0;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        .resume-card:hover {
            border-color: rgba(102, 126, 234, 0.4);
            box-shadow: 0 2px 8px rgba(102, 126, 234, 0.10);
        }
        .resume-name { font-weight: 600; font-size: 0.92rem; }
        .resume-meta {
            opacity: 0.6;
            font-size: 0.8rem;
            margin-top: 2px;
        }

        /* Pill badges */
        .pill {
            display: inline-block;
            padding: 0.2rem 0.65rem;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            white-space: nowrap;
        }
        .pill-green  { background: rgba(40,167,69,0.12); color: #28a745; }
        .pill-purple { background: rgba(102,126,234,0.12); color: #667eea; }
        .pill-orange { background: rgba(255,165,0,0.12); color: #e69500; }

        /* Section headers */
        .section-header {
            font-size: 1.1rem;
            font-weight: 700;
            margin: 1.5rem 0 0.8rem 0;
            display: flex;
            align-items: center;
            gap: 8px;
        }
    </style>
    """, unsafe_allow_html=True)

    # ── Header ────────────────────────────────────────────────────────────
    st.markdown("""
    <div class="main-header">
        <h1>🎯 Resume Screening Pipeline</h1>
        <p>Compare extracted résumés against job descriptions using vector similarity + keyword matching</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Sidebar ───────────────────────────────────────────────────────────
    with st.sidebar:
        w_project = st.slider(
            "📂 Project Relevance Weight",
            min_value=0.0, max_value=1.0, value=1.0, step=0.05,
            help="Importance of job history and project semantic relevance.",
        )

        scoring_weights = {
            "project_relevance": w_project,
        }

        st.divider()
        st.markdown("### ⚙️ Pipeline Settings")

        top_k_enabled = st.toggle("🔝 Limit Top-K", value=False, help="Only return the top K candidates")
        top_k = None
        if top_k_enabled:
            top_k = st.number_input("Top K", min_value=1, max_value=100, value=10, step=1)

        st.divider()
        st.markdown("### 📂 Data Sources")
        st.caption("Résumé JSONs: `extracted_json_resumes/`")
        st.caption("JD JSONs: `extracted_json_jd/`")

    # ── Load available roles ──────────────────────────────────────────────
    roles = get_extracted_roles()

    if not roles:
        st.warning("⚠️ No extracted résumé data found.")
        st.info("Run the **Resume Extraction Pipeline** first to generate JSON files in `extracted_json_resumes/`.")
        st.stop()

    # ── Role selector ─────────────────────────────────────────────────────
    col_sel, col_info = st.columns([2, 1])

    with col_sel:
        selected_role = st.selectbox(
            "📁 Select Job Role",
            options=roles,
            help="Only roles with extracted résumé JSONs are shown.",
        )

    with col_info:
        n_resumes = count_resume_jsons(selected_role) if selected_role else 0
        st.markdown(f"""
        <div class="stat-card" style="margin-top: 24px;">
            <div class="stat-number">{n_resumes}</div>
            <div class="stat-label">Résumés Available</div>
        </div>
        """, unsafe_allow_html=True)

    if not selected_role:
        st.stop()

    # ── JD file selector ──────────────────────────────────────────────────
    st.markdown('<div class="section-header">📋 Job Description</div>', unsafe_allow_html=True)

    available_jds = get_available_jds()

    if not available_jds:
        st.warning("No JD JSON files found in `extracted_json_jd/`. Run the JD pipeline first.")
        st.stop()

    jd_titles = [jd["title"] for jd in available_jds]

    # Pre-select the best matching JD based on role name word overlap
    role_words = set(selected_role.lower().replace("_", " ").split())
    best_idx = 0
    best_overlap = 0
    for i, jd_info in enumerate(available_jds):
        jd_words = set(jd_info["title"].lower().replace("-", " ").split())
        slug_words = set(jd_info["slug"].lower().replace("-", " ").split())
        overlap = len(role_words & (jd_words | slug_words))
        if overlap > best_overlap:
            best_overlap = overlap
            best_idx = i

    selected_jd_title = st.selectbox(
        "📄 Select Job Description",
        options=jd_titles,
        index=best_idx,
        help="Choose which JD to compare résumés against.",
    )

    # Find the selected JD info
    selected_jd_info = next((jd for jd in available_jds if jd["title"] == selected_jd_title), available_jds[best_idx])
    selected_jd_file = selected_jd_info["filename"]

    # Show JD metadata preview
    meta_parts = []
    if selected_jd_info.get("location"):
        meta_parts.append(f"📍 {selected_jd_info['location']}")
    if selected_jd_info.get("department"):
        meta_parts.append(f"🏢 {selected_jd_info['department']}")
    if selected_jd_info.get("experience"):
        meta_parts.append(f"💼 {selected_jd_info['experience']}")
    if meta_parts:
        st.caption(" · ".join(meta_parts))

    st.markdown("---")

    # ── Résumé preview ────────────────────────────────────────────────────
    st.markdown('<div class="section-header">📋 Extracted Résumés</div>', unsafe_allow_html=True)

    previews = load_resume_jsons_preview(selected_role)

    for p in previews:
        skills_pill = f'<span class="pill pill-green">{p["skills_count"]} skills</span>'
        jobs_pill = f'<span class="pill pill-purple">{p["jobs_count"]} jobs</span>'
        st.markdown(f"""
        <div class="resume-card">
            <div>
                <div class="resume-name">👤 {p['full_name']}</div>
                <div class="resume-meta">{p['current_title']} · {p['source_pdf']}</div>
            </div>
            <div>{skills_pill} {jobs_pill}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # RUN SCREENING
    # ══════════════════════════════════════════════════════════════════════

    col_btn1, col_btn2 = st.columns([1, 1])

    with col_btn1:
        run_clicked = st.button(
            f"🚀 Run Screening ({n_resumes} résumés)",
            type="primary",
            use_container_width=True,
            disabled=n_resumes == 0,
        )

    with col_btn2:
        # Check for previous results
        prev_results_dir = SCREENING_RESULTS_DIR / selected_role
        prev_files = sorted(prev_results_dir.glob("screening_*.json"), reverse=True) if prev_results_dir.exists() else []
        if prev_files:
            latest = json.loads(prev_files[0].read_text(encoding="utf-8"))
            st.download_button(
                label=f"⬇️ Download Latest Results",
                data=json.dumps(latest, indent=2, ensure_ascii=False),
                file_name=f"{selected_role}_screening.json",
                mime="application/json",
                use_container_width=True,
            )

    if run_clicked:
        st.markdown('<div class="section-header">🔄 Screening Progress</div>', unsafe_allow_html=True)

        progress = st.progress(0, text="Loading data…")
        status = st.empty()

        try:
            # Step 1: Load resume JSONs
            progress.progress(0.10, text="Loading extracted résumé JSONs…")
            status.info(f"📂 Loading résumé JSONs from `extracted_json_resumes/{selected_role}/`…")
            resume_docs = load_resume_jsons(selected_role)

            if not resume_docs:
                st.error("❌ No valid résumé data found.")
                st.stop()

            total = len(resume_docs)
            progress.progress(0.20, text=f"Loaded {total} résumé(s). Loading JD…")

            # Step 2: Load JD JSON (structured fields already included)
            status.info(f"📋 Loading JD: `{selected_jd_file}`…")
            jd_parsed = load_jd_json(selected_jd_file)
            jd_title = jd_parsed.get("title", "Unknown JD")
            progress.progress(0.35, text=f"JD loaded: {jd_title}")

            # Step 3: Initialise weighted scorer
            progress.progress(0.45, text="Initialising scorer…")
            scorer = WeightedScorer()

            # Step 4: Score each candidate
            status.info(f"⚖️ Scoring {total} candidates on project match…")
            final_results = []

            for i, doc in enumerate(resume_docs):
                frac = 0.50 + (i / total) * 0.40  # 50% → 90%
                progress.progress(frac, text=f"Scoring candidate {i+1}/{total}…")

                raw = doc.get("raw_json", {})
                meta = doc.get("metadata", {})
                resume_text = doc.get("text", "")

                scores = scorer.score_candidate(
                    resume_json=raw,
                    resume_text=resume_text,
                    jd_parsed=jd_parsed,
                    weights=scoring_weights,
                    strict_education=False,
                )

                final_results.append({
                    "filename":     meta.get("filename", doc["id"]),
                    "full_name":    meta.get("full_name", raw.get("full_name", doc["id"])),
                    "final_score":  round(scores["final_score"], 2),
                    "source_pdf":   meta.get("source_pdf", raw.get("_source_file", "")),
                    "current_title": raw.get("current_job_title", "N/A"),
                    "skills":       raw.get("explicit_skillset", []),
                })

            # Sort by final_score descending
            final_results.sort(
                key=lambda x: x["final_score"],
                reverse=True,
            )
            # Assign ranks
            for rank, r in enumerate(final_results, 1):
                r["rank"] = rank

            if top_k is not None:
                final_results = final_results[:top_k]

            progress.progress(0.95, text="Scoring complete!")

            # Save results
            out_file = save_screening_results(selected_role, final_results, jd_title)
            progress.progress(1.0, text="✅ Screening complete!")
            status.empty()

            # ── Store in session state ────────────────────────────
            st.session_state["screening_results"] = final_results
            st.session_state["screening_jd_title"] = jd_title
            st.session_state["screening_role"] = selected_role
            st.session_state["screening_total"] = total
            st.session_state["screening_file"] = str(out_file)
            st.session_state["screening_weights"] = scoring_weights

            # ── Clean up parsed_jd_cache after work is done ───
            try:
                if PARSED_JD_DIR.exists():
                    shutil.rmtree(PARSED_JD_DIR)
                    PARSED_JD_DIR.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass  # non-critical cleanup

        except Exception as e:
            progress.progress(1.0, text="❌ Error")
            st.error(f"❌ Pipeline error: {e}")
            import traceback
            st.code(traceback.format_exc())
            st.stop()

    # ══════════════════════════════════════════════════════════════════════
    # DISPLAY RESULTS
    # ══════════════════════════════════════════════════════════════════════

    if "screening_results" in st.session_state:
        results = st.session_state["screening_results"]
        jd_title = st.session_state["screening_jd_title"]
        role = st.session_state["screening_role"]
        total = st.session_state["screening_total"]
        disqualified_count = st.session_state.get("screening_disqualified", 0)

        st.markdown("---")

        # ── Summary stats ─────────────────────────────────────────────
        st.markdown('<div class="section-header">📊 Screening Results</div>', unsafe_allow_html=True)
        st.caption(f"JD: **{jd_title}** · Role: `{role}` · {total} résumé(s) scored")

        if results:
            avg_score = sum(r["final_score"] for r in results) / len(results) if results else 0
            best = results[0]

            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown(f"""
                <div class="stat-card">
                    <div class="stat-number">{total}</div>
                    <div class="stat-label">Total Rated</div>
                </div>
                """, unsafe_allow_html=True)
            with c2:
                st.markdown(f"""
                <div class="stat-card">
                    <div class="stat-number">{best['final_score']:.1f}</div>
                    <div class="stat-label">Top Score</div>
                </div>
                """, unsafe_allow_html=True)
            with c3:
                st.markdown(f"""
                <div class="stat-card">
                    <div class="stat-number">{avg_score:.1f}</div>
                    <div class="stat-label">Avg Score</div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("")

            # ── Score table ───────────────────────────────────────────
            st.markdown('<div class="section-header">🏆 Ranked Scoreboard</div>', unsafe_allow_html=True)

            scoreboard_data = []
            for r in results:
                rank = r["rank"]
                medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"#{rank}")
                scoreboard_data.append({
                    "Rank": medal,
                    "Candidate": r["full_name"],
                    "Title": r.get("current_title", "N/A"),
                    "Score": r["final_score"],
                })

            df_scores = pd.DataFrame(scoreboard_data)

            st.dataframe(
                df_scores,
                column_config={
                    "Rank": st.column_config.TextColumn("Rank", width="small"),
                    "Candidate": st.column_config.TextColumn("Candidate", width="medium"),
                    "Title": st.column_config.TextColumn("Title", width="medium"),
                    "Score": st.column_config.ProgressColumn(
                        "Score",
                        format="%.1f",
                        min_value=0,
                        max_value=100,
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )

            st.markdown("")

            # ── Detailed expansion per candidate ──────────────────────
            st.markdown('<div class="section-header">🔍 Candidate Details</div>', unsafe_allow_html=True)

            for r in results:
                with st.expander(
                    f"#{r['rank']}  {r['full_name']}  —  Score: {r['final_score']:.1f}/100"
                ):
                    st.metric("🏆 Match Score", f"{r['final_score']:.1f} / 100")
                    st.caption(f"Source: `{r.get('source_pdf', 'N/A')}`")

                    skills = r.get("skills", [])
                    if skills:
                        st.markdown("**Skills:**")
                        skills_html = " ".join(
                            f'<span class="pill pill-green">{s}</span>' for s in skills[:15]
                        )
                        st.markdown(skills_html, unsafe_allow_html=True)

            st.markdown("---")

            # ── Download ──────────────────────────────────────────────
            st.download_button(
                label="⬇️ Download Full Results (JSON)",
                data=json.dumps(results, indent=2, ensure_ascii=False),
                file_name=f"{role}_screening_results.json",
                mime="application/json",
                use_container_width=True,
            )

            # ── Download as CSV ───────────────────────────────────────
            df = pd.DataFrame([{
                "Rank": r["rank"],
                "Full Name": r["full_name"],
                "Score": r["final_score"],
                "Source PDF": r.get("source_pdf", ""),
                "Current Title": r.get("current_title", ""),
            } for r in results])

            st.download_button(
                label="⬇️ Download Results (CSV)",
                data=df.to_csv(index=False),
                file_name=f"{role}_screening_results.csv",
                mime="text/csv",
                use_container_width=True,
            )




if __name__ == "__main__":
    main()
