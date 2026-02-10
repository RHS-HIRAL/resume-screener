import streamlit as st
import google.generativeai as genai
import os
import PyPDF2 as pdf
from dotenv import load_dotenv
import json
import re
from docx import Document
import pandas as pd
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import io
from pathlib import Path

# Load environment variables
load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))


# ========== LOG DIRECTORY SETUP ==========
# All logs go into a "resume_screener_logs" folder in the user's home directory
LOG_DIR = Path.home() / "resume_screener_logs"
LOG_DIR.mkdir(exist_ok=True)

# File paths
APP_LOG_FILE = LOG_DIR / "app_activity.log"  # General activity logs (.log)
LLM_LOG_FILE = LOG_DIR / "llm_calls.jsonl"  # LLM request/response logs (.jsonl)
RESULTS_EXCEL = (
    LOG_DIR / "resume_screening_results.xlsx"
)  # Single persistent Excel file


# ========== LOGGING SETUP ==========
def setup_logger():
    """File-only logger — nothing goes to the frontend"""
    logger = logging.getLogger("ResumeScreener")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # Prevent leaking to root/streamlit loggers

    if not logger.handlers:
        handler = RotatingFileHandler(
            APP_LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)

    return logger


logger = setup_logger()


def log_llm_call(record: dict):
    """Append a single LLM call record as a JSON line to the JSONL log file"""
    with open(LLM_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ========== SESSION STATE ==========
def init_session_state():
    defaults = {
        "results": None,
        "df": None,
        "processing_done": False,
        "history": [],
        "llm_request_count": 0,
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default


# ========== TEXT EXTRACTION ==========
def extract_text_from_pdf(uploaded_file):
    reader = pdf.PdfReader(uploaded_file)
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def extract_text_from_docx(uploaded_file):
    doc = Document(uploaded_file)
    parts = [para.text for para in doc.paragraphs]
    for table in doc.tables:
        for row in table.rows:
            parts.append(" ".join(cell.text for cell in row.cells))
    return "\n".join(parts)


def extract_text(uploaded_file):
    name = uploaded_file.name.lower()
    if name.endswith(".pdf"):
        return extract_text_from_pdf(uploaded_file)
    elif name.endswith(".docx"):
        return extract_text_from_docx(uploaded_file)
    return ""


# ========== AI-POWERED EXTRACTION ==========
def get_gemini_response(prompt):
    """Call Gemini with full file-based logging"""
    st.session_state.llm_request_count += 1
    req_num = st.session_state.llm_request_count
    ts = datetime.now().isoformat()

    logger.info(f"LLM Request #{req_num} — sending {len(prompt)} chars")

    call_record = {
        "request_num": req_num,
        "timestamp": ts,
        "prompt_length": len(prompt),
        "prompt": prompt,
        "status": "pending",
        "response_length": 0,
        "response": "",
        "error": None,
    }

    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content(prompt)
        text = response.text

        call_record.update(status="success", response_length=len(text), response=text)
        logger.info(f"LLM Request #{req_num} — success ({len(text)} chars)")

    except Exception as e:
        call_record.update(status="error", error=str(e))
        logger.error(f"LLM Request #{req_num} — ERROR: {e}")
        text = f"ERROR: {e}"

    log_llm_call(call_record)
    return text


def extract_all_fields_with_ai(resume_text, jd_text):
    logger.info(
        f"AI extraction start — resume {len(resume_text)} chars, JD {len(jd_text)} chars"
    )

    prompt = f"""
You are an expert resume parser. Extract the following information from this resume with 100% accuracy.

CRITICAL INSTRUCTIONS:
1. Return ONLY valid JSON in the exact format shown below
2. If any field is not found, use "N/A"
3. For name: Look for the actual person's name (usually at the top)
4. For location: Look for city, state, or geographic location (NOT technical terms)
5. For current job: Find the most recent job title and company
6. For LinkedIn: Look for linkedin.com/in/ URLs or LinkedIn profile mentions
7. For job history: Format as "Company - Title - Duration" separated by " | "
8. For total experience: Extract as number only (e.g., "5" for 5 years)
9. Count all distinct jobs/positions in the work history

RESUME TEXT:
{resume_text[:3000]}

JOB DESCRIPTION:
{jd_text[:1000]}

Return JSON in this EXACT format:
{{
    "name": "Full Name Here",
    "role_in_jd": "Job title from JD",
    "current_job_title": "Current position title",
    "current_organization": "Current company name",
    "location": "City, State or geographic location",
    "phone": "Phone number",
    "email": "Email address",
    "linkedin": "LinkedIn profile URL or N/A",
    "other_socials": "Other social media or N/A",
    "total_experience": "X years",
    "job_history": "Company1 - Title1 - Duration1 | Company2 - Title2 - Duration2",
    "total_jobs": "Number of jobs",
    "match_score": "XX%",
    "summary": "Brief professional summary in 2-3 sentences"
}}
"""
    response = get_gemini_response(prompt)

    try:
        json_match = re.search(r"\{.*\}", response, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group(0))
            logger.info(f"AI extraction success — {len(data)} fields parsed")
            return data
    except Exception as e:
        logger.error(f"JSON parsing error: {e}")
        st.error(f"JSON parsing error: {e}")

    return {}


# ========== BACKUP EXTRACTION ==========
def extract_name_backup(text):
    skip_words = {
        "resume",
        "cv",
        "email",
        "phone",
        "address",
        "linkedin",
        "objective",
        "summary",
        "experience",
    }
    for line in text.split("\n")[:8]:
        line = line.strip()
        if not line or any(kw in line.lower() for kw in skip_words):
            continue
        words = line.split()
        if (
            2 <= len(words) <= 4
            and re.match(r"^[A-Za-z\s\.]+$", line)
            and not line.isupper()
        ):
            return line
    return "N/A"


def extract_contact_backup(text):
    email_m = re.search(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", text)
    email = email_m.group(0) if email_m else "N/A"

    phone = "N/A"
    for pat in [
        r"(?:\+91|91)[\s\-]?[6-9]\d{9}",
        r"[6-9]\d{9}",
        r"\d{3}[\s\-]?\d{3}[\s\-]?\d{4}",
    ]:
        m = re.search(pat, text)
        if m:
            phone = m.group(0)
            break

    li_m = re.search(r"linkedin\.com/in/([A-Za-z0-9\-]+)", text, re.IGNORECASE)
    linkedin = f"linkedin.com/in/{li_m.group(1)}" if li_m else "N/A"

    return email, phone, linkedin


def extract_experience_backup(text):
    for pat in [
        r"(?:total|overall)[\s\-:]*(?:experience|exp)[\s\-:]*(\d+(?:\.\d+)?)\s*(?:years?|yrs?)",
        r"(\d+(?:\.\d+)?)\s*(?:years?|yrs?)\s*(?:of\s*)?(?:experience|exp)",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            exp = float(m.group(1))
            if 0 <= exp <= 50:
                return f"{exp} years"
    return "N/A"


# ========== HELPERS ==========
def count_jobs_from_history(job_history):
    if not job_history or job_history == "N/A":
        return 0
    return len([e for e in job_history.split(" | ") if e.strip()])


def extract_numeric_experience(exp_str):
    if not exp_str or exp_str == "N/A":
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)", exp_str)
    return float(m.group(1)) if m else 0


def calculate_average_tenure(total_experience, total_jobs):
    try:
        years = extract_numeric_experience(total_experience)
        jobs = (
            int(total_jobs)
            if str(total_jobs).isdigit()
            else count_jobs_from_history(str(total_jobs))
        )
        return f"{years / jobs:.1f} years" if years > 0 and jobs > 0 else "N/A"
    except Exception:
        return "N/A"


def _pick(ai_val, backup_val):
    """Return AI value if valid, otherwise backup"""
    return ai_val if ai_val and ai_val != "N/A" else backup_val


# ========== MAIN PROCESSING ==========
def process_resume_enhanced(resume_file, jd_text, index, batch_timestamp):
    logger.info(f"Resume #{index}: start — {resume_file.name}")

    try:
        resume_text = extract_text(resume_file)
        if not resume_text or len(resume_text) < 50:
            logger.warning(
                f"Resume #{index}: extraction failed ({len(resume_text or '')} chars)"
            )
            return create_error_record(index, "Text extraction failed", batch_timestamp)

        logger.info(f"Resume #{index}: extracted {len(resume_text)} chars")

        ai = extract_all_fields_with_ai(resume_text, jd_text)
        b_name = extract_name_backup(resume_text)
        b_email, b_phone, b_linkedin = extract_contact_backup(resume_text)
        b_exp = extract_experience_backup(resume_text)

        job_history = ai.get("job_history", "N/A")
        total_jobs = ai.get("total_jobs", count_jobs_from_history(job_history))
        total_exp = _pick(ai.get("total_experience"), b_exp)
        avg_tenure = calculate_average_tenure(total_exp, total_jobs)

        result = {
            "Processed At": batch_timestamp,
            "Sr.no": index,
            "Name": _pick(ai.get("name"), b_name),
            "Role in JD": ai.get("role_in_jd", "N/A"),
            "Current Job Title": ai.get("current_job_title", "N/A"),
            "Current Organization": ai.get("current_organization", "N/A"),
            "Location": ai.get("location", "N/A"),
            "Resume Match Score": ai.get("match_score", "N/A"),
            "Resume Summary": ai.get("summary", "N/A"),
            "Phone": _pick(ai.get("phone"), b_phone),
            "Email": _pick(ai.get("email"), b_email),
            "LinkedIn": _pick(ai.get("linkedin"), b_linkedin),
            "Other Socials": ai.get("other_socials", "N/A"),
            "Total Experience": total_exp,
            "Total Jobs": str(total_jobs)
            if str(total_jobs).isdigit()
            else str(count_jobs_from_history(job_history)),
            "Average Tenure": avg_tenure,
            "Job History (org-title-jobDuration)": job_history,
        }

        logger.info(
            f"Resume #{index}: done — Name={result['Name']}, Score={result['Resume Match Score']}"
        )
        return result

    except Exception as e:
        logger.error(f"Resume #{index}: error — {e}")
        return create_error_record(index, f"Processing error: {e}", batch_timestamp)


def create_error_record(index, error_msg, batch_timestamp):
    fields = [
        "Name",
        "Role in JD",
        "Current Job Title",
        "Current Organization",
        "Location",
        "Other Socials",
        "Total Experience",
        "Total Jobs",
        "Average Tenure",
        "Job History (org-title-jobDuration)",
        "Phone",
        "Email",
        "LinkedIn",
    ]
    record = {"Processed At": batch_timestamp, "Sr.no": index}
    for f in fields:
        record[f] = "N/A"
    record["Resume Match Score"] = "Error"
    record["Resume Summary"] = error_msg
    return record


# ========== EXCEL APPEND ==========
def append_results_to_excel(results: list):
    """Append new results to the single persistent Excel file.
    Creates the file if it doesn't exist; appends rows if it does."""
    new_df = pd.DataFrame(results)

    if RESULTS_EXCEL.exists():
        try:
            existing_df = pd.read_excel(RESULTS_EXCEL, engine="openpyxl")
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception as e:
            logger.error(f"Error reading existing Excel: {e}. Overwriting.")
            combined_df = new_df
    else:
        combined_df = new_df

    combined_df.to_excel(RESULTS_EXCEL, index=False, engine="openpyxl")
    logger.info(f"Excel updated: {RESULTS_EXCEL} — now {len(combined_df)} total rows")
    return combined_df


# ========== RESULTS DISPLAY ==========
def display_results():
    results = st.session_state.results
    df = st.session_state.df

    st.success(f"✅ Successfully processed {len(results)} resumes!")

    st.subheader("📋 Sample Results (First 3 rows)")
    st.dataframe(df.head(3), width="stretch")

    # Stats
    total = len(results)
    successful = sum(1 for r in results if r["Resume Match Score"] != "Error")
    names_extracted = sum(1 for r in results if r["Name"] != "N/A")
    tenure_calculated = sum(1 for r in results if r["Average Tenure"] != "N/A")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📊 Total Resumes", total)
    c2.metric("✅ Successful", successful)
    c3.metric("👤 Names Extracted", names_extracted)
    c4.metric("📈 Tenure Calculated", tenure_calculated)

    # Tenure insights
    tenure_values = []
    for r in results:
        if r["Average Tenure"] != "N/A":
            try:
                tenure_values.append(float(r["Average Tenure"].replace(" years", "")))
            except ValueError:
                pass

    if tenure_values:
        st.subheader("📊 Average Tenure Insights")
        c1, c2, c3 = st.columns(3)
        avg_t = sum(tenure_values) / len(tenure_values)
        c1.metric("📊 Overall Average", f"{avg_t:.1f} years")
        c2.metric("📈 Highest", f"{max(tenure_values):.1f} years")
        c3.metric("📉 Lowest", f"{min(tenure_values):.1f} years")

        st.subheader("📊 Tenure Distribution")
        buckets = [
            ("0-2 years", 0, 2),
            ("2-5 years", 2, 5),
            ("5-10 years", 5, 10),
            ("10+ years", 10, 999),
        ]
        for label, lo, hi in buckets:
            if lo == 0:
                count = sum(1 for t in tenure_values if lo <= t <= hi)
            else:
                count = sum(1 for t in tenure_values if lo < t <= hi)
            pct = count / len(tenure_values) * 100
            st.write(f"• **{label}**: {count} candidates ({pct:.1f}%)")

    with st.expander("📈 View All Results"):
        st.dataframe(df, width="stretch")

    # Downloads — in-memory bytes, no rerun side effects
    excel_buf = io.BytesIO()
    df.to_excel(excel_buf, index=False, engine="openpyxl")

    dl_col1, dl_col2 = st.columns(2)
    with dl_col1:
        st.download_button(
            "📥 Download Results (Excel)",
            data=excel_buf.getvalue(),
            file_name=f"resume_results_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with dl_col2:
        st.download_button(
            "📥 Download Results (CSV)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"resume_results_{datetime.now():%Y%m%d_%H%M%S}.csv",
            mime="text/csv",
        )

    # Quality check
    st.subheader("🔍 Extraction Quality Check")
    checks = [
        ("Names", sum(1 for r in results if r["Name"] != "N/A")),
        ("Emails", sum(1 for r in results if r["Email"] != "N/A")),
        ("Phones", sum(1 for r in results if r["Phone"] != "N/A")),
        ("LinkedIn", sum(1 for r in results if r["LinkedIn"] != "N/A")),
        ("Tenure", tenure_calculated),
        (
            "Job History",
            sum(
                1 for r in results if r["Job History (org-title-jobDuration)"] != "N/A"
            ),
        ),
    ]
    for label, count in checks:
        st.write(f"• **{label}**: {count}/{total} ({count / total * 100:.1f}%)")


def display_history():
    if not st.session_state.history:
        st.info("No processing history yet. Run your first batch above!")
        return

    for i, run in enumerate(reversed(st.session_state.history), 1):
        idx = len(st.session_state.history) - i + 1
        with st.expander(
            f"Run {idx} — {run['timestamp']} — {run['total_resumes']} resumes"
        ):
            st.write(
                f"**Resumes:** {run['total_resumes']} | **Successful:** {run['successful']} | **LLM calls:** {run['llm_calls']}"
            )
            st.dataframe(pd.DataFrame(run["results"]).head(5), width="stretch")


# ========== STREAMLIT UI ==========
def main():
    st.set_page_config(page_title="🎯 Resume Screener", layout="wide")
    init_session_state()

    st.title("🎯 Resume Screener")
    st.markdown(
        "**Fine-tuned AI extraction for precise results with Average Tenure Calculation**"
    )

    # Sidebar
    with st.sidebar:
        st.header("📊 Session Dashboard")
        st.metric("🤖 Total LLM Calls", st.session_state.llm_request_count)
        st.metric("📁 Runs Completed", len(st.session_state.history))

        st.divider()
        st.subheader("📂 Log Files Location")
        st.code(str(LOG_DIR), language=None)
        st.caption("Files saved automatically:")
        st.caption("• `app_activity.log` — activity log")
        st.caption("• `llm_calls.jsonl` — all LLM I/O")
        st.caption("• `resume_screening_results.xlsx` — cumulative results")

        if RESULTS_EXCEL.exists():
            try:
                row_count = len(pd.read_excel(RESULTS_EXCEL, engine="openpyxl"))
                st.metric("📊 Total Rows in Excel", row_count)
            except Exception:
                pass

        st.divider()
        if st.button("🗑️ Clear Current Results"):
            st.session_state.results = None
            st.session_state.df = None
            st.session_state.processing_done = False
            st.rerun()

    # File upload
    col1, col2 = st.columns(2)
    with col1:
        jd_file = st.file_uploader("📋 Upload Job Description", type=["pdf", "docx"])
    with col2:
        resume_files = st.file_uploader(
            "📄 Upload Resume Files", type=["pdf", "docx"], accept_multiple_files=True
        )

    # Processing
    if st.button("🚀 Start Processing", type="primary"):
        if not jd_file or not resume_files:
            st.error("⚠️ Please upload both JD and resume files")
        else:
            jd_text = extract_text(jd_file)
            if not jd_text:
                st.error("❌ Failed to extract JD text")
            else:
                batch_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(
                    f"=== NEW RUN: {len(resume_files)} resumes, JD: {jd_file.name}, batch: {batch_ts} ==="
                )
                llm_before = st.session_state.llm_request_count
                results = []

                with st.status(
                    "🚀 Orchestrating AI Extraction...", expanded=True
                ) as status:
                    progress = st.progress(0)
                    file_display = st.empty()

                    for idx, rf in enumerate(resume_files, 1):
                        file_display.markdown(
                            f"**🔍 Analyzing File {idx} of {len(resume_files)}:** `{rf.name}`"
                        )
                        results.append(
                            process_resume_enhanced(rf, jd_text, idx, batch_ts)
                        )
                        progress.progress(idx / len(resume_files))

                    file_display.empty()
                    status.update(
                        label="✅ AI Analysis Complete!",
                        state="complete",
                        expanded=False,
                    )

                # Persist to session state
                st.session_state.results = results
                st.session_state.df = pd.DataFrame(results)
                st.session_state.processing_done = True

                # Append to persistent Excel
                append_results_to_excel(results)

                # Save to in-memory history
                llm_this_run = st.session_state.llm_request_count - llm_before
                st.session_state.history.append(
                    {
                        "timestamp": batch_ts,
                        "total_resumes": len(results),
                        "successful": sum(
                            1 for r in results if r["Resume Match Score"] != "Error"
                        ),
                        "llm_calls": llm_this_run,
                        "results": results,
                    }
                )
                logger.info(
                    f"=== RUN COMPLETE: {len(results)} resumes, {llm_this_run} LLM calls ==="
                )

    # Display results if available
    if st.session_state.processing_done and st.session_state.results:
        display_results()

    # History
    st.divider()
    with st.expander("📜 Processing History"):
        display_history()


if __name__ == "__main__":
    main()
