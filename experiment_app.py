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
from pathlib import Path
from groq import Groq

# Load environment variables
load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
api_key = os.getenv("GROQ_API_KEY")
client = Groq(api_key=api_key)


# ========== LOG DIRECTORY SETUP ==========
# Logs live alongside the app file itself
APP_DIR = Path(__file__).parent
LOG_DIR = APP_DIR / "resume_screener_logs"
LOG_DIR.mkdir(exist_ok=True)

APP_LOG_FILE = LOG_DIR / "app_activity.log"
LLM_LOG_FILE = LOG_DIR / "llm_calls.jsonl"
RESULTS_EXCEL = LOG_DIR / "resume_screening_results.xlsx"

# Column order for the final Excel — human-readable priority
EXCEL_COLUMNS = [
    "Processed At",
    "Job Description",
    "Name",
    "Email",
    "Phone",
    "LinkedIn",
    "Other Socials",
    "Location",
    "Current Job Title",
    "Current Organization",
    "Total Experience",
    "Total Jobs",
    "Average Tenure",
    "Role in JD",
    "Resume Match Score",
    "Resume Summary",
    "Job History (org-title-jobDuration)",
]


# ========== LOGGING SETUP ==========
def setup_logger():
    """File-only logger with beautified formatting"""
    logger = logging.getLogger("ResumeScreener")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if not logger.handlers:
        handler = RotatingFileHandler(
            APP_LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        fmt = (
            "┌─────────────────────────────────────────────\n"
            "│ %(asctime)s  [%(levelname)-8s]\n"
            "│ %(message)s\n"
            "└─────────────────────────────────────────────"
        )
        handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(handler)

    return logger


logger = setup_logger()


def log_llm_call(record: dict):
    """Write a beautified JSON record per LLM call to the JSONL file"""
    beautified = json.dumps(record, ensure_ascii=False, indent=2)
    separator = "\n" + "=" * 80 + "\n"
    with open(LLM_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(separator)
        f.write(
            f"  LLM CALL #{record['request_num']}  |  {record['timestamp']}  |  Status: {record['status']}\n"
        )
        f.write(separator)
        f.write(beautified + "\n")


# ========== SESSION STATE ==========
def init_session_state():
    defaults = {
        "results": None,
        "df": None,
        "processing_done": False,
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
    # def get_gemini_response(prompt):
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
        logger.info(f"LLM Request #{req_num} — success ({len(text)} chars response)")

    except Exception as e:
        call_record.update(status="error", error=str(e))
        logger.error(f"LLM Request #{req_num} — ERROR: {e}")
        text = f"ERROR: {e}"

    log_llm_call(call_record)
    return text


def get_groq_response(prompt):
    try:
        response = client.chat.completions.create(
            model="groq/compound",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"ERROR: {str(e)}"


def extract_all_fields_with_ai(resume_text, jd_text):
    logger.info(
        f"AI field extraction — resume: {len(resume_text)} chars, JD: {len(jd_text)} chars"
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
    response = get_groq_response(prompt)

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
_SKIP_WORDS = frozenset(
    [
        "resume",
        "cv",
        "email",
        "phone",
        "address",
        "linkedin",
        "objective",
        "summary",
        "experience",
    ]
)


def extract_name_backup(text):
    for line in text.split("\n")[:8]:
        line = line.strip()
        if not line or any(kw in line.lower() for kw in _SKIP_WORDS):
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
    return ai_val if ai_val and ai_val != "N/A" else backup_val


# ========== MAIN PROCESSING ==========
def process_resume(resume_file, jd_text, jd_name, batch_ts):
    logger.info(f"Processing: {resume_file.name}")

    try:
        resume_text = extract_text(resume_file)
        if not resume_text or len(resume_text) < 50:
            logger.warning(
                f"{resume_file.name}: text extraction failed ({len(resume_text or '')} chars)"
            )
            return _error_record(
                resume_file.name, "Text extraction failed", jd_name, batch_ts
            )

        logger.info(f"{resume_file.name}: extracted {len(resume_text)} chars")

        ai = extract_all_fields_with_ai(resume_text, jd_text)
        b_name = extract_name_backup(resume_text)
        b_email, b_phone, b_linkedin = extract_contact_backup(resume_text)
        b_exp = extract_experience_backup(resume_text)

        job_history = ai.get("job_history", "N/A")
        total_jobs = ai.get("total_jobs", count_jobs_from_history(job_history))
        total_exp = _pick(ai.get("total_experience"), b_exp)

        result = {
            "Processed At": batch_ts,
            "Job Description": jd_name,
            "Name": _pick(ai.get("name"), b_name),
            "Email": _pick(ai.get("email"), b_email),
            "Phone": _pick(ai.get("phone"), b_phone),
            "LinkedIn": _pick(ai.get("linkedin"), b_linkedin),
            "Other Socials": ai.get("other_socials", "N/A"),
            "Location": ai.get("location", "N/A"),
            "Current Job Title": ai.get("current_job_title", "N/A"),
            "Current Organization": ai.get("current_organization", "N/A"),
            "Total Experience": total_exp,
            "Total Jobs": str(total_jobs)
            if str(total_jobs).isdigit()
            else str(count_jobs_from_history(job_history)),
            "Average Tenure": calculate_average_tenure(total_exp, total_jobs),
            "Role in JD": ai.get("role_in_jd", "N/A"),
            "Resume Match Score": ai.get("match_score", "N/A"),
            "Resume Summary": ai.get("summary", "N/A"),
            "Job History (org-title-jobDuration)": job_history,
        }

        logger.info(
            f"{resume_file.name}: done — Name={result['Name']}, Score={result['Resume Match Score']}"
        )
        return result

    except Exception as e:
        logger.error(f"{resume_file.name}: error — {e}")
        return _error_record(
            resume_file.name, f"Processing error: {e}", jd_name, batch_ts
        )


def _error_record(filename, error_msg, jd_name, batch_ts):
    record = {col: "N/A" for col in EXCEL_COLUMNS}
    record["Processed At"] = batch_ts
    record["Job Description"] = jd_name
    record["Name"] = filename
    record["Resume Match Score"] = "Error"
    record["Resume Summary"] = error_msg
    return record


# ========== EXCEL APPEND ==========
def append_results_to_excel(results: list):
    new_df = pd.DataFrame(results)
    # Enforce column order
    new_df = new_df.reindex(columns=EXCEL_COLUMNS)

    if RESULTS_EXCEL.exists():
        try:
            existing_df = pd.read_excel(RESULTS_EXCEL, engine="openpyxl")
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception as e:
            logger.error(f"Excel read error: {e} — overwriting")
            combined_df = new_df
    else:
        combined_df = new_df

    combined_df.to_excel(RESULTS_EXCEL, index=False, engine="openpyxl")
    logger.info(f"Excel updated: {len(combined_df)} total rows in {RESULTS_EXCEL.name}")
    return combined_df


# ========== RESULTS DISPLAY ==========
def display_results():
    results = st.session_state.results
    df = st.session_state.df

    st.success(
        f"✅ Successfully processed {len(results)} resumes! Results saved to Excel."
    )

    st.subheader("📋 Results Preview (First 5 rows)")
    st.dataframe(df.head(5), width="stretch")

    # Stats
    total = len(results)
    successful = sum(1 for r in results if r["Resume Match Score"] != "Error")
    names_ok = sum(1 for r in results if r["Name"] != "N/A")
    tenure_ok = sum(1 for r in results if r["Average Tenure"] != "N/A")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📊 Total", total)
    c2.metric("✅ Successful", successful)
    c3.metric("👤 Names", names_ok)
    c4.metric("📈 Tenure", tenure_ok)

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
        c1.metric("Overall Avg", f"{sum(tenure_values) / len(tenure_values):.1f} years")
        c2.metric("Highest", f"{max(tenure_values):.1f} years")
        c3.metric("Lowest", f"{min(tenure_values):.1f} years")

        st.subheader("📊 Tenure Distribution")
        for label, lo, hi in [
            ("0–2 yrs", 0, 2),
            ("2–5 yrs", 2, 5),
            ("5–10 yrs", 5, 10),
            ("10+ yrs", 10, 999),
        ]:
            count = sum(
                1 for t in tenure_values if (lo <= t <= hi if lo == 0 else lo < t <= hi)
            )
            st.write(
                f"• **{label}**: {count} candidates ({count / len(tenure_values) * 100:.1f}%)"
            )

    with st.expander("📈 View All Results"):
        st.dataframe(df, width="stretch")

    # Quality check
    st.subheader("🔍 Extraction Quality")
    for label, key in [
        ("Names", "Name"),
        ("Emails", "Email"),
        ("Phones", "Phone"),
        ("LinkedIn", "LinkedIn"),
        ("Tenure", "Average Tenure"),
        ("Job History", "Job History (org-title-jobDuration)"),
    ]:
        count = sum(1 for r in results if r[key] != "N/A")
        st.write(f"• **{label}**: {count}/{total} ({count / total * 100:.1f}%)")


# ========== STREAMLIT UI ==========
def main():
    st.set_page_config(page_title="🎯 Resume Screener", layout="wide")
    init_session_state()

    st.title("🎯 Resume Screener")
    st.markdown("**AI-powered resume parsing with automatic Excel logging**")

    # Sidebar
    with st.sidebar:
        st.header("📊 Session Dashboard")
        st.metric("🤖 LLM Calls This Session", st.session_state.llm_request_count)

        st.divider()
        st.subheader("📂 Files")
        st.code(str(LOG_DIR), language=None)
        st.caption("• `app_activity.log` — activity log")
        st.caption("• `llm_calls.jsonl` — LLM input/output")
        st.caption("• `resume_screening_results.xlsx` — all results")

        if RESULTS_EXCEL.exists():
            try:
                row_count = len(pd.read_excel(RESULTS_EXCEL, engine="openpyxl"))
                st.metric("📊 Total Rows in Excel", row_count)
            except Exception:
                pass

        st.divider()
        if st.button("🗑️ Clear Screen"):
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
            "📄 Upload Resumes", type=["pdf", "docx"], accept_multiple_files=True
        )

    # Processing
    if st.button("🚀 Start Processing", type="primary"):
        if not jd_file or not resume_files:
            st.error("⚠️ Please upload both a JD and at least one resume")
        else:
            jd_text = extract_text(jd_file)
            if not jd_text:
                st.error("❌ Failed to extract JD text")
            else:
                jd_name = jd_file.name
                batch_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"{'=' * 60}")
                logger.info(
                    f"NEW RUN — {len(resume_files)} resumes | JD: {jd_name} | {batch_ts}"
                )
                logger.info(f"{'=' * 60}")

                results = []

                with st.status("🚀 Processing resumes...", expanded=True) as status:
                    progress = st.progress(0)
                    file_display = st.empty()

                    for idx, rf in enumerate(resume_files, 1):
                        file_display.markdown(
                            f"**🔍 [{idx}/{len(resume_files)}]** `{rf.name}`"
                        )
                        results.append(process_resume(rf, jd_text, jd_name, batch_ts))
                        progress.progress(idx / len(resume_files))

                    file_display.empty()
                    status.update(
                        label="✅ Complete!", state="complete", expanded=False
                    )

                # Save
                st.session_state.results = results
                st.session_state.df = pd.DataFrame(results).reindex(
                    columns=EXCEL_COLUMNS
                )
                st.session_state.processing_done = True

                append_results_to_excel(results)

                successful = sum(
                    1 for r in results if r["Resume Match Score"] != "Error"
                )
                logger.info(
                    f"RUN COMPLETE — {successful}/{len(results)} successful | LLM calls: {st.session_state.llm_request_count}"
                )

    # Persistent display
    if st.session_state.processing_done and st.session_state.results:
        display_results()


if __name__ == "__main__":
    main()
