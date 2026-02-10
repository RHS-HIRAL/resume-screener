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

# ========== ENVIRONMENT & API SETUP ==========
load_dotenv()

# Gemini
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Groq — primary and alternate keys
GROQ_KEY_PRIMARY = os.getenv("GROQ_API_KEY")
GROQ_KEY_ALT = os.getenv("GROQ_API_KEY_ALT")


# ========= LOG DIRECTORY SETUP =========
APP_DIR = Path(__file__).parent
LOG_DIR = APP_DIR / "resume_screener_logs"
LOG_DIR.mkdir(exist_ok=True)

APP_LOG_FILE = LOG_DIR / "app_activity.log"
LLM_LOG_FILE = LOG_DIR / "llm_calls.jsonl"
RESULTS_EXCEL = LOG_DIR / "resume_screening_results.xlsx"

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


# ========== LOGGING ==========
def setup_logger():
    lgr = logging.getLogger("ResumeScreener")
    lgr.setLevel(logging.DEBUG)
    lgr.propagate = False
    if not lgr.handlers:
        handler = RotatingFileHandler(
            APP_LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
        handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))
        lgr.addHandler(handler)
    return lgr


logger = setup_logger()


def log_batch_separator(batch_ts, jd_name, resume_count):
    """Write a single batch header to both log files"""
    header = f"BATCH {batch_ts} | JD: {jd_name} | Resumes: {resume_count}"
    divider = "-" * len(header)

    logger.info(divider)
    logger.info(header)
    logger.info(divider)

    with open(LLM_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n--- {header} ---\n")


def log_llm_call(record: dict):
    """Append one compact JSON line per LLM call — no per-call separators"""
    with open(LLM_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ========== SESSION STATE ==========
def init_session_state():
    for key, default in {
        "results": None,
        "df": None,
        "processing_done": False,
        "llm_request_count": 0,
    }.items():
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


# ========== LLM PROVIDERS ==========
def _call_gemini(prompt):
    """Call Google Gemini API"""
    model = genai.GenerativeModel("gemini-2.5-flash")
    response = model.generate_content(prompt)
    return response.text


def _call_groq(prompt, api_key):
    """Call Groq API with the given key"""
    client = Groq(api_key=api_key)
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )
    return response.choices[0].message.content


# Fallback chain: (provider_name, callable)
def _build_fallback_chain():
    chain = [("Gemini", lambda p: _call_gemini(p))]
    if GROQ_KEY_PRIMARY:
        chain.append(("Groq-Primary", lambda p: _call_groq(p, GROQ_KEY_PRIMARY)))
    if GROQ_KEY_ALT:
        chain.append(("Groq-Alt", lambda p: _call_groq(p, GROQ_KEY_ALT)))
    return chain


FALLBACK_CHAIN = _build_fallback_chain()


def get_llm_response(prompt):
    """Try each provider in order until one succeeds. Logs every attempt."""
    st.session_state.llm_request_count += 1
    req_num = st.session_state.llm_request_count
    ts = datetime.now().isoformat()

    errors = []

    for provider_name, call_fn in FALLBACK_CHAIN:
        logger.info(f"LLM #{req_num} — trying {provider_name} ({len(prompt)} chars)")

        record = {
            "request_num": req_num,
            "timestamp": ts,
            "provider": provider_name,
            "prompt_length": len(prompt),
            "prompt": prompt,
            "status": "pending",
            "response_length": 0,
            "response": "",
            "error": None,
        }

        try:
            text = call_fn(prompt)

            record.update(status="success", response_length=len(text), response=text)
            logger.info(f"LLM #{req_num} — {provider_name} success ({len(text)} chars)")
            log_llm_call(record)
            return text

        except Exception as e:
            err_msg = f"{provider_name}: {e}"
            errors.append(err_msg)
            record.update(status="error", error=str(e))
            logger.warning(f"LLM #{req_num} — {provider_name} failed: {e}")
            log_llm_call(record)
            continue

    # All providers failed
    all_errors = " | ".join(errors)
    logger.error(f"LLM #{req_num} — ALL PROVIDERS FAILED: {all_errors}")
    return f"ERROR: All providers failed — {all_errors}"


# ========== AI FIELD EXTRACTION ==========
def extract_all_fields_with_ai(resume_text, jd_text):
    logger.info(
        f"AI extraction — resume: {len(resume_text)} chars, JD: {len(jd_text)} chars"
    )

    prompt = f"""You are an expert resume parser. Extract the following information from this resume with 100% accuracy.

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
    "match_score": "Percentage of resume candidate match with JD (0-100%)",
    "summary": "Brief professional summary in 2-3 sentences"
}}"""

    response = get_llm_response(prompt)

    if response.startswith("ERROR:"):
        logger.error(f"AI extraction failed: {response}")
        return {}

    try:
        json_match = re.search(r"\{.*\}", response, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group(0))
            logger.info(f"AI extraction success — {len(data)} fields parsed")
            return data
    except (json.JSONDecodeError, ValueError) as e:
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
        if (
            2 <= len(line.split()) <= 4
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
        if m and 0 <= float(m.group(1)) <= 50:
            return f"{float(m.group(1))} years"
    return "N/A"


# ========== HELPERS ==========
def _count_jobs(job_history):
    if not job_history or job_history == "N/A":
        return 0
    return len([e for e in job_history.split(" | ") if e.strip()])


def _extract_years(exp_str):
    if not exp_str or exp_str == "N/A":
        return 0.0
    m = re.search(r"(\d+(?:\.\d+)?)", exp_str)
    return float(m.group(1)) if m else 0.0


def _avg_tenure(total_exp, total_jobs):
    try:
        years = _extract_years(total_exp)
        jobs = (
            int(total_jobs)
            if str(total_jobs).isdigit()
            else _count_jobs(str(total_jobs))
        )
        return f"{years / jobs:.1f} years" if years > 0 and jobs > 0 else "N/A"
    except Exception:
        return "N/A"


def _pick(ai_val, backup_val):
    return ai_val if ai_val and ai_val != "N/A" else backup_val


# ========== RESUME PROCESSING ==========
def process_resume(resume_file, jd_text, jd_name, batch_ts):
    logger.info(f"Processing: {resume_file.name}")

    try:
        resume_text = extract_text(resume_file)
        if not resume_text or len(resume_text) < 50:
            logger.warning(
                f"{resume_file.name}: extraction failed ({len(resume_text or '')} chars)"
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
        total_jobs = ai.get("total_jobs", _count_jobs(job_history))
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
            else str(_count_jobs(job_history)),
            "Average Tenure": _avg_tenure(total_exp, total_jobs),
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
    record.update(
        {
            "Processed At": batch_ts,
            "Job Description": jd_name,
            "Name": filename,
            "Resume Match Score": "Error",
            "Resume Summary": error_msg,
        }
    )
    return record


# ========== EXCEL APPEND ==========
def append_results_to_excel(results: list):
    new_df = pd.DataFrame(results).reindex(columns=EXCEL_COLUMNS)

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
    logger.info(f"Excel updated: {len(combined_df)} total rows")
    return combined_df


# ========== DISPLAY ==========
def display_results():
    results = st.session_state.results
    df = st.session_state.df
    total = len(results)

    st.success(f"✅ Processed {total} resumes — results saved to Excel.")

    st.subheader("📋 Results Preview")
    st.dataframe(df.head(5), width="stretch")

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
        st.subheader("📊 Tenure Insights")
        c1, c2, c3 = st.columns(3)
        c1.metric("Average", f"{sum(tenure_values) / len(tenure_values):.1f} yrs")
        c2.metric("Highest", f"{max(tenure_values):.1f} yrs")
        c3.metric("Lowest", f"{min(tenure_values):.1f} yrs")

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
                f"• **{label}**: {count} ({count / len(tenure_values) * 100:.1f}%)"
            )

    with st.expander("📈 View All Results"):
        st.dataframe(df, width="stretch")

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


# ========== MAIN UI ==========
def main():
    st.set_page_config(page_title="🎯 Resume Screener", layout="wide")
    init_session_state()

    st.title("🎯 Resume Screener")
    st.markdown("**AI-powered resume parsing with automatic Excel logging**")

    # Sidebar
    with st.sidebar:
        st.header("📊 Dashboard")
        st.metric("🤖 LLM Calls", st.session_state.llm_request_count)

        # Show which providers are configured
        providers = ["Gemini"]
        if GROQ_KEY_PRIMARY:
            providers.append("Groq")
        if GROQ_KEY_ALT:
            providers.append("Groq-Alt")
        st.caption(f"**Providers:** {' → '.join(providers)}")

        st.divider()
        st.subheader("📂 Files")
        st.code(str(LOG_DIR), language=None)
        st.caption("• `app_activity.log`")
        st.caption("• `llm_calls.jsonl`")
        st.caption("• `resume_screening_results.xlsx`")

        if RESULTS_EXCEL.exists():
            try:
                row_count = len(pd.read_excel(RESULTS_EXCEL, engine="openpyxl"))
                st.metric("📊 Total Excel Rows", row_count)
            except Exception:
                pass

        st.divider()
        if st.button("🗑️ Clear Screen"):
            st.session_state.results = None
            st.session_state.df = None
            st.session_state.processing_done = False
            st.rerun()

    # Upload
    col1, col2 = st.columns(2)
    with col1:
        jd_file = st.file_uploader("📋 Job Description", type=["pdf", "docx"])
    with col2:
        resume_files = st.file_uploader(
            "📄 Resumes", type=["pdf", "docx"], accept_multiple_files=True
        )

    # Process
    if st.button("🚀 Start Processing", type="primary"):
        if not jd_file or not resume_files:
            st.error("⚠️ Upload both a JD and at least one resume")
        else:
            jd_text = extract_text(jd_file)
            if not jd_text:
                st.error("❌ Failed to extract JD text")
            else:
                jd_name = jd_file.name
                batch_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_batch_separator(batch_ts, jd_name, len(resume_files))

                results = []
                with st.status("🚀 Processing...", expanded=True) as status:
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

                st.session_state.results = results
                st.session_state.df = pd.DataFrame(results).reindex(
                    columns=EXCEL_COLUMNS
                )
                st.session_state.processing_done = True

                append_results_to_excel(results)

                ok = sum(1 for r in results if r["Resume Match Score"] != "Error")
                logger.info(
                    f"RUN COMPLETE — {ok}/{len(results)} successful | LLM calls: {st.session_state.llm_request_count}"
                )

    if st.session_state.processing_done and st.session_state.results:
        display_results()


if __name__ == "__main__":
    main()
