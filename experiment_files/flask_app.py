"""
flask_app.py — Unified Flask web application for the Resume Screener.

Consolidates logic from server2.py (FastAPI), alpha_app.py (Streamlit),
outreach_tab.py, and database.py into a single Flask server.
"""

import os
import time
import json
import io
import ssl
import smtplib
import threading
from datetime import datetime
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    jsonify,
    Response,
    session,
)
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    login_required,
    logout_user,
    current_user,
)
from flask_cors import CORS
from dotenv import load_dotenv
from pydantic import BaseModel, Field

import google.generativeai as genai


from database import (
    init_db,
    create_user,
    verify_user,
    get_user_by_username,
    delete_candidate,
    get_all_jobs,
    get_stats,
    get_all_candidates,
    mark_outreach_sent,
    update_candidate_selection_status,
    update_candidate_form_response,
    get_unsynced_candidates,
    save_candidate,
    get_candidate_by_id,
    get_jd_text,
    extract_job_code,
    bulk_update_candidate_status,
)
from sharepoint_helper import SharePointMatchScoreUpdater

# ── Load environment ──────────────────────────────────────────────────────────
load_dotenv()

# ── Initialize App ────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "super-secret-key-change-me")
CORS(app)

# ── Initialize DB ─────────────────────────────────────────────────────────────
init_db()

# ── Login Manager ─────────────────────────────────────────────────────────────
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


class User(UserMixin):
    def __init__(self, user_id, username, is_admin):
        self.id = user_id
        self.username = username
        self.is_admin = is_admin


@login_manager.user_loader
def load_user(user_id):
    from database import _cursor

    with _cursor() as cur:
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    if row:
        return User(row["id"], row["username"], row["is_admin"])
    return None


# ── AI Config ─────────────────────────────────────────────────────────────────
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
model = genai.GenerativeModel("gemini-2.5-flash")


# ═══════════════════════════════════════════════════════════════════════════════
# PYDANTIC MODELS  (ported verbatim from server2.py)
# ═══════════════════════════════════════════════════════════════════════════════


class ParameterMatch(BaseModel):
    status: str = Field(description="Match, Partial Match, or No Match")
    summary: str = Field(
        description="A 1-line summary indicating if and why it matches the JD"
    )


class ResumeJDMatch(BaseModel):
    overall_match_score: int = Field(
        description="Overall match score strictly on a scale of 0 to 100"
    )
    experience: ParameterMatch
    education: ParameterMatch
    location: ParameterMatch
    project_history_relevance: ParameterMatch
    tools_used: ParameterMatch
    certifications: ParameterMatch


class PersonalInfo(BaseModel):
    full_name: str
    location: str
    email: str
    phone: str


class Employment(BaseModel):
    current_job_title: str
    current_organization: str


class CareerMetrics(BaseModel):
    total_experience_in_years: float
    total_jobs: int


class Socials(BaseModel):
    linkedin: str
    github: str
    portfolio: str


class Education(BaseModel):
    degree: str
    institution: str
    graduation_year: str


class ResumeDataExtraction(BaseModel):
    personal_information: PersonalInfo
    professional_summary: str
    current_employment: Employment
    career_metrics: CareerMetrics
    social_profiles: Socials
    education_history: list[Education]


class ComprehensiveResumeAnalysis(BaseModel):
    function_1_resume_jd_matching: ResumeJDMatch
    function_2_resume_data_extraction: ResumeDataExtraction


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL HELPERS  (ported verbatim from server2.py)
# ═══════════════════════════════════════════════════════════════════════════════


def _build_email_html(
    candidate_name: str,
    jd_title: str,
    jd_text: str,
    form_link: str,
    custom_message: str,
) -> str:
    """Build a clean HTML email body."""
    form_block = ""
    if form_link:
        form_block = f"""
        <div style="margin:28px 0; text-align:center;">
            <a href="{form_link}"
               style="background:#6366f1;color:#fff;padding:14px 32px;
                      border-radius:8px;text-decoration:none;font-weight:600;
                      font-size:15px;display:inline-block;">
                &#128221; Submit Candidate Form
            </a>
        </div>
        """

    custom_block = ""
    if custom_message and custom_message.strip():
        custom_block = f"""
        <p style="color:#374151;margin-bottom:16px;">{custom_message}</p>
        """

    jd_html = jd_text.replace("\n", "<br>")

    return f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background:#f3f4f6;font-family:'Segoe UI',Arial,sans-serif;">
        <div style="max-width:620px;margin:40px auto;background:#fff;
                    border-radius:12px;overflow:hidden;
                    box-shadow:0 4px 24px rgba(0,0,0,0.08);">
            <div style="background:linear-gradient(135deg,#6366f1,#4f46e5);padding:32px 40px;">
                <div style="font-size:22px;font-weight:700;color:#fff;">
                    Exciting Opportunity For You 🎉
                </div>
                <div style="font-size:14px;color:rgba(255,255,255,0.8);margin-top:4px;">
                    {jd_title}
                </div>
            </div>
            <div style="padding:36px 40px;">
                <p style="color:#111827;font-size:16px;margin-bottom:16px;">
                    Dear <strong>{candidate_name}</strong>,
                </p>
                <p style="color:#374151;margin-bottom:16px;">
                    Thank you for your interest. After reviewing your profile, we're pleased to
                    share this opportunity and invite you to the next step in our hiring process.
                </p>
                {custom_block}
                <div style="background:#f9fafb;border-left:4px solid #6366f1;
                            border-radius:0 8px 8px 0;padding:20px 24px;margin:24px 0;">
                    <div style="font-size:12px;font-weight:700;color:#6366f1;
                                text-transform:uppercase;letter-spacing:0.08em;margin-bottom:12px;">
                        Job Description
                    </div>
                    <div style="font-size:14px;color:#374151;line-height:1.7;">
                        {jd_html}
                    </div>
                </div>
                {form_block}
                <p style="color:#6b7280;font-size:13px;margin-top:24px;">
                    If you have any questions, please reply to this email directly.
                </p>
                <p style="color:#374151;font-size:14px;margin-top:16px;">
                    Best regards,<br>
                    <strong>HR Team</strong>
                </p>
            </div>
            <div style="background:#f9fafb;padding:20px 40px;
                        border-top:1px solid #e5e7eb;
                        font-size:12px;color:#9ca3af;text-align:center;">
                This email was sent as part of our candidate outreach process.
            </div>
        </div>
    </body>
    </html>
    """


def _send_email(
    to_email: str,
    to_name: str,
    subject: str,
    html_body: str,
) -> tuple[bool, str]:
    """Send via SMTP.  Reads SMTP_* from env."""
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    from_name = os.getenv("SMTP_FROM_NAME", "HR Team")

    if not smtp_user or not smtp_pass:
        return False, "SMTP credentials not configured in .env."

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{smtp_user}>"
    msg["To"] = f"{to_name} <{to_email}>" if to_name else to_email
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, to_email, msg.as_string())
        return True, "Email sent successfully."
    except smtplib.SMTPAuthenticationError:
        return False, "SMTP authentication failed."
    except smtplib.SMTPRecipientsRefused:
        return False, f"Recipient refused: {to_email}"
    except Exception as e:
        return False, str(e)


# ═══════════════════════════════════════════════════════════════════════════════
# PROGRESS TRACKING  (SSE — simple global store)
# ═══════════════════════════════════════════════════════════════════════════════

progress_store = {"percent": 0, "message": "Waiting..."}


# ═══════════════════════════════════════════════════════════════════════════════
# SHAREPOINT PUSH HELPER
# ═══════════════════════════════════════════════════════════════════════════════


def _sp_config():
    """Build SharePoint config dict from environment."""
    return {
        "tenant_id": os.getenv("AZURE_TENANT_ID"),
        "client_id": os.getenv("AZURE_CLIENT_ID"),
        "client_secret": os.getenv("AZURE_CLIENT_SECRET"),
        "site_domain": os.getenv("SHAREPOINT_SITE_DOMAIN"),
        "site_path": os.getenv("SHAREPOINT_SITE_PATH"),
        "drive_name": os.getenv("SHAREPOINT_DRIVE_NAME"),
    }


def push_to_sharepoint(filename, metadata, role_hint=""):
    """Background task — push Metadata back to SharePoint."""
    try:
        cfg = _sp_config()
        if not all(cfg.values()):
            print("[SP ERROR] Missing SharePoint config in .env")
            return
        updater = SharePointMatchScoreUpdater(**cfg)
        status, msg, _ = updater.push_metadata(filename, metadata, role_hint=role_hint)
        print(f"[SP SYNC] {status}: {msg}")
    except Exception as e:
        print(f"[SP ERROR] Sync failed: {e}")


def sync_ms_form_responses():
    """Background function to fetch and sync MS Form Excel data."""
    try:
        print("[SYNC] Starting MS Form sync...")
        cfg = _sp_config()
        updater = SharePointMatchScoreUpdater(**cfg)

        # Exact file name provided by user
        excel_filename = "candidate information"
        rows = []
        try:
            # 1. Try SharePoint (Shared Site)
            print(f"[SYNC] Searching for '{excel_filename}' in SharePoint...")
            rows = updater.get_excel_rows(excel_filename)

            # 2. Try OneDrive (Personal) if SharePoint fails
            if not rows:
                user_email = os.getenv("MAILBOX_USER")
                # Prioritize deep.malusare because we confirmed the file is there
                possible_emails = ["deep.malusare@si2tech.com", user_email]
                for email in possible_emails:
                    if not email:
                        continue
                    print(
                        f"[SYNC] SharePoint failed or skipped .url. Trying OneDrive for {email}..."
                    )
                    rows = updater.get_onedrive_excel_rows(email, excel_filename)
                    if rows:
                        print(
                            f"[SYNC] Successfully found and read Excel from {email}'s OneDrive."
                        )
                        break

        except Exception as e:
            print(f"[SYNC] Error during file fetch: {e}")
            return 0

        if not rows:
            print("[SYNC] No rows found in MS Form Excel.")
            return 0

        # Optimization: Only match against candidates who don't have responses yet
        unsynced = get_unsynced_candidates()
        unsynced_emails = {c["email"].lower(): c["full_name"] for c in unsynced}

        if not unsynced_emails:
            print("[SYNC] All candidates already have form responses. Skipping.")
            return 0

        print(
            f"[SYNC] Found {len(rows)} rows to process. Target candidates: {len(unsynced_emails)}"
        )
        sync_count = 0
        for row in rows:
            try:
                # Match by "Email Address" as specified by user
                email = row.get("Email Address") or row.get("Email") or row.get("email")

                if email:
                    email_clean = str(email).strip().lower()
                    if email_clean in unsynced_emails:
                        print(
                            f"[SYNC] New response found for: {unsynced_emails[email_clean]} ({email_clean})"
                        )
                        updated = update_candidate_form_response(email_clean, row)
                        if updated:
                            sync_count += 1
                    # Note: We skip if already synced as per user request
                else:
                    print(f"[SYNC] Row skipped - no 'Email Address' column found.")
            except Exception as e:
                print(f"[SYNC ERROR] Error processing row: {e}")

        print(f"[SYNC] Finished. Updated {sync_count} candidates.")
        return sync_count
    except Exception as e:
        print(f"[SYNC ERROR] {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH ROUTES
# ═══════════════════════════════════════════════════════════════════════════════


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        user_data = verify_user(username, password)
        if user_data:
            user = User(user_data["id"], user_data["username"], user_data["is_admin"])
            login_user(user)
            return redirect(url_for("dashboard"))
        flash("Invalid username or password", "danger")
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        confirm = request.form.get("confirm_password")

        if password != confirm:
            flash("Passwords do not match", "danger")
            return redirect(url_for("register"))

        success, msg = create_user(username, password)
        if success:
            flash("Registration successful! Please login.", "success")
            return redirect(url_for("login"))
        flash(msg, "danger")
    return render_template("register.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE ROUTES
# ═══════════════════════════════════════════════════════════════════════════════


@app.route("/")
@login_required
def dashboard():
    stats = get_stats()
    recent = get_all_candidates()[:5]
    return render_template("dashboard.html", stats=stats, recent_candidates=recent)


@app.route("/screener")
@login_required
def screener():
    return render_template("screener.html")


@app.route("/outreach")
@login_required
def outreach():
    roles = get_all_jobs()
    return render_template("outreach.html", roles=roles)


@app.route("/responses")
@login_required
def responses():
    """Review Dashboard."""
    roles = get_all_jobs()
    return render_template("responses.html", roles=roles)


@app.route("/api/sync-responses", methods=["POST"])
@login_required
def api_sync_responses():
    """Manual trigger for MS Form sync."""
    count = sync_ms_form_responses()
    return jsonify({"success": True, "updated_count": count})


@app.route("/api/candidate/status", methods=["POST"])
@login_required
def api_update_status():
    """Update selection status and sync to SharePoint."""
    data = request.json
    cid = data.get("candidate_id")
    status = data.get("status")

    if not cid or not status:
        return jsonify({"error": "Missing id or status"}), 400

    try:
        updated = update_candidate_selection_status(cid, status)
        if not updated:
            return jsonify({"error": "Candidate not found"}), 404

        # Optional: Sync status back to SharePoint
        candidate = get_candidate_by_id(cid)
        if candidate and candidate.get("resume_filename"):
            metadata = {"SelectionStatus": status}
            threading.Thread(
                target=push_to_sharepoint,
                args=(candidate["resume_filename"], metadata, candidate["role_name"]),
            ).start()

        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/candidate/status/bulk", methods=["POST"])
@login_required
def api_bulk_update_status():
    """Update selection status for multiple candidates."""
    data = request.json
    cids = data.get("candidate_ids")
    status = data.get("status")

    if not cids or not status:
        return jsonify({"error": "Missing ids or status"}), 400

    try:
        count = bulk_update_candidate_status(cids, status)
        return jsonify({"success": True, "updated_count": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# SHAREPOINT API ROUTES
# ═══════════════════════════════════════════════════════════════════════════════


@app.route("/api/sp/files")
@login_required
def api_sp_files():
    """Return grouped resume folders + flat JD list from SharePoint."""
    try:
        cfg = _sp_config()
        updater = SharePointMatchScoreUpdater(**cfg)
        resumes = updater.list_resumes_grouped()
        jds = updater.list_jd_files()
        return jsonify({"resumes": resumes, "jds": jds, "connected": True})
    except Exception as e:
        return jsonify({"connected": False, "error": str(e)}), 500


@app.route("/api/sp/content")
@login_required
def api_sp_content():
    """Download the text content of a single SharePoint item by id."""
    item_id = request.args.get("item_id")
    if not item_id:
        return jsonify({"error": "No item_id provided"}), 400
    try:
        cfg = _sp_config()
        updater = SharePointMatchScoreUpdater(**cfg)
        content = updater.download_text_content(item_id)
        return jsonify({"content": content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# CANDIDATE / ANALYSIS API ROUTES
# ═══════════════════════════════════════════════════════════════════════════════


@app.route("/api/candidates")
@login_required
def api_list_candidates():
    min_score = request.args.get("min_score", 0, type=int)
    role = request.args.get("role", "")
    candidates = get_all_candidates(min_score=min_score)
    if role:
        candidates = [c for c in candidates if c["role_name"] == role]
    return jsonify(candidates)


@app.route("/api/progress")
@login_required
def api_progress():
    """SSE endpoint for real-time progress updates."""

    def generate():
        while True:
            yield f"data: {json.dumps(progress_store)}\n\n"
            if progress_store["percent"] >= 100:
                break
            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/analyze", methods=["POST"])
@login_required
def api_analyze():
    """Run AI analysis on resume + JD content (already fetched from SharePoint)."""
    global progress_store

    progress_store = {"percent": 10, "message": "Reading content..."}

    jd_title = request.form.get("jd_title")
    jd_text = request.form.get("jd_text")
    resume_text = request.form.get("resume_text")
    resume_filename = request.form.get("resume_filename")
    sync_sp = request.form.get("sync_sharepoint") == "on"

    if not all([jd_title, jd_text, resume_text, resume_filename]):
        return jsonify({"error": "Missing required fields (JD or Resume content)"}), 400

    try:
        progress_store = {"percent": 30, "message": "Applying AI logic..."}

        prompt = f"""
        You are an expert technical recruiter. Analyze the Resume and JD.
        Return strictly JSON with matching parameters and data extraction.

        Resume: {resume_text}
        JD: {jd_text}
        """

        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                response_schema=ComprehensiveResumeAnalysis,
                temperature=0.1,
            ),
        )

        progress_store = {"percent": 70, "message": "Saving to database..."}

        analysis_dict = json.loads(response.text)
        print(
            f"[ANALYZE] AI response parsed. Score: {analysis_dict.get('function_1_resume_jd_matching', {}).get('overall_match_score', '?')}"
        )

        cid = save_candidate(
            result=analysis_dict,
            role_name=jd_title,
            jd_filename=jd_title,
            jd_text=jd_text,
            resume_filename=resume_filename,
        )
        print(f"[ANALYZE] Candidate saved to PostgreSQL with id={cid}")

        if sync_sp:
            progress_store = {"percent": 90, "message": "Syncing to SharePoint..."}

            # Extract metadata for SharePoint using discovered internal names
            score = analysis_dict.get("function_1_resume_jd_matching", {}).get(
                "overall_match_score", 0
            )
            extraction = analysis_dict.get("function_2_resume_data_extraction", {})
            personal = extraction.get("personal_information", {})

            job_id_val = "Unknown"
            try:
                # Extracts 4 digits from role folder name
                job_id_val = str(extract_job_code(jd_title))
            except:
                pass

            metadata = {
                "MatchScore": score,
                "CandidateName": personal.get("full_name", "Unknown"),
                "CandidateEmail": personal.get("email", ""),
                "CandidatePhone": personal.get("phone", ""),
                "JobID": job_id_val,
                "JobRole": jd_title,
            }

            threading.Thread(
                target=push_to_sharepoint,
                args=(resume_filename, metadata, jd_title),
            ).start()

        progress_store = {"percent": 100, "message": "Analysis Complete!"}
        return response.text, 200, {"Content-Type": "application/json"}

    except Exception as e:
        import traceback

        traceback.print_exc()
        progress_store = {"percent": 100, "message": f"Error: {str(e)}"}
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# OUTREACH API ROUTES
# ═══════════════════════════════════════════════════════════════════════════════


@app.route("/api/outreach", methods=["POST"])
@login_required
def api_outreach():
    data = request.json
    ids = data.get("candidate_ids", [])
    form_link = data.get("form_link", "")
    custom_msg = data.get("custom_message", "")

    results = []
    for cid in ids:
        try:
            candidate = get_candidate_by_id(cid)
            if not candidate or not candidate.get("email"):
                continue

            jd_text_for_email = get_jd_text(candidate["job_id"])
            html_body = _build_email_html(
                candidate_name=candidate["full_name"],
                jd_title=candidate["role_name"],
                jd_text=jd_text_for_email,
                form_link=form_link,
                custom_message=custom_msg,
            )

            success, msg = _send_email(
                to_email=candidate["email"],
                to_name=candidate["full_name"],
                subject=f"Invitation: {candidate['role_name']}",
                html_body=html_body,
            )

            if success:
                mark_outreach_sent(cid, form_link)
                results.append({"id": cid, "status": "sent"})
            else:
                results.append({"id": cid, "status": "failed", "error": msg})

        except Exception as e:
            results.append({"id": cid, "status": "error", "error": str(e)})

    sent = sum(1 for r in results if r["status"] == "sent")
    return jsonify({"sent": sent, "failed": len(results) - sent, "details": results})


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Ensure at least one admin user exists on first run
    with app.app_context():
        user = get_user_by_username("admin")
        if not user:
            print("Creating default admin user...")
            create_user("admin", "admin123", is_admin=1)

    app.run(debug=True, port=5001)
