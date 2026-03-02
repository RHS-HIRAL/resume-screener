"""
database.py — PostgreSQL persistence layer for the Resume Screener.

Tables
------
users       : Application users with hashed passwords
jobs        : One row per JD screened against (id = first 4 digits of subfolder)
candidates  : One row per resume analysed (candidate_id = job_code + 2-digit seq)

Usage
-----
    import database
    database.init_db()
    job_id = database.upsert_job(3237, 'jd.txt', 'SAP_SD_Consultant', jd_text)
    cid    = database.save_candidate(result, role, jd_filename, jd_text, resume_filename)
    rows   = database.get_candidates_for_role(role)
"""

import os
import re
import json
from typing import Optional
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv

load_dotenv()


# ═══════════════════════════════════════════════════════════════════════════
# CONNECTION
# ═══════════════════════════════════════════════════════════════════════════


def _conn():
    """Return a new PostgreSQL connection with RealDictCursor."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5433")),
        dbname=os.getenv("PG_DATABASE", "resume_screener"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
    )


@contextmanager
def _cursor(commit=False):
    """Context manager yielding a RealDictCursor. Auto-closes connection."""
    con = _conn()
    try:
        cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cur
        if commit:
            con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        cur.close()
        con.close()


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA INIT
# ═══════════════════════════════════════════════════════════════════════════


def init_db() -> None:
    """Create tables if they don't exist. Safe to call on every startup."""
    with _cursor(commit=True) as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            SERIAL PRIMARY KEY,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin      INTEGER DEFAULT 0,
            created_at    TIMESTAMP DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id           INTEGER PRIMARY KEY,
            jd_filename  TEXT NOT NULL,
            role_name    TEXT NOT NULL,
            jd_text      TEXT,
            created_at   TIMESTAMP DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            id                  SERIAL PRIMARY KEY,
            candidate_id        TEXT UNIQUE,
            job_id              INTEGER REFERENCES jobs(id),
            role_name           TEXT,
            full_name           TEXT,
            email               TEXT,
            phone               TEXT,
            location            TEXT,
            current_title       TEXT,
            current_company     TEXT,
            total_experience    REAL,
            match_score         INTEGER,
            exp_status          TEXT,
            edu_status          TEXT,
            loc_status          TEXT,
            proj_status         TEXT,
            tools_status        TEXT,
            certs_status        TEXT,
            resume_filename     TEXT,
            sharepoint_link     TEXT,
            outreach_sent       INTEGER DEFAULT 0,
            outreach_sent_at    TIMESTAMP,
            meeting_link        TEXT,
            screened_at         TIMESTAMP DEFAULT NOW(),
            raw_json            TEXT
        );
        """)


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════


def extract_job_code(folder_or_role_name: str) -> int:
    """Extract the first 4 digits from a subfolder name like '3237_SAP_SD_Consultant'."""
    m = re.match(r"(\d{4})", folder_or_role_name)
    if m:
        return int(m.group(1))
    raise ValueError(f"Cannot extract 4-digit job code from '{folder_or_role_name}'")


# ═══════════════════════════════════════════════════════════════════════════
# JOBS
# ═══════════════════════════════════════════════════════════════════════════


def upsert_job(job_id: int, jd_filename: str, role_name: str, jd_text: str) -> int:
    """Insert a job with a specific ID or return it if it already exists."""
    with _cursor(commit=True) as cur:
        cur.execute("SELECT id FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute(
            "INSERT INTO jobs (id, jd_filename, role_name, jd_text) VALUES (%s, %s, %s, %s) RETURNING id",
            (job_id, jd_filename, role_name, jd_text),
        )
        return cur.fetchone()["id"]


def get_all_jobs() -> list:
    with _cursor() as cur:
        cur.execute(
            "SELECT id, jd_filename, role_name, created_at FROM jobs ORDER BY id DESC"
        )
        return [dict(r) for r in cur.fetchall()]


# ═══════════════════════════════════════════════════════════════════════════
# CANDIDATES
# ═══════════════════════════════════════════════════════════════════════════


def _next_candidate_id(cur, job_id: int) -> str:
    """Generate the next candidate_id for a given job_id.
    E.g., job_id=3237 → first candidate is '323701', second is '323702', etc.
    """
    prefix = str(job_id)
    cur.execute(
        "SELECT candidate_id FROM candidates WHERE job_id = %s ORDER BY candidate_id DESC LIMIT 1",
        (job_id,),
    )
    row = cur.fetchone()
    if row and row["candidate_id"]:
        # Extract the last 2 digits and increment
        last_seq = int(row["candidate_id"][-2:])
        new_seq = last_seq + 1
    else:
        new_seq = 1
    return f"{prefix}{new_seq:02d}"


def save_candidate(
    result: dict,
    role_name: str,
    jd_filename: str,
    jd_text: str,
    resume_filename: str = "",
    sharepoint_link: str = "",
) -> int:
    """Parse the API result dict and persist a candidate row. Returns the new row id."""
    match = result.get("function_1_resume_jd_matching", {})
    extract = result.get("function_2_resume_data_extraction", {})
    personal = extract.get("personal_information", {})
    employment = extract.get("current_employment", {})
    career = extract.get("career_metrics", {})

    job_code = extract_job_code(role_name)
    job_id = upsert_job(job_code, jd_filename or "unknown_jd", role_name, jd_text)

    with _cursor(commit=True) as cur:
        candidate_id = _next_candidate_id(cur, job_id)
        cur.execute(
            """
            INSERT INTO candidates (
                candidate_id, job_id, role_name,
                full_name, email, phone, location,
                current_title, current_company, total_experience,
                match_score,
                exp_status, edu_status, loc_status,
                proj_status, tools_status, certs_status,
                resume_filename, sharepoint_link, raw_json
            ) VALUES (%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s, %s,%s,%s, %s,%s,%s, %s,%s,%s)
            RETURNING id
        """,
            (
                candidate_id,
                job_id,
                role_name,
                personal.get("full_name", "Unknown"),
                personal.get("email", ""),
                personal.get("phone", ""),
                personal.get("location", ""),
                employment.get("current_job_title", ""),
                employment.get("current_organization", ""),
                career.get("total_experience_in_years", 0.0),
                match.get("overall_match_score", 0),
                match.get("experience", {}).get("status", ""),
                match.get("education", {}).get("status", ""),
                match.get("location", {}).get("status", ""),
                match.get("project_history_relevance", {}).get("status", ""),
                match.get("tools_used", {}).get("status", ""),
                match.get("certifications", {}).get("status", ""),
                resume_filename,
                sharepoint_link,
                json.dumps(result, ensure_ascii=False),
            ),
        )
        print(f"[DB] Saved candidate_id={candidate_id} for job_id={job_id}")
        return cur.fetchone()["id"]


def get_candidates_for_role(role_name: str) -> list:
    with _cursor() as cur:
        cur.execute(
            """SELECT * FROM candidates
               WHERE role_name = %s
               ORDER BY match_score DESC, screened_at DESC""",
            (role_name,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_all_candidates(min_score: int = 0) -> list:
    with _cursor() as cur:
        cur.execute(
            """SELECT * FROM candidates
               WHERE match_score >= %s
               ORDER BY match_score DESC, screened_at DESC""",
            (min_score,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_candidate_by_id(cid: int) -> Optional[dict]:
    with _cursor() as cur:
        cur.execute("SELECT * FROM candidates WHERE id = %s", (cid,))
        row = cur.fetchone()
        return dict(row) if row else None


def mark_outreach_sent(candidate_id: int, meeting_link: str = "") -> None:
    with _cursor(commit=True) as cur:
        cur.execute(
            """UPDATE candidates
               SET outreach_sent = 1,
                   outreach_sent_at = NOW(),
                   meeting_link = %s
               WHERE id = %s""",
            (meeting_link, candidate_id),
        )


def delete_candidate(candidate_id: int) -> None:
    with _cursor(commit=True) as cur:
        cur.execute("DELETE FROM candidates WHERE id = %s", (candidate_id,))


def get_stats() -> dict:
    with _cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM candidates")
        total = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) AS cnt FROM candidates WHERE outreach_sent = 1")
        sent = cur.fetchone()["cnt"]
        cur.execute("SELECT AVG(match_score) AS avg FROM candidates")
        avg_sc = cur.fetchone()["avg"] or 0
        return {
            "total_screened": total,
            "outreach_sent": sent,
            "pending_outreach": total - sent,
            "avg_score": round(float(avg_sc), 1),
        }


# ═══════════════════════════════════════════════════════════════════════════
# USERS & AUTH
# ═══════════════════════════════════════════════════════════════════════════


def create_user(username, password, is_admin=0):
    """Create a new user with a hashed password."""
    hash_pw = generate_password_hash(password)
    try:
        with _cursor(commit=True) as cur:
            cur.execute(
                "INSERT INTO users (username, password_hash, is_admin) VALUES (%s, %s, %s)",
                (username, hash_pw, is_admin),
            )
        return True, "User created successfully."
    except psycopg2.errors.UniqueViolation:
        return False, "Username already exists."
    except Exception as e:
        return False, str(e)


def get_user_by_username(username):
    """Return a user dict or None."""
    with _cursor() as cur:
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        return dict(row) if row else None


def verify_user(username, password):
    """Verify credentials and return user dict if successful."""
    user = get_user_by_username(username)
    if user and check_password_hash(user["password_hash"], password):
        return user
    return None


def get_jd_text(job_id: int) -> str:
    with _cursor() as cur:
        cur.execute("SELECT jd_text FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    return row["jd_text"] if row else ""
