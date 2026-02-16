"""
Email Fetcher — reads application notification emails from the mailbox via
Microsoft Graph API and extracts structured candidate data.

Expected email format (from your ATS / career portal):
  Subject: New application received for the position: {Role} [{JobID}]
  Body:
    Job Opening: {Role} [{JobID}]
    Name: {Full Name}
    Email: {email}
    Phone: {phone}
    Resume: {URL to PDF}
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from bs4 import BeautifulSoup
import requests

import config

logger = logging.getLogger(__name__)

# ─── Regex tuned to the exact ATS email format ───────────────────────────────
# Subject pattern:  "...for the position: Some Role Name [1234]"
SUBJECT_PATTERN = re.compile(
    r"for the position:\s*(.+?)\s*\[(\w+)\]\s*$", re.IGNORECASE
)
# Body field patterns (key: value on its own line)
FIELD_PATTERNS = {
    "job_opening": re.compile(r"Job\s*Opening\s*:\s*(.+)", re.IGNORECASE),
    "name": re.compile(r"Name\s*:\s*(.+)", re.IGNORECASE),
    "email": re.compile(r"Email\s*:\s*(\S+@\S+\.\S+)", re.IGNORECASE),
    "phone": re.compile(r"Phone\s*:\s*([\d\s\+\-().]+)", re.IGNORECASE),
    "resume_url": re.compile(r"Resume\s*:\s*(https?://\S+)", re.IGNORECASE),
}
# Extract Job ID from "Job Opening: Role Name [5101]"
JOB_ID_BRACKET = re.compile(r"\[(\w+)\]\s*$")


@dataclass
class CandidateInfo:
    """Structured candidate data extracted from one application email."""

    name: str = ""
    email: str = ""
    phone: str = ""
    job_role: str = ""
    job_id: str = ""
    resume_url: str = ""
    attachments: list[dict] = field(default_factory=list)  # [{id, name, content_type}]
    source_email_id: str = ""
    source_subject: str = ""
    received_date: str = ""

    @property
    def safe_name(self) -> str:
        """Filename-safe version of the candidate name (title-cased)."""
        # Convert "MITESHKUMAR BHAILALBHAI ROHIT" → "Miteshkumar_Bhailalbhai_Rohit"
        cleaned = re.sub(r"[^\w\s\-]", "", self.name).strip()
        return "_".join(w.capitalize() for w in cleaned.split()) or "Unknown"

    @property
    def safe_job_id(self) -> str:
        return re.sub(r"[^\w\-]", "", self.job_id).strip() or "NO-ID"

    @property
    def safe_job_role(self) -> str:
        """Filesystem-safe shortened job role for subfolder names."""
        cleaned = re.sub(r"[^\w\s\-]", "", self.job_role).strip()
        # Limit length and replace spaces with underscores
        return "_".join(cleaned.split())[:80] or "General"


class EmailFetcher:
    """Fetches and parses recruitment emails from the past N hours."""

    def __init__(self, auth_headers: dict):
        self.headers = auth_headers
        self.base = config.GRAPH_BASE_URL

    # ── Public ────────────────────────────────────────────────────────────────

    def fetch_recent_emails(self) -> list[CandidateInfo]:
        """Main entry: returns structured CandidateInfo for each relevant email."""
        raw_emails = self._get_emails_since(hours=config.LOOKBACK_HOURS)
        logger.info(
            "Fetched %d emails from the last %d hours.",
            len(raw_emails),
            config.LOOKBACK_HOURS,
        )

        candidates: list[CandidateInfo] = []
        for msg in raw_emails:
            if not self._is_relevant(msg):
                logger.debug("Skipping irrelevant email: %s", msg.get("subject", ""))
                continue

            candidate = self._parse_email(msg)
            candidates.append(candidate)
            logger.info(
                "Parsed → name=%s | email=%s | phone=%s | job_id=%s | role=%s | resume=%s",
                candidate.name,
                candidate.email,
                candidate.phone,
                candidate.job_id,
                candidate.job_role,
                "URL"
                if candidate.resume_url
                else f"{len(candidate.attachments)} attachments",
            )

        return candidates

    # ── Graph API Calls ───────────────────────────────────────────────────────

    def _get_emails_since(self, hours: int) -> list[dict]:
        """Fetch emails received in the last `hours` hours (handles pagination)."""
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        url = (
            f"{self.base}/users/{config.MAILBOX_USER}/messages"
            f"?$filter=receivedDateTime ge {since}"
            f"&$orderby=receivedDateTime desc"
            f"&$top=200"
            f"&$select=id,subject,from,receivedDateTime,body,hasAttachments"
        )
        all_messages: list[dict] = []
        while url:
            resp = requests.get(url, headers=self.headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            all_messages.extend(data.get("value", []))
            url = data.get("@odata.nextLink")

        return all_messages

    def get_attachment_content(self, email_id: str, attachment_id: str) -> bytes:
        """Download a specific attachment's raw bytes."""
        url = (
            f"{self.base}/users/{config.MAILBOX_USER}/messages/{email_id}"
            f"/attachments/{attachment_id}/$value"
        )
        resp = requests.get(url, headers=self.headers, timeout=60)
        resp.raise_for_status()
        return resp.content

    def get_attachments_metadata(self, email_id: str) -> list[dict]:
        """List attachments on an email."""
        url = (
            f"{self.base}/users/{config.MAILBOX_USER}/messages/{email_id}/attachments"
            f"?$select=id,name,contentType,size"
        )
        resp = requests.get(url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        return resp.json().get("value", [])

    # ── Parsing ───────────────────────────────────────────────────────────────

    def _is_relevant(self, msg: dict) -> bool:
        """Quick check: does the subject match our ATS notification pattern?"""
        if not config.SUBJECT_KEYWORDS:
            return True
        subject = (msg.get("subject") or "").lower()
        return any(kw in subject for kw in config.SUBJECT_KEYWORDS)

    def _parse_email(self, msg: dict) -> CandidateInfo:
        """
        Parse one ATS notification email into structured CandidateInfo.

        Strategy:
          1. Try to extract Job Role + Job ID from the Subject line first.
          2. Parse the body line-by-line for Name, Email, Phone, Resume URL.
          3. If body also has Job Opening line, use it as fallback / confirmation.
          4. If no resume URL, check for PDF attachments.
        """
        subject = msg.get("subject", "")
        body_html = msg.get("body", {}).get("content", "")
        body_text = BeautifulSoup(body_html, "html.parser").get_text(separator="\n")

        candidate = CandidateInfo(
            source_email_id=msg["id"],
            source_subject=subject,
            received_date=msg.get("receivedDateTime", "")[:10],
        )

        # ── Subject parsing ──
        subj_match = SUBJECT_PATTERN.search(subject)
        if subj_match:
            candidate.job_role = subj_match.group(1).strip()
            candidate.job_id = subj_match.group(2).strip()

        # ── Body parsing (line-by-line key:value) ──
        for line in body_text.splitlines():
            line = line.strip()
            if not line:
                continue

            for field_key, pattern in FIELD_PATTERNS.items():
                m = pattern.search(line)
                if not m:
                    continue

                value = m.group(1).strip()

                if field_key == "name":
                    candidate.name = value.title()  # "MITESHKUMAR" → "Miteshkumar"

                elif field_key == "email":
                    candidate.email = value.lower()

                elif field_key == "phone":
                    candidate.phone = re.sub(r"[^\d+\-() ]", "", value).strip()

                elif field_key == "resume_url":
                    candidate.resume_url = value

                elif field_key == "job_opening":
                    # Fallback: extract job_id and role from body if subject didn't work
                    if not candidate.job_id:
                        bracket = JOB_ID_BRACKET.search(value)
                        if bracket:
                            candidate.job_id = bracket.group(1)
                            candidate.job_role = value[: bracket.start()].strip()
                        else:
                            candidate.job_role = candidate.job_role or value

        # ── Fallback: PDF attachments if no resume URL ──
        if not candidate.resume_url and msg.get("hasAttachments"):
            attachments = self.get_attachments_metadata(msg["id"])
            candidate.attachments = [
                {
                    "id": a["id"],
                    "name": a["name"],
                    "content_type": a.get("contentType", ""),
                }
                for a in attachments
                if a.get("name", "").lower().endswith(".pdf")
                or "pdf" in a.get("contentType", "").lower()
            ]

        # ── Fallback name from sender if still empty ──
        if not candidate.name:
            sender_name = msg.get("from", {}).get("emailAddress", {}).get("name", "")
            candidate.name = sender_name.title() if sender_name else "Unknown"

        # ── Fallback email from sender ──
        if not candidate.email:
            candidate.email = (
                msg.get("from", {}).get("emailAddress", {}).get("address", "")
            )

        return candidate
