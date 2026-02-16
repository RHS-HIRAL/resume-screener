import os
import sys
import json
import re
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from urllib.parse import quote

import msal
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()


class config:
    """
    Configuration for the Outlook Resume Pipeline.
    All values are loaded from environment variables or a .env file.
    """

    # ─── Entra ID (Azure AD) Credentials ────────────────────────────────────
    TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
    CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
    CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "")

    # ─── Microsoft Graph API ─────────────────────────────────────────────────
    GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
    SCOPES = ["https://graph.microsoft.com/.default"]

    # ─── Mailbox to Monitor ──────────────────────────────────────────────────
    MAILBOX_USER = os.getenv("MAILBOX_USER", "recruitment@yourcompany.com")

    # ─── Email Filtering ─────────────────────────────────────────────────────
    SUBJECT_KEYWORDS = ["new application received"]
    LOOKBACK_HOURS = 24

    # ─── SharePoint Target ───────────────────────────────────────────────────
    SHAREPOINT_SITE_DOMAIN = os.getenv(
        "SHAREPOINT_SITE_DOMAIN", "yourcompany.sharepoint.com"
    )
    SHAREPOINT_SITE_PATH = os.getenv("SHAREPOINT_SITE_PATH", "/sites/Recruitment")
    SHAREPOINT_DRIVE_NAME = os.getenv("SHAREPOINT_DRIVE_NAME", "Documents")
    SHAREPOINT_BASE_FOLDER = os.getenv("SHAREPOINT_BASE_FOLDER", "Resumes")

    # ─── File Naming ─────────────────────────────────────────────────────────
    # Final path: Resumes/{JobID}_{JobRole}/{Name}_{JobID}_{Date}.pdf
    FILE_NAME_TEMPLATE = "{name}_{job_id}_{date}.pdf"
    SUBFOLDER_TEMPLATE = "{job_id}_{job_role}"

    # ─── Notifications ───────────────────────────────────────────────────────
    TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL", "")

    # ─── Logging ─────────────────────────────────────────────────────────────
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "logs/pipeline.log")

    # ─── Local Temp Directory ────────────────────────────────────────────────
    TEMP_DIR = os.getenv("TEMP_DIR", "./tmp_resumes")


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTH
# ═══════════════════════════════════════════════════════════════════════════════

logger_auth = logging.getLogger("auth")


class GraphAuthProvider:
    """Handles OAuth2 client-credentials authentication against Microsoft Entra ID."""

    def __init__(self):
        self._app = msal.ConfidentialClientApplication(
            client_id=config.CLIENT_ID,
            client_credential=config.CLIENT_SECRET,
            authority=config.AUTHORITY,
        )
        self._token_cache: dict | None = None

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing silently if possible."""
        result = self._app.acquire_token_silent(config.SCOPES, account=None)
        if not result:
            logger_auth.info(
                "No cached token — acquiring new token via client credentials."
            )
            result = self._app.acquire_token_for_client(scopes=config.SCOPES)

        if "access_token" in result:
            return result["access_token"]

        error = result.get("error_description", result.get("error", "Unknown error"))
        logger_auth.error("Token acquisition failed: %s", error)
        raise RuntimeError(f"Could not acquire token: {error}")

    def get_headers(self) -> dict:
        """Return standard headers for Graph API calls."""
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json",
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  EMAIL FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

logger_fetcher = logging.getLogger("email_fetcher")

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
        logger_fetcher.info(
            "Fetched %d emails from the last %d hours.",
            len(raw_emails),
            config.LOOKBACK_HOURS,
        )

        candidates: list[CandidateInfo] = []
        for msg in raw_emails:
            if not self._is_relevant(msg):
                logger_fetcher.debug(
                    "Skipping irrelevant email: %s", msg.get("subject", "")
                )
                continue

            candidate = self._parse_email(msg)
            candidates.append(candidate)
            logger_fetcher.info(
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


# ═══════════════════════════════════════════════════════════════════════════════
#  SHAREPOINT UPLOADER
# ═══════════════════════════════════════════════════════════════════════════════

logger_sp = logging.getLogger("sharepoint_uploader")

FIELD_MAP = {
    "CandidateName": "CandidateName",
    "CandidateEmail": "CandidateEmail",
    "CandidatePhone": "CandidatePhone",
    "JobID": "JobID",
    "JobRole": "JobRole",
}


class SharePointUploader:
    """Uploads files to SharePoint with subfolder routing and metadata tagging."""

    def __init__(self, auth_headers: dict):
        self.headers = auth_headers
        self.base = config.GRAPH_BASE_URL
        self._site_id: str | None = None
        self._drive_id: str | None = None
        self._ensured_folders: set[str] = set()  # cache to avoid redundant checks

    # ── Public ────────────────────────────────────────────────────────────────

    def upload_resume(
        self,
        file_path: str,
        target_filename: str,
        subfolder: str,
        metadata: dict,
    ) -> dict:
        """
        Upload a local PDF to SharePoint under the correct job-specific subfolder
        and tag it with candidate metadata.

        Args:
            file_path:        Local path to the PDF file.
            target_filename:  Desired filename on SharePoint.
            subfolder:        Job-specific subfolder name (e.g. "5101_Trainee_Accountant").
            metadata:         Dict with keys matching FIELD_MAP values.

        Returns:
            The Graph API response for the uploaded DriveItem.
        """
        drive_id = self._get_drive_id()

        # Build full folder path: Resumes/5101_Trainee_Accountant/
        full_folder = f"{config.SHAREPOINT_BASE_FOLDER.strip('/')}/{subfolder}"
        self._ensure_folder(drive_id, full_folder)

        file_size = os.path.getsize(file_path)

        if file_size < 4 * 1024 * 1024:
            item = self._simple_upload(
                drive_id, full_folder, target_filename, file_path
            )
        else:
            item = self._resumable_upload(
                drive_id, full_folder, target_filename, file_path, file_size
            )

        logger_sp.info(
            "Uploaded '%s' → SharePoint:/%s/%s (item id: %s)",
            target_filename,
            full_folder,
            target_filename,
            item.get("id"),
        )

        self._set_metadata(drive_id, item["id"], metadata)
        return item

    # ── Site / Drive Resolution ───────────────────────────────────────────────

    def _get_site_id(self) -> str:
        if self._site_id:
            return self._site_id
        domain = config.SHAREPOINT_SITE_DOMAIN
        path = config.SHAREPOINT_SITE_PATH.strip("/")
        url = f"{self.base}/sites/{domain}:/{path}"
        resp = requests.get(url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        self._site_id = resp.json()["id"]
        logger_sp.debug("Resolved site id: %s", self._site_id)
        return self._site_id

    def _get_drive_id(self) -> str:
        if self._drive_id:
            return self._drive_id
        site_id = self._get_site_id()
        url = f"{self.base}/sites/{site_id}/drives"
        resp = requests.get(url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        drives = resp.json().get("value", [])
        target = config.SHAREPOINT_DRIVE_NAME
        for d in drives:
            if d["name"].lower() == target.lower():
                self._drive_id = d["id"]
                return self._drive_id
        if drives:
            self._drive_id = drives[0]["id"]
            logger_sp.warning(
                "Drive '%s' not found — using default '%s'.", target, drives[0]["name"]
            )
            return self._drive_id
        raise RuntimeError(f"No drives found on site {site_id}")

    # ── Folder Management ─────────────────────────────────────────────────────

    def _ensure_folder(self, drive_id: str, folder_path: str) -> None:
        """Recursively create the folder path if it doesn't exist (cached)."""
        if folder_path in self._ensured_folders:
            return

        parts = folder_path.strip("/").split("/")
        current = ""

        for part in parts:
            current = f"{current}/{part}" if current else part
            if current in self._ensured_folders:
                continue

            encoded = quote(current)
            check_url = f"{self.base}/drives/{drive_id}/root:/{encoded}"
            resp = requests.get(check_url, headers=self.headers, timeout=15)

            if resp.status_code == 404:
                # Create folder
                if "/" in current:
                    parent_encoded = quote("/".join(current.split("/")[:-1]))
                    create_url = f"{self.base}/drives/{drive_id}/root:/{parent_encoded}:/children"
                else:
                    create_url = f"{self.base}/drives/{drive_id}/root/children"

                body = {
                    "name": part,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "fail",
                }
                cr = requests.post(
                    create_url, headers=self.headers, json=body, timeout=15
                )
                if cr.status_code in (
                    201,
                    409,
                ):  # 409 = already exists (race condition)
                    logger_sp.info("Created folder: %s", current)
                else:
                    logger_sp.error(
                        "Could not create folder '%s': %s %s",
                        current,
                        cr.status_code,
                        cr.text,
                    )

            self._ensured_folders.add(current)

    def ensure_base_folder(self) -> None:
        """Ensure the top-level Resumes/ folder exists."""
        drive_id = self._get_drive_id()
        self._ensure_folder(drive_id, config.SHAREPOINT_BASE_FOLDER.strip("/"))

    # ── Upload Methods ────────────────────────────────────────────────────────

    def _simple_upload(
        self, drive_id: str, folder: str, filename: str, file_path: str
    ) -> dict:
        """PUT upload for files < 4 MB."""
        encoded_path = quote(f"{folder}/{filename}")
        url = f"{self.base}/drives/{drive_id}/root:/{encoded_path}:/content"
        with open(file_path, "rb") as f:
            headers = {**self.headers, "Content-Type": "application/pdf"}
            resp = requests.put(url, headers=headers, data=f, timeout=120)
        resp.raise_for_status()
        return resp.json()

    def _resumable_upload(
        self, drive_id: str, folder: str, filename: str, file_path: str, file_size: int
    ) -> dict:
        """Upload session for files >= 4 MB."""
        encoded_path = quote(f"{folder}/{filename}")
        url = f"{self.base}/drives/{drive_id}/root:/{encoded_path}:/createUploadSession"
        body = {
            "item": {
                "@microsoft.graph.conflictBehavior": "rename",
                "name": filename,
            }
        }
        resp = requests.post(url, headers=self.headers, json=body, timeout=30)
        resp.raise_for_status()
        upload_url = resp.json()["uploadUrl"]

        chunk_size = 4 * 1024 * 1024
        with open(file_path, "rb") as f:
            offset = 0
            while offset < file_size:
                chunk = f.read(chunk_size)
                end = offset + len(chunk) - 1
                chunk_headers = {
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {offset}-{end}/{file_size}",
                }
                resp = requests.put(
                    upload_url, headers=chunk_headers, data=chunk, timeout=120
                )
                resp.raise_for_status()
                offset += len(chunk)

        return resp.json()

    # ── Metadata ──────────────────────────────────────────────────────────────

    def _set_metadata(self, drive_id: str, item_id: str, metadata: dict) -> None:
        """Set custom column values on the uploaded file's SharePoint list item."""
        url = f"{self.base}/drives/{drive_id}/items/{item_id}/listItem/fields"
        fields = {FIELD_MAP[k]: v for k, v in metadata.items() if k in FIELD_MAP and v}
        if not fields:
            logger_sp.warning("No metadata to set for item %s", item_id)
            return

        resp = requests.patch(url, headers=self.headers, json=fields, timeout=30)
        if resp.status_code == 200:
            logger_sp.info("Metadata set on item %s: %s", item_id, fields)
        else:
            logger_sp.warning(
                "Failed to set metadata on %s (HTTP %s): %s",
                item_id,
                resp.status_code,
                resp.text,
            )


# ═══════════════════════════════════════════════════════════════════════════════
#  NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════════

logger_notif = logging.getLogger("notifications")


def _teams_stat_column(label: str, value: str, style: str) -> dict:
    return {
        "type": "Column",
        "width": "stretch",
        "items": [
            {
                "type": "TextBlock",
                "text": value,
                "size": "ExtraLarge",
                "weight": "Bolder",
                "horizontalAlignment": "Center",
            },
            {
                "type": "TextBlock",
                "text": label,
                "horizontalAlignment": "Center",
                "spacing": "None",
                "isSubtle": True,
            },
        ],
    }


def _send_teams(results: dict, candidates: list[dict]) -> None:
    """
    Post an Adaptive Card to a Teams channel via an Incoming Webhook.

    Works with both:
      - Legacy Office 365 connectors (being retired)
      - New Workflows-based webhooks (recommended)

    For Workflows webhooks, the payload must be an Adaptive Card wrapped in
    an "attachments" array inside a "body" object.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    total = results["success"] + results["failed"] + results["skipped_no_resume"]

    # Build candidate rows for the table
    candidate_rows = []
    for c in candidates[:20]:  # cap at 20 to avoid oversized payloads
        status_emoji = {"uploaded": "✅", "failed": "❌", "no_resume": "⚠️"}.get(
            c["status"], "❓"
        )
        candidate_rows.append(
            {
                "type": "TableRow",
                "cells": [
                    {
                        "type": "TableCell",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": c.get("name", "—"),
                                "wrap": True,
                            }
                        ],
                    },
                    {
                        "type": "TableCell",
                        "items": [{"type": "TextBlock", "text": c.get("job_id", "—")}],
                    },
                    {
                        "type": "TableCell",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": c.get("job_role", "—"),
                                "wrap": True,
                            }
                        ],
                    },
                    {
                        "type": "TableCell",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": f"{status_emoji} {c['status']}",
                            }
                        ],
                    },
                ],
            }
        )

    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "size": "Large",
                            "weight": "Bolder",
                            "text": f"📄 Resume Pipeline Summary — {now}",
                        },
                        {
                            "type": "ColumnSet",
                            "columns": [
                                _teams_stat_column(
                                    "Uploaded", str(results["success"]), "good"
                                ),
                                _teams_stat_column(
                                    "Failed", str(results["failed"]), "attention"
                                ),
                                _teams_stat_column(
                                    "No Resume",
                                    str(results["skipped_no_resume"]),
                                    "warning",
                                ),
                                _teams_stat_column("Total", str(total), "accent"),
                            ],
                        },
                        {
                            "type": "TextBlock",
                            "text": "**Candidates**",
                            "spacing": "Medium",
                        },
                        {
                            "type": "Table",
                            "columns": [
                                {"width": 2},
                                {"width": 1},
                                {"width": 2},
                                {"width": 1},
                            ],
                            "firstRowAsHeader": True,
                            "rows": [
                                {
                                    "type": "TableRow",
                                    "style": "accent",
                                    "cells": [
                                        {
                                            "type": "TableCell",
                                            "items": [
                                                {
                                                    "type": "TextBlock",
                                                    "text": "Name",
                                                    "weight": "Bolder",
                                                }
                                            ],
                                        },
                                        {
                                            "type": "TableCell",
                                            "items": [
                                                {
                                                    "type": "TextBlock",
                                                    "text": "Job ID",
                                                    "weight": "Bolder",
                                                }
                                            ],
                                        },
                                        {
                                            "type": "TableCell",
                                            "items": [
                                                {
                                                    "type": "TextBlock",
                                                    "text": "Role",
                                                    "weight": "Bolder",
                                                }
                                            ],
                                        },
                                        {
                                            "type": "TableCell",
                                            "items": [
                                                {
                                                    "type": "TextBlock",
                                                    "text": "Status",
                                                    "weight": "Bolder",
                                                }
                                            ],
                                        },
                                    ],
                                },
                                *candidate_rows,
                            ],
                        },
                    ],
                },
            }
        ],
    }

    try:
        resp = requests.post(
            config.TEAMS_WEBHOOK_URL,
            json=card,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code in (200, 202):
            logger_notif.info("Teams notification sent successfully.")
        else:
            logger_notif.warning(
                "Teams webhook returned %s: %s", resp.status_code, resp.text
            )
    except Exception as e:
        logger_notif.error("Failed to send Teams notification: %s", e)


def send_summary(
    results: dict,
    candidates_processed: list[dict],
) -> None:
    """
    Send a run summary to all configured channels.

    Args:
        results: {"success": int, "failed": int, "skipped_no_resume": int}
        candidates_processed: list of dicts with keys:
            name, email, job_id, job_role, status ("uploaded" | "failed" | "no_resume")
    """
    if config.TEAMS_WEBHOOK_URL:
        _send_teams(results, candidates_processed)


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

logger = logging.getLogger("pipeline")

PROCESSED_LOG = "logs/processed_emails.json"


# ─── Logging Setup ────────────────────────────────────────────────────────────


def setup_logging():
    fmt = "%(asctime)s │ %(levelname)-7s │ %(name)-22s │ %(message)s"
    os.makedirs("logs", exist_ok=True)
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOG_FILE, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=fmt,
        handlers=handlers,
    )


# ─── Deduplication ────────────────────────────────────────────────────────────


def load_processed() -> set:
    if os.path.exists(PROCESSED_LOG):
        with open(PROCESSED_LOG, "r") as f:
            return set(json.load(f))
    return set()


def save_processed(ids: set):
    with open(PROCESSED_LOG, "w") as f:
        json.dump(list(ids), f, indent=2)


# ─── File Helpers ─────────────────────────────────────────────────────────────


def download_pdf_from_url(url: str, dest_path: str) -> bool:
    """Download the resume PDF as-is from the URL. No parsing — just a raw download."""
    try:
        resp = requests.get(url, timeout=60, stream=True)
        resp.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        # Quick sanity check: PDF files start with %PDF
        with open(dest_path, "rb") as f:
            header = f.read(5)
        if header != b"%PDF-":
            logger.warning(
                "Downloaded file may not be a PDF (header: %s). Keeping anyway.", header
            )

        logger.info("Downloaded resume → %s", dest_path)
        return True
    except Exception as e:
        logger.error("Failed to download PDF from %s: %s", url, e)
        return False


def build_target_filename(candidate: CandidateInfo) -> str:
    """Generate a clean, descriptive filename for the resume."""
    return config.FILE_NAME_TEMPLATE.format(
        name=candidate.safe_name,
        job_id=candidate.safe_job_id,
        date=candidate.received_date or datetime.now().strftime("%Y-%m-%d"),
    )


def build_subfolder_name(candidate: CandidateInfo) -> str:
    """Generate subfolder name for the job opening, e.g. '5101_Trainee_Accountant'."""
    return config.SUBFOLDER_TEMPLATE.format(
        job_id=candidate.safe_job_id,
        job_role=candidate.safe_job_role,
    )


def unique_path(directory: str, filename: str) -> str:
    """Append a short hash if a file with that name already exists locally."""
    path = os.path.join(directory, filename)
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(filename)
    h = hashlib.md5(f"{filename}{datetime.now().isoformat()}".encode()).hexdigest()[:6]
    return os.path.join(directory, f"{base}_{h}{ext}")


# ─── Pipeline ─────────────────────────────────────────────────────────────────


def run_pipeline():
    setup_logging()
    logger.info("=" * 70)
    logger.info("RESUME PIPELINE — RUN STARTED at %s", datetime.now().isoformat())
    logger.info("=" * 70)

    # ── Step 1: Validate config ──
    missing = []
    for var in ("TENANT_ID", "CLIENT_ID", "CLIENT_SECRET"):
        if not getattr(config, var):
            missing.append(var)
    if missing:
        logger.critical(
            "Missing required config: %s. Set them in .env or environment.", missing
        )
        sys.exit(1)

    # ── Step 2: Authenticate ──
    auth = GraphAuthProvider()
    headers = auth.get_headers()
    logger.info("Authentication successful.")

    # ── Step 3: Fetch & parse emails ──
    fetcher = EmailFetcher(auth_headers=headers)
    candidates = fetcher.fetch_recent_emails()
    logger.info("Found %d candidate emails to process.", len(candidates))

    if not candidates:
        logger.info("Nothing to process. Exiting.")
        return

    # ── Step 4: Deduplication ──
    processed = load_processed()
    new_candidates = [c for c in candidates if c.source_email_id not in processed]
    logger.info(
        "After dedup: %d new emails (skipped %d already processed).",
        len(new_candidates),
        len(candidates) - len(new_candidates),
    )

    # ── Step 5: Prepare ──
    os.makedirs(config.TEMP_DIR, exist_ok=True)
    uploader = SharePointUploader(auth_headers=headers)
    uploader.ensure_base_folder()

    # ── Step 6: Process each candidate ──
    results = {"success": 0, "failed": 0, "skipped_no_resume": 0}
    notification_rows: list[dict] = []

    for candidate in new_candidates:
        logger.info("─" * 60)
        logger.info(
            "Processing: %s | %s | Job: %s [%s]",
            candidate.name,
            candidate.email,
            candidate.job_role,
            candidate.job_id,
        )

        target_filename = build_target_filename(candidate)
        subfolder = build_subfolder_name(candidate)
        local_path = unique_path(config.TEMP_DIR, target_filename)
        downloaded = False

        # Priority 1: Resume URL from the email body
        if candidate.resume_url:
            logger.info("Downloading resume from URL: %s", candidate.resume_url)
            downloaded = download_pdf_from_url(candidate.resume_url, local_path)

        # Priority 2: PDF attachments (fallback if no URL in body)
        if not downloaded and candidate.attachments:
            att = candidate.attachments[0]
            logger.info("Downloading PDF attachment: %s", att["name"])
            try:
                content = fetcher.get_attachment_content(
                    candidate.source_email_id, att["id"]
                )
                with open(local_path, "wb") as f:
                    f.write(content)
                downloaded = True
            except Exception as e:
                logger.error("Attachment download failed: %s", e)

        if not downloaded:
            logger.warning(
                "No downloadable resume for %s. Skipping upload.", candidate.name
            )
            results["skipped_no_resume"] += 1
            notification_rows.append(
                {
                    "name": candidate.name,
                    "email": candidate.email,
                    "job_id": candidate.job_id,
                    "job_role": candidate.job_role,
                    "status": "no_resume",
                }
            )
            processed.add(candidate.source_email_id)
            continue

        # ── Upload to SharePoint with metadata from email fields only ──
        metadata = {
            "CandidateName": candidate.name,
            "CandidateEmail": candidate.email,
            "CandidatePhone": candidate.phone,
            "JobID": candidate.job_id,
            "JobRole": candidate.job_role,
        }

        try:
            uploader.upload_resume(
                file_path=local_path,
                target_filename=target_filename,
                subfolder=subfolder,
                metadata=metadata,
            )
            results["success"] += 1
            status = "uploaded"
            logger.info(
                "✓ Uploaded: %s → Resumes/%s/%s",
                candidate.name,
                subfolder,
                target_filename,
            )
        except Exception as e:
            results["failed"] += 1
            status = "failed"
            logger.error("✗ Upload failed for %s: %s", candidate.name, e)

        notification_rows.append(
            {
                "name": candidate.name,
                "email": candidate.email,
                "job_id": candidate.job_id,
                "job_role": candidate.job_role,
                "status": status,
            }
        )
        processed.add(candidate.source_email_id)

        # Clean up local temp file
        try:
            os.remove(local_path)
        except OSError:
            pass

    # ── Step 7: Save dedup state ──
    save_processed(processed)

    # ── Step 8: Send notification to Teams ──
    if notification_rows:
        send_summary(results, notification_rows)
    else:
        logger.info(
            "No new updates found (all candidates were duplicates). Notification skipped."
        )

    # ── Summary ──
    logger.info("=" * 70)
    logger.info(
        "PIPELINE COMPLETE — Uploaded: %d | Failed: %d | No Resume: %d",
        results["success"],
        results["failed"],
        results["skipped_no_resume"],
    )
    logger.info("=" * 70)


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_pipeline()
