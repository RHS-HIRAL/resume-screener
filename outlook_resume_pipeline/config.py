"""
Configuration for the Outlook Resume Pipeline.
All values are loaded from environment variables or a .env file.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── Entra ID (Azure AD) Credentials ────────────────────────────────────────
TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "")

# ─── Microsoft Graph API ─────────────────────────────────────────────────────
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://graph.microsoft.com/.default"]

# ─── Mailbox to Monitor ──────────────────────────────────────────────────────
MAILBOX_USER = os.getenv("MAILBOX_USER", "recruitment@yourcompany.com")

# ─── Email Filtering ─────────────────────────────────────────────────────────
SUBJECT_KEYWORDS = ["new application received"]
LOOKBACK_HOURS = 24

# ─── SharePoint Target ───────────────────────────────────────────────────────
SHAREPOINT_SITE_DOMAIN = os.getenv(
    "SHAREPOINT_SITE_DOMAIN", "yourcompany.sharepoint.com"
)
SHAREPOINT_SITE_PATH = os.getenv("SHAREPOINT_SITE_PATH", "/sites/Recruitment")
SHAREPOINT_DRIVE_NAME = os.getenv("SHAREPOINT_DRIVE_NAME", "Documents")
SHAREPOINT_BASE_FOLDER = os.getenv("SHAREPOINT_BASE_FOLDER", "Resumes")

# ─── File Naming ──────────────────────────────────────────────────────────────
# Final path: Resumes/{JobID}_{JobRole}/{Name}_{JobID}_{Date}.pdf
FILE_NAME_TEMPLATE = "{name}_{job_id}_{date}.pdf"
SUBFOLDER_TEMPLATE = "{job_id}_{job_role}"

# ─── Notifications ────────────────────────────────────────────────────────────
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL", "")

# ─── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "logs/pipeline.log")

# ─── Local Temp Directory ─────────────────────────────────────────────────────
TEMP_DIR = os.getenv("TEMP_DIR", "./tmp_resumes")
