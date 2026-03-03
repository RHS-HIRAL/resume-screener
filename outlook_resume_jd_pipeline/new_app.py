"""
jd_pipeline.py — Unified Job Description Pipeline
════════════════════════════════════════════════════
Single-pass pipeline that processes each job listing end-to-end:

  For each job discovered on the website:
    1. Parse the job detail page into structured data.
    2. Generate a branded PDF and upload to SharePoint (JobDescriptions/).
    3. Convert the structured data to plain text in memory and upload to
       SharePoint (Text Files/JobDescriptions/<slug>.txt).

  No intermediate JSON files are saved. Everything flows through memory
  per job — the only disk I/O is temporary PDFs during generation.

Usage:
    python jd_pipeline.py
"""

import logging
import os
import re
import shutil
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from urllib.parse import quote, urljoin

import msal
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

load_dotenv()


# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════


class Config:
    """All values loaded from environment variables or .env file."""

    # ─── Entra ID (Azure AD) Credentials ────────────────────────────────────
    TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
    CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
    CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "")

    # ─── Microsoft Graph API ─────────────────────────────────────────────────
    GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
    SCOPES = ["https://graph.microsoft.com/.default"]

    # ─── SharePoint Target ───────────────────────────────────────────────────
    SHAREPOINT_SITE_DOMAIN = os.getenv(
        "SHAREPOINT_SITE_DOMAIN", "yourcompany.sharepoint.com"
    )
    SHAREPOINT_SITE_PATH = os.getenv("SHAREPOINT_SITE_PATH", "/sites/Recruitment")
    SHAREPOINT_DRIVE_NAME = os.getenv("SHAREPOINT_DRIVE_NAME", "Documents")
    SHAREPOINT_JD_FOLDER = os.getenv("SHAREPOINT_JD_FOLDER", "JobDescriptions")

    # ─── Text Files Target ─────────────────────────────────────────────────
    TEXT_JD_FOLDER = os.getenv(
        "SHAREPOINT_TEXT_JD_FOLDER", "Text Files/JobDescriptions"
    )

    # ─── Website to Scrape ───────────────────────────────────────────────────
    JOBS_ARCHIVE_URL = os.getenv("JOBS_ARCHIVE_URL", "https://si2tech.com/jobs/")
    SITE_BASE_URL = os.getenv("SITE_BASE_URL", "https://si2tech.com")

    # ─── Scraping ────────────────────────────────────────────────────────────
    REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.5"))
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
    MAX_PAGES = int(os.getenv("MAX_PAGES", "20"))

    # ─── Logging ─────────────────────────────────────────────────────────────
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("JD_LOG_FILE", "logs/jd_pipeline.log")

    # ─── Local Temp Directory (PDF generation only) ──────────────────────────
    TEMP_DIR = os.getenv("JD_TEMP_DIR", "./tmp_job_descriptions")

    # ─── Notifications ───────────────────────────────────────────────────────
    TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL", "")


# ═══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════════════════


def setup_logging():
    os.makedirs(os.path.dirname(Config.LOG_FILE) or ".", exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s │ %(levelname)-7s │ %(name)-22s │ %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Config.LOG_FILE, encoding="utf-8"),
        ],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)


logger = logging.getLogger("jd_pipeline")


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTH — Unified Microsoft Graph Authentication
# ═══════════════════════════════════════════════════════════════════════════════


class GraphAuthProvider:
    """Handles OAuth2 client-credentials authentication against Microsoft Entra ID."""

    def __init__(self):
        self._app = msal.ConfidentialClientApplication(
            client_id=Config.CLIENT_ID,
            client_credential=Config.CLIENT_SECRET,
            authority=Config.AUTHORITY,
        )

    def get_access_token(self) -> str:
        result = self._app.acquire_token_silent(Config.SCOPES, account=None)
        if not result:
            logger.info("Acquiring new token via client credentials.")
            result = self._app.acquire_token_for_client(scopes=Config.SCOPES)

        if "access_token" in result:
            return result["access_token"]

        error = result.get("error_description", result.get("error", "Unknown error"))
        raise RuntimeError(f"Could not acquire token: {error}")

    def get_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json",
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  JOB DESCRIPTION DATA MODEL
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class JobDescription:
    """Structured data extracted from one job detail page."""

    slug: str = ""
    title: str = ""
    url: str = ""

    # ── Metadata fields ──
    location: str = ""
    job_type: str = ""
    department: str = ""
    shifts: str = ""
    experience: str = ""
    job_category: str = ""
    employment_type: str = ""

    # ── Content sections (preserved in order) ──
    # Each entry: {"heading": str, "paragraphs": [str], "bullets": [str]}
    sections: list[dict] = field(default_factory=list)

    scraped_date: str = ""

    @property
    def safe_slug(self) -> str:
        return re.sub(r"[^\w\-]", "", self.slug).strip() or "unknown"

    @property
    def pdf_filename(self) -> str:
        return f"JD_{self.safe_slug}.pdf"


# ═══════════════════════════════════════════════════════════════════════════════
#  SHAREPOINT MANAGER — Unified Upload, Listing, Metadata for Both Phases
# ═══════════════════════════════════════════════════════════════════════════════

# Maps code-side keys to SharePoint custom column internal names.
JD_FIELD_MAP = {
    "JDTitle": "JDTitle",
    "JDLocation": "JDLocation",
    "JDJobType": "JDJobType",
    "JDDepartment": "JDDepartment",
    "JDExperience": "JDExperience",
    "JDJobCategory": "JDJobCategory",
    "JDScrapedDate": "JDScrapedDate",
    "JDSourceURL": "JDSourceURL",
    "Title": "Title",
}


class SharePointManager:
    """
    Single class for all SharePoint operations:
    - PDF upload + metadata tagging
    - Text file upload + metadata tagging (in-memory content)
    - File existence checks, folder management
    """

    def __init__(self, auth_headers: dict):
        self.headers = auth_headers
        self.base = Config.GRAPH_BASE_URL
        self._site_id: str | None = None
        self._drive_id: str | None = None
        self._ensured_folders: set[str] = set()
        self._existing_jd_pdfs: set[str] | None = None

    # ── Site & Drive Resolution ───────────────────────────────────────────────

    def _get_site_id(self) -> str:
        if self._site_id:
            return self._site_id
        domain = Config.SHAREPOINT_SITE_DOMAIN
        path = Config.SHAREPOINT_SITE_PATH.strip("/")
        url = f"{self.base}/sites/{domain}:/{path}"
        resp = requests.get(url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        self._site_id = resp.json()["id"]
        return self._site_id

    def _get_drive_id(self) -> str:
        if self._drive_id:
            return self._drive_id
        url = f"{self.base}/sites/{self._get_site_id()}/drives"
        resp = requests.get(url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        drives = resp.json().get("value", [])
        target = Config.SHAREPOINT_DRIVE_NAME
        for d in drives:
            if d["name"].lower() == target.lower():
                self._drive_id = d["id"]
                return self._drive_id
        if drives:
            self._drive_id = drives[0]["id"]
            logger.warning(
                "Drive '%s' not found — using default '%s'.", target, drives[0]["name"]
            )
            return self._drive_id
        raise RuntimeError("No drives found on SharePoint site.")

    # ── Folder Management ─────────────────────────────────────────────────────

    def _ensure_folder(self, drive_id: str, folder_path: str) -> None:
        if folder_path in self._ensured_folders:
            return
        current = ""
        for part in folder_path.strip("/").split("/"):
            current = f"{current}/{part}" if current else part
            if current in self._ensured_folders:
                continue

            check_url = f"{self.base}/drives/{drive_id}/root:/{quote(current)}"
            if (
                requests.get(check_url, headers=self.headers, timeout=15).status_code
                == 404
            ):
                if "/" in current:
                    parent_encoded = quote("/".join(current.split("/")[:-1]))
                    create_url = (
                        f"{self.base}/drives/{drive_id}/root:/"
                        f"{parent_encoded}:/children"
                    )
                else:
                    create_url = f"{self.base}/drives/{drive_id}/root/children"

                requests.post(
                    create_url,
                    headers=self.headers,
                    json={
                        "name": part,
                        "folder": {},
                        "@microsoft.graph.conflictBehavior": "fail",
                    },
                    timeout=15,
                )
            self._ensured_folders.add(current)

    # ── File Existence (Generic) ──────────────────────────────────────────────

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists at an arbitrary SharePoint path."""
        drive_id = self._get_drive_id()
        encoded = quote(remote_path.strip("/"))
        url = f"{self.base}/drives/{drive_id}/root:/{encoded}"
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            return resp.status_code == 200
        except Exception:
            return False

    # ── Cached PDF Existence (optimisation) ─────────────────────────────────

    def jd_pdf_exists(self, filename: str) -> bool:
        """Check if a JD PDF exists in the JD folder (cached list)."""
        if self._existing_jd_pdfs is None:
            self._existing_jd_pdfs = self._list_existing_jd_pdfs()
        return filename.lower() in self._existing_jd_pdfs

    def _list_existing_jd_pdfs(self) -> set[str]:
        try:
            drive_id = self._get_drive_id()
            folder = Config.SHAREPOINT_JD_FOLDER.strip("/")
            url = (
                f"{self.base}/drives/{drive_id}/root:/{quote(folder)}:/children"
                f"?$select=name&$top=1000"
            )
            filenames: set[str] = set()
            while url:
                resp = requests.get(url, headers=self.headers, timeout=30)
                if resp.status_code == 404:
                    return set()
                resp.raise_for_status()
                data = resp.json()
                for item in data.get("value", []):
                    filenames.add(item["name"].lower())
                url = data.get("@odata.nextLink")
            logger.info(
                "Found %d existing PDFs in SharePoint:/%s/", len(filenames), folder
            )
            return filenames
        except Exception as e:
            logger.warning("Could not list existing JD PDFs: %s", e)
            return set()

    # ── Upload Methods ────────────────────────────────────────────────────────

    def _get_content_type(self, filename: str) -> str:
        ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
        return {
            "pdf": "application/pdf",
            "txt": "text/plain; charset=utf-8",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "json": "application/json",
        }.get(ext, "application/octet-stream")

    def _simple_upload(
        self, drive_id: str, folder: str, filename: str, file_path: str
    ) -> dict:
        encoded_path = quote(f"{folder}/{filename}")
        url = f"{self.base}/drives/{drive_id}/root:/{encoded_path}:/content"
        headers = {**self.headers, "Content-Type": self._get_content_type(filename)}
        with open(file_path, "rb") as f:
            resp = requests.put(url, headers=headers, data=f, timeout=120)
        resp.raise_for_status()
        return resp.json()

    def _resumable_upload(
        self, drive_id: str, folder: str, filename: str, file_path: str, file_size: int
    ) -> dict:
        encoded_path = quote(f"{folder}/{filename}")
        url = f"{self.base}/drives/{drive_id}/root:/{encoded_path}:/createUploadSession"
        body = {
            "item": {
                "@microsoft.graph.conflictBehavior": "replace",
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

    def _upload_file(self, folder: str, filename: str, file_path: str) -> dict:
        """Upload a file to a SharePoint folder. Handles small vs large files."""
        drive_id = self._get_drive_id()
        self._ensure_folder(drive_id, folder)

        file_size = os.path.getsize(file_path)
        if file_size < 4 * 1024 * 1024:
            return self._simple_upload(drive_id, folder, filename, file_path)
        else:
            return self._resumable_upload(
                drive_id, folder, filename, file_path, file_size
            )

    def _upload_content(self, folder: str, filename: str, content: bytes) -> dict:
        """Upload raw bytes/text content to SharePoint without a temp file."""
        drive_id = self._get_drive_id()
        self._ensure_folder(drive_id, folder)
        encoded_path = quote(f"{folder}/{filename}")
        url = f"{self.base}/drives/{drive_id}/root:/{encoded_path}:/content"
        headers = {**self.headers, "Content-Type": self._get_content_type(filename)}
        resp = requests.put(url, headers=headers, data=content, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # ── Metadata ──────────────────────────────────────────────────────────────

    def set_metadata(self, item_id: str, metadata: dict) -> None:
        """Set custom column values on an uploaded file's SharePoint list item."""
        if not item_id or item_id == "resumable_upload_complete":
            return
        drive_id = self._get_drive_id()
        url = f"{self.base}/drives/{drive_id}/items/{item_id}/listItem/fields"
        fields = {
            JD_FIELD_MAP[k]: v for k, v in metadata.items() if k in JD_FIELD_MAP and v
        }
        if not fields:
            return

        resp = requests.patch(url, headers=self.headers, json=fields, timeout=30)
        if resp.status_code == 200:
            logger.info("  Metadata set on item %s: %s", item_id, list(fields.keys()))
        else:
            # Retry with only standard 'Title' if custom fields fail
            if any(k != "Title" for k in fields):
                logger.warning(
                    "  Metadata patch partial failure (%d). Retrying with safe fields.",
                    resp.status_code,
                )
                safe_fields = {k: v for k, v in fields.items() if k == "Title"}
                if safe_fields:
                    requests.patch(
                        url, headers=self.headers, json=safe_fields, timeout=30
                    )
            else:
                logger.warning(
                    "  Metadata patch failed (%d): %s",
                    resp.status_code,
                    resp.text[:200],
                )

    # ── Upload JD PDF ─────────────────────────────────────────────────────

    def upload_jd_pdf(
        self, file_path: str, target_filename: str, metadata: dict | None = None
    ) -> dict:
        """Upload a JD PDF to JobDescriptions/ and tag with metadata."""
        folder = Config.SHAREPOINT_JD_FOLDER.strip("/")
        item = self._upload_file(folder, target_filename, file_path)

        logger.info(
            "  Uploaded PDF '%s' → SharePoint:/%s/ (id: %s)",
            target_filename,
            folder,
            item.get("id"),
        )

        if metadata:
            self.set_metadata(item["id"], metadata)

        if self._existing_jd_pdfs is not None:
            self._existing_jd_pdfs.add(target_filename.lower())

        return item

    # ── Upload JD Text File ───────────────────────────────────────────────

    def upload_jd_text(
        self,
        text_content: str,
        filename: str,
        metadata: dict | None = None,
        skip_existing: bool = True,
    ) -> dict | None:
        """
        Upload a .txt JD file to Text Files/JobDescriptions/.
        Returns the Graph API response dict, or None if skipped.
        """
        folder = Config.TEXT_JD_FOLDER.strip("/")
        remote_path = f"{folder}/{filename}"

        if skip_existing and self.file_exists(remote_path):
            logger.info("  ⏭️  Skipping (already exists): %s", filename)
            return None

        item = self._upload_content(folder, filename, text_content.encode("utf-8"))

        logger.info("  ✅ Uploaded text: %s (id: %s)", filename, item.get("id"))

        if metadata:
            self.set_metadata(item["id"], metadata)

        return item


# ═══════════════════════════════════════════════════════════════════════════════
#  WEBSITE SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

# HTTP session with browser-like headers for polite scraping
_session = requests.Session()
_session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
)


def _polite_get(url: str) -> requests.Response:
    """GET with a polite delay between requests."""
    time.sleep(Config.REQUEST_DELAY)
    resp = _session.get(url, timeout=Config.REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp


# ─── Step 1: Discover all job URLs from /jobs/ archive pages ──────────────────


def discover_job_urls() -> list[dict]:
    """
    Crawl the WordPress job archive at /jobs/, /jobs/page/2/, etc.
    Returns a deduplicated list of {"url": str, "title": str}.
    """
    jobs: dict[str, str] = {}
    current_url = Config.JOBS_ARCHIVE_URL
    page_num = 0

    while current_url and page_num < Config.MAX_PAGES:
        page_num += 1
        logger.info("Crawling archive page %d: %s", page_num, current_url)

        try:
            resp = _polite_get(current_url)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                logger.info("Page %d returned 404 — end of archive.", page_num)
                break
            raise

        soup = BeautifulSoup(resp.text, "html.parser")

        found_on_page = 0
        for h2 in soup.find_all("h2"):
            a_tag = h2.find("a", href=True)
            if not a_tag:
                continue
            href = a_tag["href"]
            if "/jobs/" not in href:
                continue
            full_url = urljoin(Config.SITE_BASE_URL, href).rstrip("/") + "/"
            slug = full_url.rstrip("/").split("/jobs/")[-1].split("/")[0]
            if not slug or slug == "page":
                continue

            title = a_tag.get_text(strip=True)
            if full_url not in jobs:
                jobs[full_url] = title
                found_on_page += 1

        logger.info("  Found %d jobs on page %d.", found_on_page, page_num)

        # Follow "Next" pagination link
        next_link = None
        for a_tag in soup.find_all("a", href=True):
            text = a_tag.get_text(strip=True)
            href = a_tag["href"]
            if ("next" in text.lower() or "\u2192" in text) and "/jobs/page/" in href:
                next_link = urljoin(Config.SITE_BASE_URL, href)
                break

        if next_link and next_link != current_url:
            current_url = next_link
        else:
            break

    result = [{"url": url, "title": title} for url, title in jobs.items()]
    logger.info("Total unique jobs discovered: %d", len(result))
    return result


# ─── Step 2: Parse one job detail page ────────────────────────────────────────

META_LABEL_MAP = {
    "location": "location",
    "job type": "job_type",
    "internship type": "job_type",
    "department": "department",
    "shift": "shifts",
    "experience": "experience",
    "job category": "job_category",
    "employment type": "employment_type",
    "job title": "_skip",
    "internship title": "_skip",
    "job location": "location",
}

SECTION_KEYWORDS = [
    "job summary",
    "role overview",
    "job description",
    "position summary",
    "key responsibilities",
    "responsibilities",
    "must have skills",
    "must-have skills",
    "required skills",
    "technical skills",
    "knowledge",
    "good to have",
    "preferred qualifications",
    "nice to have",
    "preferred certification",
    "certifications",
    "certification",
    "qualifications",
    "candidate requirements",
    "education",
    "integrations",
    "data management",
    "reporting",
    "dashboards",
    "administration",
    "support",
    "monitoring",
    "security strategy",
    "governance",
    "incident response",
    "threat",
    "soc operations",
    "leadership",
    "management",
    "jira configuration",
    "customization",
]


def parse_job_detail(url: str, fallback_title: str = "") -> JobDescription:
    resp = _polite_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    slug = url.rstrip("/").split("/")[-1]

    jd = JobDescription(
        slug=slug,
        url=url,
        scraped_date=datetime.now().strftime("%Y-%m-%d"),
    )

    # Title from <h1>
    h1 = soup.find("h1")
    jd.title = h1.get_text(strip=True) if h1 else fallback_title

    content_div = soup.find("div", class_="entry-content")
    if not content_div:
        content_div = soup.find("article")

    if not content_div:
        logger.warning("No content container found for %s", url)
        return jd

    # Walk elements sequentially
    current_heading = "Job Description"
    current_paragraphs: list[str] = []
    current_bullets: list[str] = []

    def _flush():
        nonlocal current_heading, current_paragraphs, current_bullets
        if current_paragraphs or current_bullets:
            jd.sections.append(
                {
                    "heading": current_heading,
                    "paragraphs": list(current_paragraphs),
                    "bullets": list(current_bullets),
                }
            )
        current_paragraphs.clear()
        current_bullets.clear()

    tags_to_parse = content_div.find_all(
        ["p", "ul", "ol", "h2", "h3", "h4", "h5", "div"], recursive=True
    )
    processed_elements = set()

    for elem in tags_to_parse:
        if elem in processed_elements:
            continue

        for child in elem.find_all(["p", "ul", "ol", "h2", "h3", "h4", "h5"]):
            processed_elements.add(child)

        text_content = elem.get_text(strip=True)
        if not text_content:
            continue

        tag_name = elem.name.lower()

        # Headers: start new section
        if tag_name in ("h2", "h3", "h4", "h5") or (
            tag_name == "p" and _is_section_heading(text_content)
        ):
            if "apply for this position" in text_content.lower():
                break

            _flush()
            current_heading = text_content.rstrip(":")
            continue

        # <p> tags
        if tag_name == "p":
            match = re.match(r"^([A-Za-z\s]+)\s*[:]\s*(.*)", text_content)
            if match:
                label, value = match.groups()
                if _try_set_metadata(jd, label, value):
                    continue

            current_paragraphs.append(elem.decode_contents())

        # <ul>/<ol> bullet lists
        elif tag_name in ("ul", "ol"):
            for li in elem.find_all("li", recursive=False):
                li_text = li.get_text(" ", strip=True)
                if li_text:
                    current_bullets.append(li_text)

    _flush()

    # Extract footer metadata
    _extract_footer_metadata(soup, jd)

    return jd


def _try_set_metadata(jd: JobDescription, label: str, value: str) -> bool:
    label_lower = label.lower().strip()
    for key_substr, attr_name in META_LABEL_MAP.items():
        if key_substr in label_lower:
            if attr_name == "_skip":
                return True
            if not getattr(jd, attr_name):
                setattr(jd, attr_name, value)
            return True
    return False


def _is_section_heading(text: str) -> bool:
    if not text or len(text) < 3 or len(text) > 80:
        return False
    text_lower = text.lower()
    skip_exact = {
        "apply",
        "submit",
        "home",
        "careers",
        "full name",
        "email",
        "phone",
        "cover letter",
        "upload",
        "contact",
        "si2 technologies",
    }
    if text_lower in skip_exact:
        return False
    for kw in SECTION_KEYWORDS:
        if kw in text_lower:
            return True
    if ":" not in text and len(text) < 40 and text[0].isupper():
        return True
    return False


def _extract_footer_metadata(soup: BeautifulSoup, jd: JobDescription) -> None:
    full_text = soup.get_text(separator="\n")
    patterns = [
        (r"Job\s+Category\s*:\s*(.+)", "job_category"),
        (r"Job\s+Location\s*:\s*(.+)", "location"),
    ]
    for pattern, attr in patterns:
        if not getattr(jd, attr):
            m = re.search(pattern, full_text, re.I)
            if m:
                val = m.group(1).strip().split("\n")[0].strip()
                if val and len(val) < 120:
                    setattr(jd, attr, val)


# ═══════════════════════════════════════════════════════════════════════════════
#  STRUCTURED FIELD EXTRACTION (for scorer compatibility)
# ═══════════════════════════════════════════════════════════════════════════════

_SKILL_HEADINGS = {
    "must have skills",
    "must-have skills",
    "required skills",
    "technical skills",
    "key skills",
    "core competencies",
    "skills required",
    "skills",
}
_TOOL_HEADINGS = {
    "tools",
    "technologies",
    "tech stack",
    "platforms",
    "software",
    "tools and technologies",
}
_EDUCATION_HEADINGS = {
    "qualifications",
    "education",
    "academic qualifications",
    "candidate requirements",
    "required qualifications",
    "eligibility",
}
_RESPONSIBILITY_HEADINGS = {
    "responsibilities",
    "key responsibilities",
    "job description",
    "job summary",
    "role overview",
    "position summary",
    "duties",
    "what you will do",
}
_GOOD_TO_HAVE_HEADINGS = {
    "good to have",
    "good to have skills",
    "nice to have",
    "preferred qualifications",
    "preferred skills",
    "preferred certification",
    "certifications",
    "certification",
}


def extract_structured_jd_fields(jd_dict: dict) -> dict:
    """
    Extract scorer-compatible structured fields from the raw JD dict.
    Reads sections and populates required_skills, good_to_have_skills,
    required_tools, min_education, required_experience_years, responsibilities_text.
    Returns the original dict with these keys added/merged.
    """
    sections = jd_dict.get("sections", [])

    required_skills: list[str] = []
    good_to_have_skills: list[str] = []
    required_tools: list[str] = []
    education_bullets: list[str] = []
    responsibilities_parts: list[str] = []

    for sec in sections:
        heading = sec.get("heading", "").strip()
        heading_lower = heading.lower().rstrip(":")
        bullets = sec.get("bullets", [])
        paragraphs = sec.get("paragraphs", [])

        # Skills
        if heading_lower in _SKILL_HEADINGS or any(
            kw in heading_lower
            for kw in ("must have", "required skill", "technical skill", "key skill")
        ):
            required_skills.extend(_clean_html(b) for b in bullets if b.strip())
            for p in paragraphs:
                cleaned = _clean_html(p)
                if cleaned and not cleaned.endswith(":"):
                    required_skills.append(cleaned)
            continue

        # Tools
        if heading_lower in _TOOL_HEADINGS or any(
            kw in heading_lower for kw in ("tools", "technologies", "tech stack")
        ):
            required_tools.extend(_clean_html(b) for b in bullets if b.strip())
            continue

        # Good to have
        if heading_lower in _GOOD_TO_HAVE_HEADINGS or any(
            kw in heading_lower for kw in ("good to have", "nice to have", "preferred")
        ):
            good_to_have_skills.extend(_clean_html(b) for b in bullets if b.strip())
            continue

        # Education / Qualifications
        if heading_lower in _EDUCATION_HEADINGS or any(
            kw in heading_lower for kw in ("qualif", "education", "eligib")
        ):
            education_bullets.extend(_clean_html(b) for b in bullets if b.strip())
            for p in paragraphs:
                cleaned = _clean_html(p)
                if cleaned:
                    education_bullets.append(cleaned)
            continue

        # Responsibilities / Job Description
        if heading_lower in _RESPONSIBILITY_HEADINGS or any(
            kw in heading_lower
            for kw in ("responsibilit", "job description", "job summary", "dut")
        ):
            for p in paragraphs:
                cleaned = _clean_html(p)
                if cleaned:
                    responsibilities_parts.append(cleaned)
            for b in bullets:
                cleaned = _clean_html(b)
                if cleaned:
                    responsibilities_parts.append(cleaned)
            continue

        # Catch-all: add to responsibilities
        for b in bullets:
            cleaned = _clean_html(b)
            if cleaned:
                responsibilities_parts.append(cleaned)
        for p in paragraphs:
            cleaned = _clean_html(p)
            if cleaned and not cleaned.endswith(":"):
                responsibilities_parts.append(cleaned)

    # Parse experience years
    exp_text = jd_dict.get("experience", "") or ""
    required_yoe = _parse_experience_years(exp_text)

    min_education = "; ".join(education_bullets) if education_bullets else ""

    jd_dict["required_skills"] = required_skills
    jd_dict["good_to_have_skills"] = good_to_have_skills
    jd_dict["required_tools"] = required_tools
    jd_dict["min_education"] = min_education
    jd_dict["required_experience_years"] = required_yoe
    jd_dict["responsibilities_text"] = " ".join(responsibilities_parts)

    return jd_dict


def _clean_html(text: str) -> str:
    if not text:
        return ""
    return BeautifulSoup(text, "html.parser").get_text(strip=True)


def _parse_experience_years(text: str) -> int:
    if not text:
        return 0
    m = re.search(r"(\d+)\s*[+\-–—]?", text)
    if m:
        return int(m.group(1))
    return 0


# ═══════════════════════════════════════════════════════════════════════════════
#  PDF GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

# Brand Colors
C_PRIMARY = HexColor("#0F3A68")
C_ACCENT = HexColor("#1976D2")
C_BG_LIGHT = HexColor("#EDF4FC")
C_TEXT = HexColor("#222222")
C_SUBTLE = HexColor("#555555")
C_DIVIDER = HexColor("#B0C4DE")


def _build_pdf_styles() -> dict:
    """Custom ReportLab paragraph styles for a professional JD PDF."""
    base = getSampleStyleSheet()
    s = {}

    s["CompanyName"] = ParagraphStyle(
        "CompanyName",
        fontName="Helvetica-Bold",
        fontSize=10,
        textColor=C_ACCENT,
        spaceAfter=0,
    )
    s["Title"] = ParagraphStyle(
        "JDTitle",
        parent=base["Title"],
        fontSize=18,
        leading=24,
        textColor=C_PRIMARY,
        spaceAfter=4,
        alignment=TA_LEFT,
        fontName="Helvetica-Bold",
    )
    s["MetaCell"] = ParagraphStyle(
        "MetaCell",
        fontName="Helvetica",
        fontSize=9,
        leading=13,
        textColor=C_TEXT,
        spaceAfter=0,
    )
    s["SectionHeading"] = ParagraphStyle(
        "SectionHeading",
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=17,
        textColor=C_PRIMARY,
        spaceBefore=14,
        spaceAfter=5,
    )
    s["Body"] = ParagraphStyle(
        "Body",
        fontName="Helvetica",
        fontSize=9.5,
        leading=14.5,
        textColor=C_TEXT,
        alignment=TA_JUSTIFY,
        spaceAfter=5,
    )
    s["BodyBold"] = ParagraphStyle(
        "BodyBold",
        fontName="Helvetica",
        fontSize=9.5,
        leading=14.5,
        textColor=C_TEXT,
        alignment=TA_LEFT,
        spaceAfter=3,
    )
    s["Bullet"] = ParagraphStyle(
        "Bullet",
        fontName="Helvetica",
        fontSize=9.5,
        leading=14,
        textColor=C_TEXT,
        leftIndent=18,
        bulletIndent=6,
        spaceAfter=3,
    )
    s["SubBullet"] = ParagraphStyle(
        "SubBullet",
        fontName="Helvetica",
        fontSize=9,
        leading=13,
        textColor=C_SUBTLE,
        leftIndent=36,
        bulletIndent=22,
        spaceAfter=2,
    )
    s["Footer"] = ParagraphStyle(
        "Footer",
        fontName="Helvetica",
        fontSize=7,
        textColor=C_SUBTLE,
        alignment=TA_CENTER,
    )
    return s


def generate_job_pdf(jd: JobDescription, output_path: str) -> str:
    """Generate a branded, professional PDF for a single JD. Returns output_path."""
    styles = _build_pdf_styles()
    story: list = []
    W = A4[0] - 1.5 * inch

    # Header bar
    story.append(Paragraph("Si2 Technologies", styles["CompanyName"]))
    story.append(Spacer(1, 2))
    story.append(
        HRFlowable(width="100%", thickness=2.5, color=C_PRIMARY, spaceAfter=10)
    )

    # Job Title
    story.append(Paragraph(_safe(jd.title) or "Job Description", styles["Title"]))
    story.append(Spacer(1, 4))

    # Metadata grid
    meta_pairs = _build_meta_pairs(jd)
    if meta_pairs:
        rows = []
        for i in range(0, len(meta_pairs), 2):
            row = []
            for j in range(2):
                idx = i + j
                if idx < len(meta_pairs):
                    label, value = meta_pairs[idx]
                    cell = Paragraph(
                        f'<font name="Helvetica-Bold" size="7.5" '
                        f'color="#{C_SUBTLE.hexval()[2:]}">'
                        f"{_safe(label)}</font><br/>"
                        f'<font name="Helvetica" size="9.5" '
                        f'color="#{C_TEXT.hexval()[2:]}">'
                        f"{_safe(value)}</font>",
                        styles["MetaCell"],
                    )
                    row.append(cell)
                else:
                    row.append("")
            rows.append(row)

        col_w = W / 2
        tbl = Table(rows, colWidths=[col_w, col_w])
        tbl.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("BACKGROUND", (0, 0), (-1, -1), C_BG_LIGHT),
                    ("BOX", (0, 0), (-1, -1), 0.5, C_DIVIDER),
                    ("INNERGRID", (0, 0), (-1, -1), 0.3, C_DIVIDER),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ]
            )
        )
        story.append(tbl)
        story.append(Spacer(1, 8))

    story.append(HRFlowable(width="100%", thickness=0.5, color=C_DIVIDER, spaceAfter=4))

    # Content sections
    for section in jd.sections:
        heading = section["heading"]
        paragraphs = section.get("paragraphs", [])
        bullets = section.get("bullets", [])

        if not paragraphs and not bullets:
            continue

        story.append(Spacer(1, 4))
        accent_hex = C_ACCENT.hexval()[2:]
        story.append(
            Paragraph(
                f'<font color="#{accent_hex}">|</font>&nbsp;&nbsp;{_safe(heading)}',
                styles["SectionHeading"],
            )
        )

        for p in paragraphs:
            if "<b>" in p:
                story.append(Paragraph(p, styles["BodyBold"]))
            else:
                story.append(Paragraph(_safe(p), styles["Body"]))

        for b in bullets:
            if b.startswith("    "):
                story.append(
                    Paragraph(f"\u2013  {_safe(b.strip())}", styles["SubBullet"])
                )
            else:
                story.append(Paragraph(f"\u2022  {_safe(b)}", styles["Bullet"]))

    # Footer
    story.append(Spacer(1, 20))
    story.append(
        HRFlowable(
            width="100%", thickness=0.5, color=C_DIVIDER, spaceBefore=8, spaceAfter=6
        )
    )
    story.append(
        Paragraph(
            f"Source: {_safe(jd.url)}  |  Scraped: {jd.scraped_date}  "
            f"|  Si2 Technologies  |  Confidential",
            styles["Footer"],
        )
    )

    # Build PDF
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.6 * inch,
        title=jd.title or "Job Description",
        author="Si2 Technologies - JD Pipeline",
    )
    doc.build(story)
    logger.info("  Generated PDF: %s (%d sections)", output_path, len(jd.sections))
    return output_path


def _build_meta_pairs(jd: JobDescription) -> list[tuple[str, str]]:
    pairs = []
    combined_job_type = " / ".join(filter(None, [jd.job_type, jd.employment_type]))
    for label, val in [
        ("Location", jd.location),
        ("Job Type", combined_job_type),
        ("Department", jd.department),
        ("Experience", jd.experience),
        ("Shifts", jd.shifts),
        ("Job Category", jd.job_category),
    ]:
        if val:
            pairs.append((label, val))
    return pairs


def _safe(text: str) -> str:
    """Escape text for ReportLab Paragraph XML."""
    if not text:
        return ""
    clean_text = BeautifulSoup(text, "html.parser").get_text()
    clean_text = clean_text.replace("&", "&amp;")
    clean_text = clean_text.replace("<", "&lt;")
    clean_text = clean_text.replace(">", "&gt;")
    return clean_text


# ═══════════════════════════════════════════════════════════════════════════════
#  JSON → TEXT CONVERSION (from experiment_json_to_text_jd.py)
# ═══════════════════════════════════════════════════════════════════════════════

# Metadata fields to include in the text rendering
_TEXT_META_FIELDS = {
    "title": "Job Title",
    "location": "Location",
    "job_type": "Job Type",
    "department": "Department",
    "shifts": "Shifts",
    "experience": "Experience Required",
}


def _bullets_to_prose(bullets: list[str]) -> str:
    """Convert a list of bullet strings into a single prose sentence."""
    cleaned = [b.strip(" .,") for b in bullets if b.strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0] + "."
    return ", ".join(cleaned[:-1]) + ", and " + cleaned[-1] + "."


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def _section_to_prose(section: dict) -> str:
    parts = []
    heading = section.get("heading", "").strip()
    paragraphs = [_strip_html(p) for p in section.get("paragraphs", []) if p.strip()]
    bullets = [_strip_html(b) for b in section.get("bullets", []) if b.strip()]

    if paragraphs:
        parts.append(" ".join(paragraphs))
    if bullets:
        parts.append(_bullets_to_prose(bullets))

    if not parts:
        return ""

    body = " ".join(parts)
    return f"{heading}: {body}" if heading else body


def json_to_text(data: dict) -> str:
    """Convert a structured JD JSON dict into a plain-text summary."""
    lines = []

    # Metadata block
    meta_parts = []
    for field_key, label in _TEXT_META_FIELDS.items():
        value = data.get(field_key, "")
        if isinstance(value, str):
            value = value.strip()
        if value:
            meta_parts.append(f"{label}: {value}")
    if meta_parts:
        lines.append(". ".join(meta_parts) + ".")

    required = data.get("required_skills", [])
    good_to_have = data.get("good_to_have_skills", [])

    if required:
        lines.append("Required Skills: " + _bullets_to_prose(required))
    if good_to_have:
        lines.append("Good to Have Skills: " + _bullets_to_prose(good_to_have))

    for section in data.get("sections", []):
        heading = section.get("heading", "")
        if heading in ("Must Have Skills", "Good to Have Skills"):
            continue
        prose = _section_to_prose(section)
        if prose:
            lines.append(prose)

    return "\n\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
#  TEAMS NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def send_jd_summary(results: dict) -> None:
    """Send a summary of the JD pipeline run to Teams."""
    if not Config.TEAMS_WEBHOOK_URL:
        return

    if results.get("uploaded", 0) == 0 and results.get("text_uploaded", 0) == 0:
        logger.info("No new JDs uploaded. Skipping Teams notification.")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

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
                            "text": f"Job Description Pipeline — {now}",
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {
                                    "title": "PDFs Uploaded",
                                    "value": str(results.get("uploaded", 0)),
                                },
                                {
                                    "title": "PDFs Skipped",
                                    "value": str(results.get("skipped", 0)),
                                },
                                {
                                    "title": "PDFs Failed",
                                    "value": str(results.get("failed", 0)),
                                },
                                {
                                    "title": "Text Files Uploaded",
                                    "value": str(results.get("text_uploaded", 0)),
                                },
                                {
                                    "title": "Text Files Skipped",
                                    "value": str(results.get("text_skipped", 0)),
                                },
                            ],
                        },
                    ],
                },
            }
        ],
    }

    try:
        requests.post(
            Config.TEAMS_WEBHOOK_URL,
            json=card,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
#  PIPELINE — Scrape → Parse → PDF Upload → Text Upload (per job, single pass)
# ═══════════════════════════════════════════════════════════════════════════════


def run_pipeline(sp: SharePointManager) -> dict:
    """
    Single-pass pipeline. For each job discovered on the website:
      1. Parse the job detail page into a structured JobDescription.
      2. Extract structured fields (skills, tools, education, etc.).
      3. Generate a branded PDF and upload to SharePoint.
      4. Convert structured data → plain text in memory → upload to SharePoint.

    No intermediate files are saved except temporary PDFs during generation.
    Returns a results dict with counts.
    """
    logger.info("=" * 70)
    logger.info("JD PIPELINE — SCRAPE → PDF → TEXT (single pass)")
    logger.info("=" * 70)

    # Discover all job listing URLs
    logger.info("Discovering jobs from %s …", Config.JOBS_ARCHIVE_URL)
    job_urls = discover_job_urls()
    logger.info("Found %d job listings total.", len(job_urls))

    if not job_urls:
        logger.info("No job listings found.")
        return {
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "text_uploaded": 0,
            "text_skipped": 0,
            "text_failed": 0,
        }

    os.makedirs(Config.TEMP_DIR, exist_ok=True)

    results = {
        "uploaded": 0,
        "skipped": 0,
        "failed": 0,
        "text_uploaded": 0,
        "text_skipped": 0,
        "text_failed": 0,
    }

    for idx, job_info in enumerate(job_urls, 1):
        url = job_info["url"]
        title = job_info.get("title", "")
        slug = url.rstrip("/").split("/")[-1]
        safe_slug = re.sub(r"[^\w\-]", "", slug)
        pdf_filename = f"JD_{safe_slug}.pdf"
        txt_filename = f"JD_{safe_slug}.txt"

        logger.info("─" * 60)
        logger.info("[%d/%d] %s (%s)", idx, len(job_urls), title or slug, pdf_filename)

        # Check if PDF already exists on SharePoint
        if sp.jd_pdf_exists(pdf_filename):
            logger.info("  SKIP — PDF already exists on SharePoint.")
            results["skipped"] += 1
            # Still check if text file needs uploading for this job
            # (handles case where PDF was uploaded in a prior run but text wasn't)
            txt_remote = f"{Config.TEXT_JD_FOLDER.strip('/')}/{txt_filename}"
            if sp.file_exists(txt_remote):
                results["text_skipped"] += 1
            else:
                logger.info(
                    "  PDF exists but text file missing — scraping for text only."
                )
                # Fall through to parse + text upload (skip PDF upload below)
                try:
                    jd = parse_job_detail(url, fallback_title=title)
                    jd_dict = asdict(jd)
                    jd_dict = extract_structured_jd_fields(jd_dict)
                    text_content = json_to_text(jd_dict)
                    if text_content.strip():
                        resp = sp.upload_jd_text(
                            text_content=text_content,
                            filename=txt_filename,
                            metadata={"Title": jd.title, "JDTitle": jd.title},
                            skip_existing=False,
                        )
                        if resp:
                            results["text_uploaded"] += 1
                    else:
                        results["text_failed"] += 1
                except Exception as e:
                    logger.error("  FAIL — text-only upload: %s", e)
                    results["text_failed"] += 1
            continue

        # ── Step 1: Parse job detail page ──
        try:
            jd = parse_job_detail(url, fallback_title=title)
            logger.info(
                "  Parsed: title='%s' | location='%s' | dept='%s' "
                "| exp='%s' | sections=%d",
                jd.title,
                jd.location,
                jd.department,
                jd.experience,
                len(jd.sections),
            )
        except Exception as e:
            logger.error("  FAIL — parse error for %s: %s", url, e, exc_info=True)
            results["failed"] += 1
            results["text_failed"] += 1
            continue

        # ── Step 2: Extract structured fields (in memory) ──
        jd_dict = asdict(jd)
        jd_dict = extract_structured_jd_fields(jd_dict)

        logger.info(
            "  Structured: skills=%d, tools=%d, yoe=%s",
            len(jd_dict.get("required_skills", [])),
            len(jd_dict.get("required_tools", [])),
            jd_dict.get("required_experience_years", 0),
        )

        # ── Step 3: Generate PDF & upload ──
        local_pdf_path = os.path.join(Config.TEMP_DIR, pdf_filename)
        try:
            generate_job_pdf(jd, local_pdf_path)
        except Exception as e:
            logger.error("  FAIL — PDF generation: %s", e, exc_info=True)
            results["failed"] += 1
            # Still attempt text upload even if PDF fails
            local_pdf_path = None

        if local_pdf_path and os.path.exists(local_pdf_path):
            try:
                combined_job_type = " / ".join(
                    filter(None, [jd.job_type, jd.employment_type])
                )
                jd_metadata = {
                    "JDTitle": jd.title,
                    "JDLocation": jd.location,
                    "JDJobType": combined_job_type,
                    "JDDepartment": jd.department,
                    "JDExperience": jd.experience,
                    "JDJobCategory": jd.job_category,
                    "JDScrapedDate": jd.scraped_date,
                    "JDSourceURL": jd.url,
                }
                sp.upload_jd_pdf(
                    file_path=local_pdf_path,
                    target_filename=pdf_filename,
                    metadata=jd_metadata,
                )
                results["uploaded"] += 1
            except Exception as e:
                results["failed"] += 1
                logger.error("  FAIL — PDF upload: %s", e, exc_info=True)

            # Clean up temp PDF
            try:
                os.remove(local_pdf_path)
            except OSError:
                pass

        # ── Step 4: Convert to text (in memory) & upload ──
        try:
            text_content = json_to_text(jd_dict)
            if not text_content.strip():
                logger.warning(
                    "  ⚠️ Text conversion produced empty result. Skipping text upload."
                )
                results["text_failed"] += 1
            else:
                resp = sp.upload_jd_text(
                    text_content=text_content,
                    filename=txt_filename,
                    metadata={"Title": jd.title, "JDTitle": jd.title},
                    skip_existing=True,
                )
                if resp is None:
                    results["text_skipped"] += 1
                else:
                    results["text_uploaded"] += 1
        except Exception as e:
            logger.error("  FAIL — text upload: %s", e)
            results["text_failed"] += 1

    # ── Summary ──
    logger.info("=" * 70)
    logger.info(
        "PIPELINE COMPLETE — PDFs: %d uploaded, %d skipped, %d failed | "
        "Text: %d uploaded, %d skipped, %d failed",
        results["uploaded"],
        results["skipped"],
        results["failed"],
        results["text_uploaded"],
        results["text_skipped"],
        results["text_failed"],
    )
    logger.info("=" * 70)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════


def main():
    setup_logging()

    logger.info("╔══════════════════════════════════════════════════════════════╗")
    logger.info("║          JD PIPELINE — RUN STARTED                          ║")
    logger.info("╚══════════════════════════════════════════════════════════════╝")

    # Validate config
    missing = []
    for var in ("TENANT_ID", "CLIENT_ID", "CLIENT_SECRET"):
        if not getattr(Config, var):
            missing.append(var)
    if missing:
        logger.critical(
            "Missing required config: %s. Set them in .env or environment.", missing
        )
        sys.exit(1)

    # Authenticate
    try:
        auth = GraphAuthProvider()
        _ = auth.get_access_token()
        logger.info("✅ Microsoft Graph authentication successful.")
    except Exception as e:
        logger.critical("❌ Authentication failed: %s", e)
        sys.exit(1)

    headers = auth.get_headers()
    sp = SharePointManager(auth_headers=headers)

    # Run pipeline
    results = {}
    try:
        results = run_pipeline(sp)
    except Exception as e:
        logger.error("Pipeline failed with error: %s", e, exc_info=True)

    # Send Teams notification
    send_jd_summary(results)

    # Cleanup
    try:
        if os.path.isdir(Config.TEMP_DIR):
            shutil.rmtree(Config.TEMP_DIR)
            logger.info("Cleaned up temp directory: %s", Config.TEMP_DIR)
    except OSError as e:
        logger.warning("Could not remove temp directory: %s", e)

    logger.info("╔══════════════════════════════════════════════════════════════╗")
    logger.info("║          JD PIPELINE — ALL DONE                             ║")
    logger.info("╚══════════════════════════════════════════════════════════════╝")


if __name__ == "__main__":
    main()
