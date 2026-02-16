import os
import sys
import re
import logging
import time
from datetime import datetime
from dataclasses import dataclass, field
from urllib.parse import quote, urljoin

import msal
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
)

load_dotenv()


# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════


class config:
    """
    Configuration for the Job Description Pipeline.
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

    # ─── SharePoint Target ───────────────────────────────────────────────────
    SHAREPOINT_SITE_DOMAIN = os.getenv(
        "SHAREPOINT_SITE_DOMAIN", "yourcompany.sharepoint.com"
    )
    SHAREPOINT_SITE_PATH = os.getenv("SHAREPOINT_SITE_PATH", "/sites/Recruitment")
    SHAREPOINT_DRIVE_NAME = os.getenv("SHAREPOINT_DRIVE_NAME", "Documents")
    SHAREPOINT_JD_FOLDER = os.getenv("SHAREPOINT_JD_FOLDER", "JobDescriptions")

    # ─── Website to Scrape ───────────────────────────────────────────────────
    JOBS_ARCHIVE_URL = os.getenv("JOBS_ARCHIVE_URL", "https://si2tech.com/jobs/")
    SITE_BASE_URL = os.getenv("SITE_BASE_URL", "https://si2tech.com")

    # ─── Scraping ────────────────────────────────────────────────────────────
    REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.5"))  # polite delay (seconds)
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
    MAX_PAGES = int(os.getenv("MAX_PAGES", "20"))  # safety cap on pagination

    # ─── Logging ─────────────────────────────────────────────────────────────
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("JD_LOG_FILE", "logs/jd_pipeline.log")

    # ─── Local Temp Directory ────────────────────────────────────────────────
    TEMP_DIR = os.getenv("JD_TEMP_DIR", "./tmp_job_descriptions")

    # ─── Notifications ───────────────────────────────────────────────────────
    TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL", "")


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTH  (same as app.py)
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

    def get_access_token(self) -> str:
        result = self._app.acquire_token_silent(config.SCOPES, account=None)
        if not result:
            logger_auth.info("Acquiring new token via client credentials.")
            result = self._app.acquire_token_for_client(scopes=config.SCOPES)

        if "access_token" in result:
            return result["access_token"]

        error = result.get("error_description", result.get("error", "Unknown error"))
        logger_auth.error("Token acquisition failed: %s", error)
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

    slug: str = ""  # URL slug used as unique ID
    title: str = ""
    url: str = ""

    # ── Metadata fields ──
    location: str = ""
    job_type: str = ""
    department: str = ""
    shifts: str = ""
    experience: str = ""
    job_category: str = ""
    positions: str = ""
    designation: str = ""
    work_hours: str = ""
    compensation: str = ""
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
#  WEBSITE SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

logger_scraper = logging.getLogger("scraper")

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
    time.sleep(config.REQUEST_DELAY)
    resp = _session.get(url, timeout=config.REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp


# ─── Step 1: Discover all job URLs from /jobs/ archive pages ──────────────────


def discover_job_urls() -> list[dict]:
    """
    Crawl the WordPress job archive at /jobs/, /jobs/page/2/, etc.

    The archive pages list jobs as <h2><a href="...">Title</a></h2>.
    Pagination is handled by following "Next" links.

    Returns a deduplicated list of {"url": str, "title": str}.
    """
    jobs: dict[str, str] = {}  # url -> title
    current_url = config.JOBS_ARCHIVE_URL
    page_num = 0

    while current_url and page_num < config.MAX_PAGES:
        page_num += 1
        logger_scraper.info("Crawling archive page %d: %s", page_num, current_url)

        try:
            resp = _polite_get(current_url)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                logger_scraper.info("Page %d returned 404 — end of archive.", page_num)
                break
            raise

        soup = BeautifulSoup(resp.text, "html.parser")

        # ── Extract job links from <h2><a> inside archive listings ──
        found_on_page = 0
        for h2 in soup.find_all("h2"):
            a_tag = h2.find("a", href=True)
            if not a_tag:
                continue
            href = a_tag["href"]
            if "/jobs/" not in href:
                continue
            full_url = urljoin(config.SITE_BASE_URL, href).rstrip("/") + "/"
            # Skip archive index pages themselves
            slug = full_url.rstrip("/").split("/jobs/")[-1].split("/")[0]
            if not slug or slug == "page":
                continue

            title = a_tag.get_text(strip=True)
            if full_url not in jobs:
                jobs[full_url] = title
                found_on_page += 1

        logger_scraper.info("  Found %d jobs on page %d.", found_on_page, page_num)

        # ── Follow "Next" pagination link ──
        next_link = None
        for a_tag in soup.find_all("a", href=True):
            text = a_tag.get_text(strip=True)
            href = a_tag["href"]
            if ("next" in text.lower() or "\u2192" in text) and "/jobs/page/" in href:
                next_link = urljoin(config.SITE_BASE_URL, href)
                break

        if next_link and next_link != current_url:
            current_url = next_link
        else:
            break

    result = [{"url": url, "title": title} for url, title in jobs.items()]
    logger_scraper.info("Total unique jobs discovered: %d", len(result))
    return result


# ─── Step 2: Parse one job detail page ────────────────────────────────────────

# Labels in <strong>Label:</strong> that map to metadata attributes.
META_LABEL_MAP = {
    "location": "location",
    "job type": "job_type",
    "internship type": "job_type",
    "department": "department",
    "shift": "shifts",
    "experience": "experience",
    "job category": "job_category",
    "position": "positions",
    "designation": "designation",
    "compensation": "compensation",
    "work hrs": "work_hours",
    "work hours": "work_hours",
    "employment type": "employment_type",
    "job title": "_skip",  # already captured from <h1>
    "internship title": "_skip",
    "job location": "location",
}

# Known section heading keywords.
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

    # ── Title from <h1> ──
    h1 = soup.find("h1")
    jd.title = h1.get_text(strip=True) if h1 else fallback_title

    content_div = soup.find("div", class_="entry-content")
    if not content_div:
        content_div = soup.find("article")

    if not content_div:
        logger_scraper.warning(
            "No content container (entry-content/article) found for %s", url
        )
        return jd

    # ── Walk elements sequentially ──
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

        # Mark children as processed to avoid duplication if we hit a wrapper div
        for child in elem.find_all(["p", "ul", "ol", "h2", "h3", "h4", "h5"]):
            processed_elements.add(child)

        text_content = elem.get_text(strip=True)
        if not text_content:
            continue

        tag_name = elem.name.lower()

        # ── Headers: Start new section ──
        if tag_name in ("h2", "h3", "h4", "h5") or (
            tag_name == "p" and _is_section_heading(text_content)
        ):
            # Check for "Apply" text to stop parsing
            if "apply for this position" in text_content.lower():
                break

            _flush()
            current_heading = text_content.rstrip(":")
            continue

        # ── <p> tags ──
        if tag_name == "p":
            is_metadata = False
            # Regex to find "Label: Value" patterns
            match = re.match(r"^([A-Za-z\s]+)\s*[:]\s*(.*)", text_content)
            if match:
                label, value = match.groups()
                if _try_set_metadata(jd, label, value):
                    is_metadata = True

            if is_metadata:
                continue

            # If not metadata, treat as body text
            current_paragraphs.append(
                elem.decode_contents()
            )  # Keep inline formatting like <b>

        # ── <ul> / <ol> bullet lists ──
        elif tag_name in ("ul", "ol"):
            for li in elem.find_all("li", recursive=False):
                # Handle nested lists or simple text
                li_text = li.get_text(" ", strip=True)
                if li_text:
                    current_bullets.append(li_text)
    _flush()

    # ── Extract footer metadata if not already set ──
    _extract_footer_metadata(soup, jd)

    return jd


def _try_set_metadata(jd: JobDescription, label: str, value: str) -> bool:
    """Try to assign value to a JD metadata field. Returns True if matched."""
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
    """Check if bold text looks like a content section header."""
    if not text or len(text) < 3 or len(text) > 80:
        return False
    text_lower = text.lower()
    # Skip form / nav labels
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
    # Check against known section keywords
    for kw in SECTION_KEYWORDS:
        if kw in text_lower:
            return True
    # Accept bold text > 15 chars without a colon as a likely section heading
    if ":" not in text and len(text) < 40 and text[0].isupper():
        return True
    return False


def _extract_footer_metadata(soup: BeautifulSoup, jd: JobDescription) -> None:
    """
    Extract Job Category / Job Type / Job Location from the bottom of the page.
    These appear as lines like "Job Category: SOC Lead" after the main content.
    """
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
#  PDF GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

logger_pdf = logging.getLogger("pdf_generator")

# ─── Brand Colors ─────────────────────────────────────────────────────────────
C_PRIMARY = HexColor("#0F3A68")  # Dark navy
C_ACCENT = HexColor("#1976D2")  # Bright blue
C_BG_LIGHT = HexColor("#EDF4FC")  # Very light blue
C_TEXT = HexColor("#222222")  # Near-black body text
C_SUBTLE = HexColor("#555555")  # Grey metadata / footer
C_DIVIDER = HexColor("#B0C4DE")  # Soft blue-grey divider


def _build_styles() -> dict:
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
    styles = _build_styles()
    story: list = []
    W = A4[0] - 1.5 * inch  # usable width

    # ── Header bar ──
    story.append(Paragraph("Si2 Technologies", styles["CompanyName"]))
    story.append(Spacer(1, 2))
    story.append(
        HRFlowable(width="100%", thickness=2.5, color=C_PRIMARY, spaceAfter=10)
    )

    # ── Job Title ──
    story.append(Paragraph(_safe(jd.title) or "Job Description", styles["Title"]))
    story.append(Spacer(1, 4))

    # ── Metadata grid ──
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

    # ── Content sections ──
    for section in jd.sections:
        heading = section["heading"]
        paragraphs = section.get("paragraphs", [])
        bullets = section.get("bullets", [])

        if not paragraphs and not bullets:
            continue

        # Section heading with accent bar
        story.append(Spacer(1, 4))
        accent_hex = C_ACCENT.hexval()[2:]
        heading_para = Paragraph(
            f'<font color="#{accent_hex}">|</font>&nbsp;&nbsp;{_safe(heading)}',
            styles["SectionHeading"],
        )
        story.append(heading_para)

        for p in paragraphs:
            if "<b>" in p:
                # Already contains safe markup from parsing
                story.append(Paragraph(p, styles["BodyBold"]))
            else:
                story.append(Paragraph(_safe(p), styles["Body"]))

        for b in bullets:
            if b.startswith("    "):
                story.append(
                    Paragraph(
                        f"\u2013  {_safe(b.strip())}",
                        styles["SubBullet"],
                    )
                )
            else:
                story.append(
                    Paragraph(
                        f"\u2022  {_safe(b)}",
                        styles["Bullet"],
                    )
                )

    # ── Footer ──
    story.append(Spacer(1, 20))
    story.append(
        HRFlowable(
            width="100%",
            thickness=0.5,
            color=C_DIVIDER,
            spaceBefore=8,
            spaceAfter=6,
        )
    )
    story.append(
        Paragraph(
            f"Source: {_safe(jd.url)}  |  Scraped: {jd.scraped_date}  "
            f"|  Si2 Technologies  |  Confidential",
            styles["Footer"],
        )
    )

    # ── Build PDF ──
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
    logger_pdf.info("Generated PDF: %s (%d sections)", output_path, len(jd.sections))
    return output_path


def _build_meta_pairs(jd: JobDescription) -> list[tuple[str, str]]:
    """Build ordered (label, value) pairs for the metadata grid."""
    pairs = []
    for label, val in [
        ("Location", jd.location),
        ("Job Type", jd.job_type),
        ("Department", jd.department),
        ("Experience", jd.experience),
        ("Shifts", jd.shifts),
        ("Designation", jd.designation),
        ("Positions", jd.positions),
        ("Work Hours", jd.work_hours),
        ("Compensation", jd.compensation),
        ("Employment Type", jd.employment_type),
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
#  SHAREPOINT UPLOADER  (reused from app.py with JD-specific logic)
# ═══════════════════════════════════════════════════════════════════════════════

logger_sp = logging.getLogger("sharepoint_uploader")


class SharePointUploader:
    """Uploads files to SharePoint and checks for existing files."""

    def __init__(self, auth_headers: dict):
        self.headers = auth_headers
        self.base = config.GRAPH_BASE_URL
        self._site_id: str | None = None
        self._drive_id: str | None = None
        self._ensured_folders: set[str] = set()
        self._existing_files: set[str] | None = None

    # ── Public ────────────────────────────────────────────────────────────────

    def file_exists_on_sharepoint(self, filename: str) -> bool:
        """Check if a file already exists in the JobDescriptions folder (cached)."""
        if self._existing_files is None:
            self._existing_files = self._list_existing_files()
        return filename.lower() in self._existing_files

    def upload_jd(self, file_path: str, target_filename: str) -> dict:
        """Upload a JD PDF to the JobDescriptions/ folder."""
        drive_id = self._get_drive_id()
        folder = config.SHAREPOINT_JD_FOLDER.strip("/")
        self._ensure_folder(drive_id, folder)

        file_size = os.path.getsize(file_path)
        if file_size < 4 * 1024 * 1024:
            item = self._simple_upload(drive_id, folder, target_filename, file_path)
        else:
            item = self._resumable_upload(
                drive_id, folder, target_filename, file_path, file_size
            )

        logger_sp.info(
            "Uploaded '%s' -> SharePoint:/%s/%s (id: %s)",
            target_filename,
            folder,
            target_filename,
            item.get("id"),
        )
        if self._existing_files is not None:
            self._existing_files.add(target_filename.lower())
        return item

    # ── List existing files ───────────────────────────────────────────────────

    def _list_existing_files(self) -> set[str]:
        try:
            drive_id = self._get_drive_id()
            folder = config.SHAREPOINT_JD_FOLDER.strip("/")
            encoded = quote(folder)
            url = (
                f"{self.base}/drives/{drive_id}/root:/{encoded}:/children"
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
            logger_sp.info(
                "Found %d existing files in SharePoint:/%s/",
                len(filenames),
                folder,
            )
            return filenames
        except Exception as e:
            logger_sp.warning("Could not list existing files: %s", e)
            return set()

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
                "Drive '%s' not found — using default '%s'.",
                target,
                drives[0]["name"],
            )
            return self._drive_id
        raise RuntimeError(f"No drives found on site {site_id}")

    # ── Folder Management ─────────────────────────────────────────────────────

    def _ensure_folder(self, drive_id: str, folder_path: str) -> None:
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
                if "/" in current:
                    parent_encoded = quote("/".join(current.split("/")[:-1]))
                    create_url = (
                        f"{self.base}/drives/{drive_id}/root:/"
                        f"{parent_encoded}:/children"
                    )
                else:
                    create_url = f"{self.base}/drives/{drive_id}/root/children"
                body = {
                    "name": part,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "fail",
                }
                cr = requests.post(
                    create_url,
                    headers=self.headers,
                    json=body,
                    timeout=15,
                )
                if cr.status_code in (201, 409):
                    logger_sp.info("Created folder: %s", current)
                else:
                    logger_sp.error(
                        "Could not create folder '%s': %s %s",
                        current,
                        cr.status_code,
                        cr.text,
                    )
            self._ensured_folders.add(current)

    # ── Upload Methods ────────────────────────────────────────────────────────

    def _simple_upload(
        self,
        drive_id: str,
        folder: str,
        filename: str,
        file_path: str,
    ) -> dict:
        encoded_path = quote(f"{folder}/{filename}")
        url = f"{self.base}/drives/{drive_id}/root:/{encoded_path}:/content"
        with open(file_path, "rb") as f:
            headers = {**self.headers, "Content-Type": "application/pdf"}
            resp = requests.put(url, headers=headers, data=f, timeout=120)
        resp.raise_for_status()
        return resp.json()

    def _resumable_upload(
        self,
        drive_id: str,
        folder: str,
        filename: str,
        file_path: str,
        file_size: int,
    ) -> dict:
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
                    upload_url,
                    headers=chunk_headers,
                    data=chunk,
                    timeout=120,
                )
                resp.raise_for_status()
                offset += len(chunk)
        return resp.json()


# ═══════════════════════════════════════════════════════════════════════════════
#  NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════════

logger_notif = logging.getLogger("notifications")


def send_jd_summary(results: dict) -> None:
    """Send a summary of the JD pipeline run to Teams."""
    if not config.TEAMS_WEBHOOK_URL:
        return

    if results.get("uploaded", 0) == 0:
        logger_notif.info("No new JDs uploaded. Skipping Teams notification.")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    total = results["uploaded"] + results["skipped"] + results["failed"]

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
                            "text": f"Job Description Pipeline - {now}",
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Total Jobs Found", "value": str(total)},
                                {
                                    "title": "New PDFs Uploaded",
                                    "value": str(results["uploaded"]),
                                },
                                {
                                    "title": "Already Existed (Skipped)",
                                    "value": str(results["skipped"]),
                                },
                                {"title": "Failed", "value": str(results["failed"])},
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
            logger_notif.info("Teams JD notification sent.")
        else:
            logger_notif.warning(
                "Teams webhook returned %s: %s",
                resp.status_code,
                resp.text,
            )
    except Exception as e:
        logger_notif.error("Failed to send Teams notification: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

logger = logging.getLogger("jd_pipeline")


def setup_logging():
    fmt = "%(asctime)s | %(levelname)-7s | %(name)-22s | %(message)s"
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


def run_pipeline():
    setup_logging()
    logger.info("=" * 70)
    logger.info(
        "JOB DESCRIPTION PIPELINE — RUN STARTED at %s",
        datetime.now().isoformat(),
    )
    logger.info("=" * 70)

    # ── Step 1: Validate config ──
    missing = []
    for var in ("TENANT_ID", "CLIENT_ID", "CLIENT_SECRET"):
        if not getattr(config, var):
            missing.append(var)
    if missing:
        logger.critical(
            "Missing required config: %s. Set them in .env or environment.",
            missing,
        )
        sys.exit(1)

    # ── Step 2: Authenticate ──
    auth = GraphAuthProvider()
    headers = auth.get_headers()
    logger.info("Authentication successful.")

    # ── Step 3: Discover all job listing URLs ──
    logger.info("Discovering jobs from %s ...", config.JOBS_ARCHIVE_URL)
    job_urls = discover_job_urls()
    logger.info("Found %d job listings total.", len(job_urls))

    if not job_urls:
        logger.info("No job listings found. Exiting.")
        return

    # ── Step 4: Set up SharePoint uploader ──
    uploader = SharePointUploader(auth_headers=headers)
    os.makedirs(config.TEMP_DIR, exist_ok=True)

    results = {"uploaded": 0, "skipped": 0, "failed": 0}

    # ── Step 5: Process each job ──
    for idx, job_info in enumerate(job_urls, 1):
        url = job_info["url"]
        title = job_info.get("title", "")
        slug = url.rstrip("/").split("/")[-1]
        safe_slug = re.sub(r"[^\w\-]", "", slug)
        pdf_filename = f"JD_{safe_slug}.pdf"

        logger.info("-" * 60)
        logger.info(
            "[%d/%d] %s (%s)",
            idx,
            len(job_urls),
            title or slug,
            pdf_filename,
        )

        # ── Check if already on SharePoint ──
        if uploader.file_exists_on_sharepoint(pdf_filename):
            logger.info("  SKIP — already exists on SharePoint.")
            results["skipped"] += 1
            continue

        # ── Parse job detail page ──
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
            continue

        # ── Generate PDF ──
        local_path = os.path.join(config.TEMP_DIR, pdf_filename)
        try:
            generate_job_pdf(jd, local_path)
        except Exception as e:
            logger.error("  FAIL — PDF generation: %s", e, exc_info=True)
            results["failed"] += 1
            continue

        # ── Upload to SharePoint ──
        try:
            uploader.upload_jd(file_path=local_path, target_filename=pdf_filename)
            results["uploaded"] += 1
            logger.info(
                "  OK — uploaded to SharePoint:/%s/%s",
                config.SHAREPOINT_JD_FOLDER,
                pdf_filename,
            )
        except Exception as e:
            results["failed"] += 1
            logger.error("  FAIL — upload: %s", e, exc_info=True)

        # ── Clean up ──
        try:
            os.remove(local_path)
        except OSError:
            pass

    # ── Step 6: Send Teams notification ──
    send_jd_summary(results)

    # ── Summary ──
    logger.info("=" * 70)
    logger.info(
        "PIPELINE COMPLETE — Uploaded: %d | Skipped: %d | Failed: %d",
        results["uploaded"],
        results["skipped"],
        results["failed"],
    )
    logger.info("=" * 70)


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_pipeline()
