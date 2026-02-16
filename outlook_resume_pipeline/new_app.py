"""
Job Description Pipeline — Single-File Application (new_app.py)

Workflow:
  1. Authenticate with Entra ID (client credentials)
  2. Scrape all job listings from https://si2tech.com/careers/
  3. Visit each job detail page → extract structured fields
     (Title, Location, Department, Experience, Job Type, Shifts,
      Summary, Responsibilities, Skills, Qualifications, etc.)
  4. Generate a professional PDF for each job description
  5. Upload to SharePoint under JobDescriptions/ folder
  6. Skip any job whose PDF already exists on SharePoint
  7. Log everything to jd_pipeline.log

Run:     python new_app.py
Schedule: cron / Task Scheduler / Azure Function Timer Trigger
"""

# ═══════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════

import os
import sys
import re
import json
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
from reportlab.lib.units import inch, mm
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
    KeepTogether,
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
    CAREERS_URL = os.getenv("CAREERS_URL", "https://si2tech.com/careers/")
    SITE_BASE_URL = os.getenv("SITE_BASE_URL", "https://si2tech.com")

    # ─── WordPress REST API Endpoints (tried in order) ───────────────────────
    WP_REST_ENDPOINTS = [
        "/wp-json/wp/v2/job-listings",
        "/wp-json/wp/v2/jobs",
        "/wp-json/wp/v2/job_listing",
    ]

    # ─── Scraping ────────────────────────────────────────────────────────────
    REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.5"))  # polite delay (seconds)
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

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

    slug: str = ""  # URL slug (unique identifier)
    title: str = ""
    url: str = ""
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
    job_summary: str = ""
    responsibilities: list[str] = field(default_factory=list)
    must_have_skills: list[str] = field(default_factory=list)
    preferred_qualifications: list[str] = field(default_factory=list)
    certifications: list[str] = field(default_factory=list)
    education: str = ""
    extra_sections: dict = field(default_factory=dict)  # any other sections found
    scraped_date: str = ""

    @property
    def safe_slug(self) -> str:
        """Filesystem-safe version of the slug."""
        return re.sub(r"[^\w\-]", "", self.slug).strip() or "unknown"

    @property
    def pdf_filename(self) -> str:
        """Generate PDF filename: JD_{slug}.pdf"""
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
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
)


def _polite_get(url: str, **kwargs) -> requests.Response:
    """GET with a polite delay between requests."""
    time.sleep(config.REQUEST_DELAY)
    resp = _session.get(url, timeout=config.REQUEST_TIMEOUT, **kwargs)
    resp.raise_for_status()
    return resp


# ── Step 1: Discover all job listing URLs ─────────────────────────────────────


def discover_job_urls() -> list[dict]:
    """
    Find all job detail page URLs from the careers page.

    Strategy (tried in order):
      1. WordPress REST API (returns all posts of the job CPT)
      2. HTML parsing of the careers page + "Load more" AJAX
      3. Fallback: parse all <a> tags linking to /jobs/

    Returns list of {"url": ..., "title": ...}
    """
    # ── Strategy 1: WP REST API ──
    jobs = _try_wp_rest_api()
    if jobs:
        logger_scraper.info("Discovered %d jobs via WP REST API.", len(jobs))
        return jobs

    # ── Strategy 2: HTML parsing + AJAX load-more ──
    logger_scraper.info("WP REST API unavailable. Parsing HTML careers page.")
    jobs = _parse_careers_page_html()
    logger_scraper.info("Discovered %d jobs from HTML parsing.", len(jobs))
    return jobs


def _try_wp_rest_api() -> list[dict]:
    """Try known WP REST API endpoints for the jobs custom post type."""
    for endpoint in config.WP_REST_ENDPOINTS:
        url = f"{config.SITE_BASE_URL}{endpoint}?per_page=100&status=publish"
        try:
            resp = _session.get(url, timeout=config.REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    jobs = []
                    for item in data:
                        link = item.get("link", "")
                        title = ""
                        title_obj = item.get("title", {})
                        if isinstance(title_obj, dict):
                            title = title_obj.get("rendered", "")
                        elif isinstance(title_obj, str):
                            title = title_obj
                        # Clean HTML entities from title
                        title = BeautifulSoup(title, "html.parser").get_text()
                        if link and "/jobs/" in link:
                            jobs.append({"url": link, "title": title})

                    # Handle pagination (WP REST uses X-WP-TotalPages)
                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                    for page_num in range(2, total_pages + 1):
                        page_url = f"{url}&page={page_num}"
                        try:
                            pr = _session.get(page_url, timeout=config.REQUEST_TIMEOUT)
                            if pr.status_code == 200:
                                for item in pr.json():
                                    link = item.get("link", "")
                                    title_obj = item.get("title", {})
                                    title = (
                                        title_obj.get("rendered", "")
                                        if isinstance(title_obj, dict)
                                        else str(title_obj)
                                    )
                                    title = BeautifulSoup(
                                        title, "html.parser"
                                    ).get_text()
                                    if link and "/jobs/" in link:
                                        jobs.append({"url": link, "title": title})
                        except Exception:
                            break

                    if jobs:
                        return jobs
        except Exception as e:
            logger_scraper.debug("REST endpoint %s failed: %s", endpoint, e)
            continue
    return []


def _parse_careers_page_html() -> list[dict]:
    """Parse the careers page HTML to find all job links."""
    resp = _polite_get(config.CAREERS_URL)
    soup = BeautifulSoup(resp.text, "html.parser")

    jobs = {}  # url → title (dedup by URL)

    # Find all links pointing to /jobs/*
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/jobs/" not in href:
            continue
        # Normalize to absolute URL
        full_url = urljoin(config.SITE_BASE_URL, href)
        # Skip if it's just the /jobs/ index
        slug = full_url.rstrip("/").split("/jobs/")[-1]
        if not slug or slug == "jobs":
            continue

        title = a_tag.get_text(strip=True) or ""
        # Prefer longer/more descriptive title if we see the same URL twice
        if full_url not in jobs or len(title) > len(jobs[full_url]):
            jobs[full_url] = title

    # ── Also try WP AJAX "load more" mechanism ──
    # WP Job Manager typically uses admin-ajax.php with action=job_manager_get_listings
    ajax_jobs = _try_ajax_load_more(soup)
    for url, title in ajax_jobs.items():
        if url not in jobs or len(title) > len(jobs[url]):
            jobs[url] = title

    return [{"url": url, "title": title} for url, title in jobs.items()]


def _try_ajax_load_more(soup: BeautifulSoup) -> dict:
    """
    Attempt to load additional jobs via WP Job Manager's AJAX endpoint.
    The careers page uses a "Load more..." link which fetches more job cards.
    """
    extra_jobs = {}
    ajax_url = f"{config.SITE_BASE_URL}/wp-admin/admin-ajax.php"

    # Try pages 2..10 (most sites won't have more than a few pages)
    for page in range(2, 11):
        try:
            data = {
                "action": "job_manager_get_listings",
                "page": page,
                "per_page": 10,
                "show_pagination": "false",
            }
            resp = _session.post(ajax_url, data=data, timeout=config.REQUEST_TIMEOUT)
            if resp.status_code != 200:
                break

            result = resp.json()
            html = result.get("html", "")
            if not html or not html.strip():
                break

            page_soup = BeautifulSoup(html, "html.parser")
            found_any = False
            for a_tag in page_soup.find_all("a", href=True):
                href = a_tag["href"]
                if "/jobs/" not in href:
                    continue
                full_url = urljoin(config.SITE_BASE_URL, href)
                slug = full_url.rstrip("/").split("/jobs/")[-1]
                if not slug:
                    continue
                title = a_tag.get_text(strip=True) or ""
                extra_jobs[full_url] = title
                found_any = True

            if not found_any:
                break

            # Check if there are more pages
            if not result.get("found_jobs", True):
                break

            time.sleep(config.REQUEST_DELAY)

        except Exception as e:
            logger_scraper.debug("AJAX load-more page %d failed: %s", page, e)
            break

    return extra_jobs


# ── Step 2: Parse individual job detail pages ─────────────────────────────────


def parse_job_detail(url: str, fallback_title: str = "") -> JobDescription:
    """
    Fetch and parse a single job detail page into a JobDescription.

    The si2tech.com job pages follow this general structure:
      <h1> or <h2>: Job Title
      Bold labels like **Location:**, **Job Type:**, **Experience:**, etc.
      Sections: Job Summary, Key Responsibilities, Must Have Skills, etc.
      Bullet lists under each section.
    """
    resp = _polite_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract slug from URL
    slug = url.rstrip("/").split("/")[-1]

    jd = JobDescription(
        slug=slug,
        url=url,
        scraped_date=datetime.now().strftime("%Y-%m-%d"),
    )

    # ── Find the main content area ──
    # Try common WordPress content containers
    content = (
        soup.find("article")
        or soup.find(
            "div",
            class_=re.compile(
                r"entry-content|job-description|single-job|job_listing", re.I
            ),
        )
        or soup.find("div", class_=re.compile(r"elementor-widget-container", re.I))
        or soup.find("main")
    )
    if not content:
        content = soup  # fallback to entire page

    # ── Title ──
    h1 = content.find("h1")
    jd.title = h1.get_text(strip=True) if h1 else fallback_title

    # ── Extract all text content (preserving structure) ──
    body_text = content.get_text(separator="\n")
    lines = [line.strip() for line in body_text.splitlines() if line.strip()]

    # ── Parse key-value metadata fields ──
    kv_patterns = {
        "location": re.compile(r"^(?:\*\*)?Location\s*:?\s*(?:\*\*)?\s*(.+)", re.I),
        "job_type": re.compile(r"^(?:\*\*)?Job\s*Type\s*:?\s*(?:\*\*)?\s*(.+)", re.I),
        "department": re.compile(r"^(?:\*\*)?Department\s*:?\s*(?:\*\*)?\s*(.+)", re.I),
        "shifts": re.compile(r"^(?:\*\*)?Shift[s]?\s*:?\s*(?:\*\*)?\s*(.+)", re.I),
        "experience": re.compile(r"^(?:\*\*)?Experience\s*:?\s*(?:\*\*)?\s*(.+)", re.I),
        "job_category": re.compile(
            r"^(?:\*\*)?Job\s*Category\s*:?\s*(?:\*\*)?\s*(.+)", re.I
        ),
        "positions": re.compile(
            r"^(?:\*\*)?Positions?\s*:?\s*(?:\*\*)?\s*(\d+.*)$", re.I
        ),
        "designation": re.compile(
            r"^(?:\*\*)?Designation\s*:?\s*(?:\*\*)?\s*(.+)", re.I
        ),
        "compensation": re.compile(
            r"^(?:\*\*)?Compensation\s*:?\s*(?:\*\*)?\s*(.+)", re.I
        ),
        "work_hours": re.compile(
            r"^(?:\*\*)?Work\s*(?:hrs|hours)\s*:?\s*(?:\*\*)?\s*(.+)", re.I
        ),
        "education": re.compile(
            r"^(?:\*\*)?(?:Education|Qualification)s?\s*:?\s*(?:\*\*)?\s*(.+)", re.I
        ),
    }

    for line in lines:
        for field_name, pattern in kv_patterns.items():
            m = pattern.match(line)
            if m:
                value = m.group(1).strip().strip("*").strip()
                if value and not getattr(jd, field_name):
                    setattr(jd, field_name, value)

    # ── Also parse from <strong>/<b> tag + following text in the HTML ──
    for strong_tag in content.find_all(["strong", "b"]):
        label = strong_tag.get_text(strip=True).rstrip(":").strip()
        # Get the text immediately after the strong tag
        next_text = ""
        for sibling in strong_tag.next_siblings:
            if hasattr(sibling, "name") and sibling.name in (
                "strong",
                "b",
                "h1",
                "h2",
                "h3",
            ):
                break
            t = (
                sibling.get_text(strip=True)
                if hasattr(sibling, "get_text")
                else str(sibling).strip()
            )
            if t:
                next_text = t.lstrip(":").strip()
                break

        if not next_text:
            continue

        label_lower = label.lower()
        if "location" in label_lower and not jd.location:
            jd.location = next_text
        elif "job type" in label_lower and not jd.job_type:
            jd.job_type = next_text
        elif "department" in label_lower and not jd.department:
            jd.department = next_text
        elif "shift" in label_lower and not jd.shifts:
            jd.shifts = next_text
        elif "experience" in label_lower and not jd.experience:
            jd.experience = next_text

    # ── Parse sections (Summary, Responsibilities, Skills, etc.) ──
    _parse_sections(content, jd)

    # ── Fallback: if no structured sections found, grab all bullet points ──
    if not jd.job_summary and not jd.responsibilities and not jd.must_have_skills:
        all_text = content.get_text(separator="\n", strip=True)
        # Remove everything before the title and after "Apply for this position"
        if jd.title and jd.title in all_text:
            all_text = all_text.split(jd.title, 1)[-1]
        if "Apply for this position" in all_text:
            all_text = all_text.split("Apply for this position")[0]
        jd.job_summary = all_text.strip()[:3000]  # cap at 3000 chars

    # ── Also look at the footer metadata (Job Category, Type, Location) ──
    for li in content.find_all("li"):
        text = li.get_text(strip=True)
        if text.startswith("Job Category:") and not jd.job_category:
            jd.job_category = text.replace("Job Category:", "").strip()
        elif text.startswith("Job Type:") and not jd.job_type:
            jd.job_type = text.replace("Job Type:", "").strip()
        elif text.startswith("Job Location:") and not jd.location:
            jd.location = text.replace("Job Location:", "").strip()

    return jd


def _parse_sections(content: BeautifulSoup, jd: JobDescription) -> None:
    """
    Parse the job page content into logical sections.

    Sections are identified by heading tags (h2-h4) or bold/strong tags
    that act as section headers. Content under each heading is collected
    as either paragraph text or bullet points.
    """
    # Identify section headings
    section_headers = content.find_all(["h2", "h3", "h4", "strong", "b"])
    current_section = ""
    current_items: list[str] = []

    def _flush_section():
        nonlocal current_section, current_items
        if not current_section:
            return
        sec = current_section.lower()
        items_text = "\n".join(current_items).strip()

        if (
            "summary" in sec
            or "overview" in sec
            or "description" in sec
            and "job" in sec
        ):
            if not jd.job_summary:
                jd.job_summary = items_text
        elif "responsibilit" in sec or "key duties" in sec:
            jd.responsibilities.extend([i for i in current_items if i.strip()])
        elif (
            "must have" in sec
            or "must-have" in sec
            or "required skill" in sec
            or "technical skill" in sec
        ):
            jd.must_have_skills.extend([i for i in current_items if i.strip()])
        elif (
            "preferred" in sec
            or "nice to have" in sec
            or "good to have" in sec
            or "desired" in sec
        ):
            jd.preferred_qualifications.extend([i for i in current_items if i.strip()])
        elif "certific" in sec:
            jd.certifications.extend([i for i in current_items if i.strip()])
        elif "education" in sec or "qualification" in sec:
            if not jd.education:
                jd.education = items_text
        elif items_text:
            jd.extra_sections[current_section] = items_text

        current_items.clear()

    for tag in section_headers:
        header_text = tag.get_text(strip=True).rstrip(":")
        # Skip very short or navigation-like headers
        if len(header_text) < 3 or header_text.lower() in (
            "apply",
            "submit",
            "home",
            "careers",
        ):
            continue

        # Check if this looks like a section header (not an inline bold field)
        is_header = tag.name in ("h2", "h3", "h4")
        if not is_header:
            # Strong/b tags that are section headers usually stand alone
            parent_text = tag.parent.get_text(strip=True) if tag.parent else ""
            if ":" in parent_text and len(parent_text) < 100:
                # Likely a key:value pair, not a section header
                continue

        _flush_section()
        current_section = header_text

        # Collect content after this header until the next header
        sibling = tag.find_next_sibling()
        while sibling:
            if sibling.name in ("h2", "h3", "h4"):
                break
            if sibling.name == "strong" or sibling.name == "b":
                # Check if this is another section header
                sib_text = sibling.get_text(strip=True)
                if len(sib_text) > 10 and ":" not in sib_text:
                    break

            if sibling.name in ("ul", "ol"):
                for li in sibling.find_all("li"):
                    item_text = li.get_text(strip=True)
                    if item_text:
                        current_items.append(item_text)
            elif sibling.name == "p":
                p_text = sibling.get_text(strip=True)
                if p_text:
                    current_items.append(p_text)
            elif sibling.name == "li":
                li_text = sibling.get_text(strip=True)
                if li_text:
                    current_items.append(li_text)

            sibling = sibling.find_next_sibling()

    _flush_section()


# ═══════════════════════════════════════════════════════════════════════════════
#  PDF GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

logger_pdf = logging.getLogger("pdf_generator")

# ─── Colors ───────────────────────────────────────────────────────────────────
PRIMARY = HexColor("#1a3c6e")  # Dark blue (header, accents)
SECONDARY = HexColor("#2e6da4")  # Medium blue
ACCENT = HexColor("#e8f0fe")  # Light blue background
TEXT_COLOR = HexColor("#333333")  # Body text
SUBTLE = HexColor("#666666")  # Subtle / metadata text
DIVIDER = HexColor("#cccccc")  # Divider lines


def _build_styles() -> dict:
    """Create custom ReportLab paragraph styles for the JD PDF."""
    base = getSampleStyleSheet()
    styles = {}

    styles["Title"] = ParagraphStyle(
        "JDTitle",
        parent=base["Title"],
        fontSize=20,
        leading=26,
        textColor=PRIMARY,
        spaceAfter=6,
        alignment=TA_LEFT,
        fontName="Helvetica-Bold",
    )

    styles["MetaLabel"] = ParagraphStyle(
        "MetaLabel",
        parent=base["Normal"],
        fontSize=9,
        textColor=SUBTLE,
        fontName="Helvetica-Bold",
        spaceAfter=1,
    )

    styles["MetaValue"] = ParagraphStyle(
        "MetaValue",
        parent=base["Normal"],
        fontSize=10,
        textColor=TEXT_COLOR,
        fontName="Helvetica",
        spaceAfter=4,
    )

    styles["SectionHeading"] = ParagraphStyle(
        "SectionHeading",
        parent=base["Heading2"],
        fontSize=13,
        leading=18,
        textColor=PRIMARY,
        spaceBefore=14,
        spaceAfter=6,
        fontName="Helvetica-Bold",
        borderWidth=0,
        borderPadding=0,
    )

    styles["Body"] = ParagraphStyle(
        "Body",
        parent=base["Normal"],
        fontSize=10,
        leading=15,
        textColor=TEXT_COLOR,
        alignment=TA_JUSTIFY,
        spaceAfter=6,
        fontName="Helvetica",
    )

    styles["Bullet"] = ParagraphStyle(
        "Bullet",
        parent=base["Normal"],
        fontSize=10,
        leading=14,
        textColor=TEXT_COLOR,
        leftIndent=20,
        bulletIndent=8,
        spaceAfter=3,
        fontName="Helvetica",
    )

    styles["Footer"] = ParagraphStyle(
        "Footer",
        parent=base["Normal"],
        fontSize=7,
        textColor=SUBTLE,
        alignment=TA_CENTER,
    )

    return styles


def generate_job_pdf(jd: JobDescription, output_path: str) -> str:
    """
    Generate a professional PDF for a single job description.

    Returns the output file path.
    """
    styles = _build_styles()
    story = []

    # ── Company header ──
    story.append(
        Paragraph(
            "Si2 Technologies",
            ParagraphStyle(
                "CompanyName",
                fontSize=11,
                textColor=SECONDARY,
                fontName="Helvetica-Bold",
                spaceAfter=2,
            ),
        )
    )
    story.append(HRFlowable(width="100%", thickness=2, color=PRIMARY, spaceAfter=12))

    # ── Job Title ──
    story.append(Paragraph(jd.title or "Job Description", styles["Title"]))

    # ── Metadata table (location, type, dept, exp, shifts) ──
    meta_fields = []
    if jd.location:
        meta_fields.append(("Location", jd.location))
    if jd.job_type:
        meta_fields.append(("Job Type", jd.job_type))
    if jd.department:
        meta_fields.append(("Department", jd.department))
    if jd.experience:
        meta_fields.append(("Experience", jd.experience))
    if jd.shifts:
        meta_fields.append(("Shifts", jd.shifts))
    if jd.designation:
        meta_fields.append(("Designation", jd.designation))
    if jd.positions:
        meta_fields.append(("Positions", jd.positions))
    if jd.work_hours:
        meta_fields.append(("Work Hours", jd.work_hours))
    if jd.compensation:
        meta_fields.append(("Compensation", jd.compensation))

    if meta_fields:
        # Arrange in 2-column layout
        rows = []
        for i in range(0, len(meta_fields), 2):
            row = []
            for j in range(2):
                idx = i + j
                if idx < len(meta_fields):
                    label, value = meta_fields[idx]
                    cell_content = (
                        f'<font name="Helvetica-Bold" size="8" color="#{SUBTLE.hexval()[2:]}">'
                        f"{label}</font><br/>"
                        f'<font name="Helvetica" size="10" color="#{TEXT_COLOR.hexval()[2:]}">'
                        f"{_safe(value)}</font>"
                    )
                    row.append(Paragraph(cell_content, styles["MetaValue"]))
                else:
                    row.append("")
            rows.append(row)

        meta_table = Table(rows, colWidths=[3.2 * inch, 3.2 * inch])
        meta_table.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("BACKGROUND", (0, 0), (-1, -1), ACCENT),
                    ("BOX", (0, 0), (-1, -1), 0.5, DIVIDER),
                ]
            )
        )
        story.append(meta_table)
        story.append(Spacer(1, 10))

    # ── Divider ──
    story.append(HRFlowable(width="100%", thickness=0.5, color=DIVIDER, spaceAfter=6))

    # ── Job Summary ──
    if jd.job_summary:
        story.append(Paragraph("Job Summary", styles["SectionHeading"]))
        # Split into paragraphs
        for para in jd.job_summary.split("\n"):
            para = para.strip()
            if para:
                story.append(Paragraph(_safe(para), styles["Body"]))

    # ── Key Responsibilities ──
    if jd.responsibilities:
        story.append(Paragraph("Key Responsibilities", styles["SectionHeading"]))
        for item in jd.responsibilities:
            story.append(Paragraph(f"\u2022  {_safe(item)}", styles["Bullet"]))

    # ── Must Have Skills ──
    if jd.must_have_skills:
        story.append(
            Paragraph("Must-Have Skills &amp; Qualifications", styles["SectionHeading"])
        )
        for item in jd.must_have_skills:
            story.append(Paragraph(f"\u2022  {_safe(item)}", styles["Bullet"]))

    # ── Preferred Qualifications ──
    if jd.preferred_qualifications:
        story.append(Paragraph("Preferred Qualifications", styles["SectionHeading"]))
        for item in jd.preferred_qualifications:
            story.append(Paragraph(f"\u2022  {_safe(item)}", styles["Bullet"]))

    # ── Certifications ──
    if jd.certifications:
        story.append(Paragraph("Certifications", styles["SectionHeading"]))
        for item in jd.certifications:
            story.append(Paragraph(f"\u2022  {_safe(item)}", styles["Bullet"]))

    # ── Education ──
    if jd.education:
        story.append(Paragraph("Education", styles["SectionHeading"]))
        story.append(Paragraph(_safe(jd.education), styles["Body"]))

    # ── Extra Sections ──
    for section_name, section_content in jd.extra_sections.items():
        story.append(Paragraph(_safe(section_name), styles["SectionHeading"]))
        for para in section_content.split("\n"):
            para = para.strip()
            if para:
                story.append(Paragraph(_safe(para), styles["Body"]))

    # ── Footer ──
    story.append(Spacer(1, 20))
    story.append(
        HRFlowable(
            width="100%", thickness=0.5, color=DIVIDER, spaceBefore=10, spaceAfter=6
        )
    )
    story.append(
        Paragraph(
            f"Source: {_safe(jd.url)} &nbsp;|&nbsp; Scraped: {jd.scraped_date} "
            f"&nbsp;|&nbsp; Si2 Technologies",
            styles["Footer"],
        )
    )

    # ── Build ──
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.6 * inch,
        title=jd.title,
        author="Si2 Technologies - JD Pipeline",
    )
    doc.build(story)
    logger_pdf.info("Generated PDF → %s", output_path)
    return output_path


def _safe(text: str) -> str:
    """Escape text for use in ReportLab Paragraph XML."""
    if not text:
        return ""
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    return text


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
        self._existing_files: set[str] | None = None  # cache of filenames already on SP

    # ── Public ────────────────────────────────────────────────────────────────

    def file_exists_on_sharepoint(self, filename: str) -> bool:
        """
        Check if a file already exists in the JobDescriptions folder.
        Uses a cached listing to avoid repeated API calls.
        """
        if self._existing_files is None:
            self._existing_files = self._list_existing_files()
        return filename.lower() in self._existing_files

    def upload_jd(self, file_path: str, target_filename: str) -> dict:
        """
        Upload a job description PDF to the JobDescriptions/ folder.

        Returns the Graph API response for the uploaded DriveItem.
        """
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
            "Uploaded '%s' → SharePoint:/%s/%s (id: %s)",
            target_filename,
            folder,
            target_filename,
            item.get("id"),
        )

        # Update the cache
        if self._existing_files is not None:
            self._existing_files.add(target_filename.lower())

        return item

    # ── List existing files ───────────────────────────────────────────────────

    def _list_existing_files(self) -> set[str]:
        """List all filenames in the JobDescriptions folder on SharePoint."""
        try:
            drive_id = self._get_drive_id()
            folder = config.SHAREPOINT_JD_FOLDER.strip("/")
            encoded = quote(folder)
            url = (
                f"{self.base}/drives/{drive_id}/root:/{encoded}:/children"
                f"?$select=name&$top=1000"
            )
            filenames = set()
            while url:
                resp = requests.get(url, headers=self.headers, timeout=30)
                if resp.status_code == 404:
                    # Folder doesn't exist yet — no files
                    return set()
                resp.raise_for_status()
                data = resp.json()
                for item in data.get("value", []):
                    filenames.add(item["name"].lower())
                url = data.get("@odata.nextLink")

            logger_sp.info(
                "Found %d existing files in SharePoint:/%s/", len(filenames), folder
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
                "Drive '%s' not found — using default '%s'.", target, drives[0]["name"]
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
        self, drive_id: str, folder: str, filename: str, file_path: str
    ) -> dict:
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


# ═══════════════════════════════════════════════════════════════════════════════
#  NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════════

logger_notif = logging.getLogger("notifications")


def send_jd_summary(results: dict) -> None:
    """Send a summary of the JD pipeline run to Teams."""
    if not config.TEAMS_WEBHOOK_URL:
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
                            "text": f"📋 Job Description Pipeline — {now}",
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
                "Teams webhook returned %s: %s", resp.status_code, resp.text
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
        "JOB DESCRIPTION PIPELINE — RUN STARTED at %s", datetime.now().isoformat()
    )
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

    # ── Step 3: Discover all job listing URLs ──
    logger.info("Discovering job listings from %s ...", config.CAREERS_URL)
    job_urls = discover_job_urls()
    logger.info("Found %d job listings.", len(job_urls))

    if not job_urls:
        logger.info("No job listings found. Exiting.")
        return

    # ── Step 4: Set up SharePoint uploader and check existing files ──
    uploader = SharePointUploader(auth_headers=headers)
    os.makedirs(config.TEMP_DIR, exist_ok=True)

    results = {"uploaded": 0, "skipped": 0, "failed": 0}

    # ── Step 5: Process each job ──
    for idx, job_info in enumerate(job_urls, 1):
        url = job_info["url"]
        title = job_info.get("title", "")
        slug = url.rstrip("/").split("/")[-1]

        # pdf_filename = f"JD_{re.sub(r'[^\\w-]', '', slug)}.pdf"

        # FIX: Define the regex replacement outside the f-string
        safe_slug = re.sub(r"[^\w-]", "", slug)
        pdf_filename = f"JD_{safe_slug}.pdf"

        logger.info("─" * 60)
        logger.info("[%d/%d] %s", idx, len(job_urls), title or slug)

        # ── Check if already on SharePoint ──
        if uploader.file_exists_on_sharepoint(pdf_filename):
            logger.info("  ⏭  Already exists on SharePoint. Skipping.")
            results["skipped"] += 1
            continue

        # ── Parse job detail page ──
        try:
            jd = parse_job_detail(url, fallback_title=title)
            logger.info(
                "  Parsed: title=%s | location=%s | dept=%s | exp=%s",
                jd.title,
                jd.location,
                jd.department,
                jd.experience,
            )
        except Exception as e:
            logger.error("  ✗ Failed to parse %s: %s", url, e)
            results["failed"] += 1
            continue

        # ── Generate PDF ──
        local_path = os.path.join(config.TEMP_DIR, pdf_filename)
        try:
            generate_job_pdf(jd, local_path)
        except Exception as e:
            logger.error("  ✗ Failed to generate PDF: %s", e)
            results["failed"] += 1
            continue

        # ── Upload to SharePoint ──
        try:
            uploader.upload_jd(file_path=local_path, target_filename=pdf_filename)
            results["uploaded"] += 1
            logger.info(
                "  ✓ Uploaded → SharePoint:/%s/%s",
                config.SHAREPOINT_JD_FOLDER,
                pdf_filename,
            )
        except Exception as e:
            results["failed"] += 1
            logger.error("  ✗ Upload failed: %s", e)

        # ── Clean up local temp file ──
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
