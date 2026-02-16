"""
Resume Pipeline — Main Orchestrator

Workflow:
  1. Authenticate with Entra ID (client credentials)
  2. Fetch emails from the last 24 hours matching "New application received"
  3. Parse each email → extract Name, Email, Phone, Job Role, Job ID, Resume URL
     (no PDF content is parsed — only the structured fields from the email body)
  4. Download the resume PDF as-is from the URL
  5. Rename: {Name}_{JobID}_{Date}.pdf
  6. Upload to SharePoint under Resumes/{JobID}_{JobRole}/ subfolder
  7. Tag metadata columns: CandidateName, CandidateEmail, CandidatePhone, JobID, JobRole
  8. Send summary notification to Teams
  9. Log everything to pipeline.log

Run:     python main.py
Schedule: cron / Task Scheduler / Azure Function Timer Trigger
"""

import logging
import os
import sys
import json
import hashlib
from datetime import datetime

import requests

import config
from auth import GraphAuthProvider
from email_fetcher import EmailFetcher, CandidateInfo
from sharepoint_uploader import SharePointUploader
from notifications import send_summary

# ─── Logging Setup ────────────────────────────────────────────────────────────


def setup_logging():
    fmt = "%(asctime)s │ %(levelname)-7s │ %(name)-22s │ %(message)s"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOG_FILE, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=fmt,
        handlers=handlers,
    )


logger = logging.getLogger("pipeline")

# ─── Deduplication ────────────────────────────────────────────────────────────

PROCESSED_LOG = "logs/processed_emails.json"


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


# ─── Main Pipeline ────────────────────────────────────────────────────────────


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
        send_summary({"success": 0, "failed": 0, "skipped_no_resume": 0}, [])
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
    send_summary(results, notification_rows)

    # ── Summary ──
    logger.info("=" * 70)
    logger.info(
        "PIPELINE COMPLETE — Uploaded: %d | Failed: %d | No Resume: %d",
        results["success"],
        results["failed"],
        results["skipped_no_resume"],
    )
    logger.info("=" * 70)


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_pipeline()
