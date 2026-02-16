"""
One-time setup helper — creates the custom metadata columns in the
SharePoint document library so that uploaded resumes can be tagged.

Run this ONCE before the first pipeline run:
    python setup_sharepoint_columns.py

If you get errors, run `python diagnose.py` first to check permissions.
"""

import logging
import sys
import requests

import config
from auth import GraphAuthProvider

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

COLUMNS = [
    {"name": "CandidateName", "displayName": "Candidate Name", "text": {}},
    {"name": "CandidateEmail", "displayName": "Candidate Email", "text": {}},
    {"name": "CandidatePhone", "displayName": "Candidate Phone", "text": {}},
    {"name": "JobID", "displayName": "Job ID", "text": {}},
    {"name": "JobRole", "displayName": "Job Role", "text": {}},
]


def resolve_list_id(headers: dict, site_id: str, drive_name: str) -> str:
    """Find the list ID backing the document library."""
    url = f"{config.GRAPH_BASE_URL}/sites/{site_id}/lists"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    for lst in resp.json().get("value", []):
        if lst["displayName"].lower() == drive_name.lower():
            return lst["id"]

    # Not found — show available lists to help user
    available = [lst["displayName"] for lst in resp.json().get("value", [])]
    logger.error(
        "Document library '%s' not found. Available lists: %s",
        drive_name,
        ", ".join(available) or "(none)",
    )
    logger.error("Update SHAREPOINT_DRIVE_NAME in .env to match one of the above.")
    raise RuntimeError(f"Document library '{drive_name}' not found.")


def create_columns(headers: dict, site_id: str, list_id: str):
    url = f"{config.GRAPH_BASE_URL}/sites/{site_id}/lists/{list_id}/columns"
    existing_resp = requests.get(url, headers=headers, timeout=30)
    existing_resp.raise_for_status()
    existing_names = {c["name"] for c in existing_resp.json().get("value", [])}

    for col in COLUMNS:
        if col["name"] in existing_names:
            logger.info("Column '%s' already exists — skipping.", col["name"])
            continue
        resp = requests.post(url, headers=headers, json=col, timeout=30)
        if resp.status_code == 201:
            logger.info("Created column: %s", col["displayName"])
        else:
            logger.error(
                "Failed to create '%s': %s %s", col["name"], resp.status_code, resp.text
            )


def main():
    # Validate config
    if not config.TENANT_ID or not config.CLIENT_ID or not config.CLIENT_SECRET:
        logger.error(
            "Missing AZURE_TENANT_ID, AZURE_CLIENT_ID, or AZURE_CLIENT_SECRET in .env"
        )
        sys.exit(1)

    # Authenticate
    auth = GraphAuthProvider()
    headers = auth.get_headers()

    # Resolve SharePoint site
    domain = config.SHAREPOINT_SITE_DOMAIN
    path = config.SHAREPOINT_SITE_PATH.strip("/")
    site_url = f"{config.GRAPH_BASE_URL}/sites/{domain}:/{path}"

    logger.info("Looking up site: %s", site_url)
    site_resp = requests.get(site_url, headers=headers, timeout=30)

    if site_resp.status_code == 401:
        logger.error(
            "401 Unauthorized — your app does not have permission to access SharePoint."
        )
        logger.error("")
        logger.error("TO FIX THIS:")
        logger.error(
            "  1. Go to Azure Portal → Entra ID → App registrations → your app"
        )
        logger.error("  2. Click 'API permissions' in the left menu")
        logger.error(
            "  3. Click 'Add a permission' → 'Microsoft Graph' → 'Application permissions'"
        )
        logger.error("  4. Search for and add: Sites.ReadWrite.All")
        logger.error(
            "  5. IMPORTANT: Click the 'Grant admin consent for <your-tenant>' button"
        )
        logger.error("     (Without admin consent, the permission has no effect)")
        logger.error("")
        logger.error("  After granting consent, wait 1-2 minutes and try again.")
        logger.error("")
        logger.error("  Tip: Run 'python diagnose.py' for a full permissions check.")
        sys.exit(1)

    elif site_resp.status_code == 404:
        logger.error("404 Not Found — site path '%s:/%s' does not exist.", domain, path)
        logger.error("")
        logger.error("TO FIX THIS:")
        logger.error(
            "  Check SHAREPOINT_SITE_DOMAIN and SHAREPOINT_SITE_PATH in your .env file."
        )
        logger.error(
            "  Your SharePoint URL is probably: https://%s/sites/SomeName", domain
        )
        logger.error("  So SHAREPOINT_SITE_PATH should be: /sites/SomeName")
        logger.error("")

        # Try to list available sites to help
        search_url = (
            f"{config.GRAPH_BASE_URL}/sites?search=*&$top=10&$select=name,webUrl"
        )
        search_resp = requests.get(search_url, headers=headers, timeout=30)
        if search_resp.status_code == 200:
            sites = search_resp.json().get("value", [])
            if sites:
                logger.error("Available sites on your tenant:")
                for s in sites:
                    logger.error("  • %s", s.get("webUrl", "?"))
        sys.exit(1)

    elif site_resp.status_code != 200:
        logger.error(
            "Unexpected error %s: %s", site_resp.status_code, site_resp.text[:500]
        )
        sys.exit(1)

    site_id = site_resp.json()["id"]
    logger.info("Site ID: %s", site_id)

    list_id = resolve_list_id(headers, site_id, config.SHAREPOINT_DRIVE_NAME)
    logger.info("List ID: %s", list_id)

    create_columns(headers, site_id, list_id)
    logger.info("Done! SharePoint columns are ready.")


if __name__ == "__main__":
    main()
