"""
SharePoint Uploader — uploads renamed resumes to a SharePoint document library
organized into subfolders by Job ID, and stamps metadata on each file.

Folder structure on SharePoint:
  Documents/
  └── Resumes/
      ├── 5101_Trainee_Accountant_US_Accounting_and_Taxation/
      │   ├── Miteshkumar_Bhailalbhai_Rohit_5101_2026-02-16.pdf
      │   └── Another_Candidate_5101_2026-02-16.pdf
      ├── 5202_Software_Engineer/
      │   └── ...
      └── ...

Prerequisites on SharePoint:
  The target document library needs custom columns created (run setup_sharepoint_columns.py):
    - CandidateName  (Single line of text)
    - CandidateEmail (Single line of text)
    - CandidatePhone (Single line of text)
    - JobID          (Single line of text)
    - JobRole        (Single line of text)
"""

import logging
import os
import requests
from urllib.parse import quote

import config

logger = logging.getLogger(__name__)

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

        logger.info(
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
        logger.debug("Resolved site id: %s", self._site_id)
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
            logger.warning(
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
                    logger.info("Created folder: %s", current)
                else:
                    logger.error(
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
            logger.warning("No metadata to set for item %s", item_id)
            return

        resp = requests.patch(url, headers=self.headers, json=fields, timeout=30)
        if resp.status_code == 200:
            logger.info("Metadata set on item %s: %s", item_id, fields)
        else:
            logger.warning(
                "Failed to set metadata on %s (HTTP %s): %s",
                item_id,
                resp.status_code,
                resp.text,
            )
