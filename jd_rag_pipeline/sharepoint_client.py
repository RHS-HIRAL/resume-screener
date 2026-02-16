"""
SharePoint client — authenticates via Microsoft Graph API (client credentials flow)
and downloads PDF files from the specified folder.
"""

import os
import time
import requests
from pathlib import Path
from msal import ConfidentialClientApplication

from config import SharePointConfig


class SharePointClient:
    """Downloads files from a SharePoint document library folder using MS Graph API."""

    GRAPH_BASE = "https://graph.microsoft.com/v1.0"

    def __init__(self, config: SharePointConfig, download_dir: str = "./downloads"):
        self.config = config
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._token: str | None = None
        self._token_expiry: float = 0

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------
    def _get_token(self) -> str:
        """Acquire or refresh an access token using client-credentials flow."""
        if self._token and time.time() < self._token_expiry:
            return self._token

        app = ConfidentialClientApplication(
            client_id=self.config.client_id,
            client_credential=self.config.client_secret,
            authority=self.config.authority,
        )
        result = app.acquire_token_for_client(scopes=self.config.scope)

        if "access_token" not in result:
            error = result.get(
                "error_description", result.get("error", "Unknown error")
            )
            raise RuntimeError(f"Failed to acquire token: {error}")

        self._token = result["access_token"]
        self._token_expiry = time.time() + result.get("expires_in", 3600) - 60
        print("✅ SharePoint authentication successful.")
        return self._token

    @property
    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ------------------------------------------------------------------
    # Site & Drive discovery
    # ------------------------------------------------------------------
    def _get_site_id(self) -> str:
        """Resolve the SharePoint site ID from domain + site name."""
        url = f"{self.GRAPH_BASE}/sites/{self.config.domain}:/sites/{self.config.site_name}"
        resp = requests.get(url, headers=self._headers, timeout=30)
        resp.raise_for_status()
        site_id = resp.json()["id"]
        print(f"📍 Site ID: {site_id}")
        return site_id

    def _get_drive_id(self, site_id: str) -> str:
        """Get the default document library drive ID for the site."""
        url = f"{self.GRAPH_BASE}/sites/{site_id}/drive"
        resp = requests.get(url, headers=self._headers, timeout=30)
        resp.raise_for_status()
        drive_id = resp.json()["id"]
        print(f"💾 Drive ID: {drive_id}")
        return drive_id

    # ------------------------------------------------------------------
    # List & Download
    # ------------------------------------------------------------------
    def _list_files_in_folder(self, drive_id: str, folder_path: str) -> list[dict]:
        """List all items inside the given folder path."""
        # Encode folder path for URL
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{folder_path}:/children"
        all_items = []

        while url:
            resp = requests.get(url, headers=self._headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            all_items.extend(data.get("value", []))
            url = data.get("@odata.nextLink")  # handle pagination

        return all_items

    def _download_file(self, download_url: str, filename: str) -> Path:
        """Download a single file and save it locally."""
        local_path = self.download_dir / filename
        resp = requests.get(
            download_url, headers=self._headers, timeout=120, stream=True
        )
        resp.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        return local_path

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def download_pdfs(self) -> list[Path]:
        """
        Main entry point — discovers the site/drive, lists PDFs in the
        configured folder, downloads them, and returns local file paths.
        """
        site_id = self._get_site_id()
        drive_id = self._get_drive_id(site_id)
        items = self._list_files_in_folder(drive_id, self.config.folder_path)

        pdf_items = [
            item
            for item in items
            if item.get("name", "").lower().endswith(".pdf") and "file" in item
        ]
        print(
            f"📄 Found {len(pdf_items)} PDF file(s) in '{self.config.folder_path}'.\n"
        )

        downloaded = []
        for item in pdf_items:
            name = item["name"]
            dl_url = item["@microsoft.graph.downloadUrl"]
            print(f"   ⬇️  Downloading: {name}")
            path = self._download_file(dl_url, name)
            downloaded.append(path)

        print(f"\n✅ Downloaded {len(downloaded)} PDF(s) to '{self.download_dir}'.")
        return downloaded
