import os
import re
import requests
import msal
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Constants migrated from alpha_app settings/env
# We use the user's folder structure or env if defined
SP_TEXT_RESUMES_FOLDER = os.getenv("SHAREPOINT_BASE_FOLDER", "Resumes")
SP_TEXT_JD_FOLDER = os.getenv("SHAREPOINT_JD_FOLDER", "JobDescriptions")


class SharePointMatchScoreUpdater:
    """
    Finds a resume file already uploaded to SharePoint (by filename) and
    writes the MatchScore rounded integer into the 'MatchScore' column.
    Also handles browsing and downloading files.
    """

    GRAPH_BASE = "https://graph.microsoft.com/v1.0"
    SCOPES = ["https://graph.microsoft.com/.default"]

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        site_domain: str,
        site_path: str,
        drive_name: str,
    ):
        self._msal_app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
        )
        self.site_domain = site_domain
        self.site_path = site_path.strip("/")
        self.drive_name = drive_name
        self._site_id: str | None = None
        self._drive_id: str | None = None

    def _headers(self) -> dict:
        result = self._msal_app.acquire_token_silent(self.SCOPES, account=None)
        if not result:
            result = self._msal_app.acquire_token_for_client(scopes=self.SCOPES)
        if "access_token" not in result:
            raise RuntimeError(
                result.get("error_description", "Token acquisition failed")
            )
        return {
            "Authorization": f"Bearer {result['access_token']}",
            "Content-Type": "application/json",
        }

    def _get_site_id(self) -> str:
        if self._site_id:
            return self._site_id
        url = f"{self.GRAPH_BASE}/sites/{self.site_domain}:/{self.site_path}"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        self._site_id = resp.json()["id"]
        return self._site_id

    def _get_drive_id(self) -> str:
        if self._drive_id:
            return self._drive_id
        site_id = self._get_site_id()
        url = f"{self.GRAPH_BASE}/sites/{site_id}/drives"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        drives = resp.json().get("value", [])
        for d in drives:
            if d["name"].lower() == self.drive_name.lower():
                self._drive_id = d["id"]
                return self._drive_id
        if drives:
            self._drive_id = drives[0]["id"]
            return self._drive_id
        raise RuntimeError(
            f"No drives found on SharePoint site '{self.site_domain}/{self.site_path}'"
        )

    # ── Folder browsing ──────────────────────────────────────────────────────

    def _list_folder_children(
        self, folder_path: str, include_fields: bool = False
    ) -> list[dict]:
        from urllib.parse import quote as _quote

        drive_id = self._get_drive_id()
        encoded = _quote(folder_path.strip("/"), safe="/")

        expand = "&$expand=listItem($expand=fields)" if include_fields else ""
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{encoded}:/children?$select=id,name,file,folder&$top=999{expand}"

        print(f"[DEBUG] SharePoint _list_folder_children: {folder_path}")

        items = []
        while url:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            if not resp.ok:
                print(
                    f"[ERROR] SharePoint folder list failed: {resp.status_code} - {resp.text}"
                )
                break
            data = resp.json()
            items.extend(data.get("value", []))
            url = data.get("@odata.nextLink")

        print(f"[DEBUG] SharePoint folder {folder_path} returned {len(items)} items")
        return items

    def list_resumes_grouped(self) -> dict[str, list[dict]]:
        """Return ALL role subfolders, but only include resume files WITHOUT a MatchScore."""
        subfolders = [
            item
            for item in self._list_folder_children(SP_TEXT_RESUMES_FOLDER)
            if "folder" in item
        ]
        groups = {}
        for sf in subfolders:
            sf_path = f"{SP_TEXT_RESUMES_FOLDER}/{sf['name']}"
            # Fetch files WITH listItem fields so we can check MatchScore
            children = self._list_folder_children(sf_path, include_fields=True)
            files = []
            for f in children:
                if "file" not in f:
                    continue
                name_lower = f["name"].lower()
                if not (
                    name_lower.endswith(".txt")
                    or name_lower.endswith(".pdf")
                    or name_lower.endswith(".docx")
                ):
                    continue

                # Skip files that already have a MatchScore
                fields = (f.get("listItem") or {}).get("fields", {})
                match_score = fields.get("MatchScore")
                if match_score is not None and match_score > 0:
                    continue

                files.append({"id": f["id"], "name": f["name"]})

            # Always include the subfolder — even if empty (so the role still shows)
            groups[sf["name"]] = files
        return groups

    def list_jd_files(self) -> list[dict]:
        items = self._list_folder_children(SP_TEXT_JD_FOLDER)
        jds = [
            {"id": f["id"], "name": f["name"]}
            for f in items
            if "file" in f
            and (
                f["name"].lower().endswith(".txt")
                or f["name"].lower().endswith(".pdf")
                or f["name"].lower().endswith(".docx")
            )
        ]
        print(f"[DEBUG] list_jd_files: found {len(jds)} files in {SP_TEXT_JD_FOLDER}")
        return jds

    def download_text_content(self, item_id: str) -> str:
        drive_id = self._get_drive_id()
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"
        resp = requests.get(
            url, headers=self._headers(), timeout=60, allow_redirects=True
        )
        resp.raise_for_status()

        # Check filename to see if we need to parse binary
        item_meta_url = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
        meta_resp = requests.get(item_meta_url, headers=self._headers())
        filename = meta_resp.json().get("name", "").lower()

        content = resp.content
        if filename.endswith(".pdf"):
            import PyPDF2
            import io

            reader = PyPDF2.PdfReader(io.BytesIO(content))
            return "\n".join(page.extract_text() or "" for page in reader.pages)
        elif filename.endswith(".docx"):
            from docx import Document
            import io

            doc = Document(io.BytesIO(content))
            return "\n".join(p.text for p in doc.paragraphs)
        else:
            return content.decode("utf-8", errors="replace")

    # ── File lookup ──────────────────────────────────────────────────────────

    def find_matching_items(self, filename: str, role_hint: str = "") -> list[dict]:
        drive_id = self._get_drive_id()
        stem = Path(filename).stem
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root/search(q='{stem}')"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        if not resp.ok:
            return []

        matches = []
        for item in resp.json().get("value", []):
            if "folder" in item:
                continue
            if item.get("name", "").lower() != filename.lower():
                continue
            parent_path = item.get("parentReference", {}).get("path", "") or ""
            matches.append(
                {
                    "id": item["id"],
                    "name": item["name"],
                    "path": parent_path,
                }
            )

        if len(matches) <= 1 or not role_hint:
            return matches

        role_tokens = [t.lower() for t in re.split(r"[\W_]+", role_hint) if len(t) > 2]

        def _score(m: dict) -> int:
            p = m["path"].lower()
            return sum(1 for t in role_tokens if t in p)

        ranked = sorted(matches, key=_score, reverse=True)
        top_score = _score(ranked[0])
        top_group = [m for m in ranked if _score(m) == top_score]
        return top_group if len(top_group) == 1 else ranked

    def push_metadata(
        self,
        filename: str,
        metadata: dict,
        role_hint: str = "",
        confirmed_item_id: str = "",
    ) -> tuple[str, str, list[dict]]:
        drive_id = self._get_drive_id()
        if confirmed_item_id:
            item_id = confirmed_item_id
        else:
            candidates = self.find_matching_items(filename, role_hint=role_hint)
            if not candidates:
                return (
                    "NOT_FOUND",
                    f"File **{filename}** not found in SharePoint.",
                    [],
                )
            if len(candidates) > 1:
                return ("NEEDS_CONFIRM", "Multiple matches found.", candidates)
            item_id = candidates[0]["id"]

        url = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}/listItem/fields"
        resp = requests.patch(url, headers=self._headers(), json=metadata, timeout=30)
        if resp.status_code == 200:
            return ("OK", f"Metadata updated successfully for `{filename}`.", [])
        return ("ERROR", f"SharePoint Error {resp.status_code}: {resp.text[:200]}", [])
