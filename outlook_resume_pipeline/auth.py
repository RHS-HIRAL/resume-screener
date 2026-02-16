"""
Authentication module — acquires tokens from Entra ID using client-credentials flow.
"""

import logging
import msal
import config

logger = logging.getLogger(__name__)


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
            logger.info("No cached token — acquiring new token via client credentials.")
            result = self._app.acquire_token_for_client(scopes=config.SCOPES)

        if "access_token" in result:
            return result["access_token"]

        error = result.get("error_description", result.get("error", "Unknown error"))
        logger.error("Token acquisition failed: %s", error)
        raise RuntimeError(f"Could not acquire token: {error}")

    def get_headers(self) -> dict:
        """Return standard headers for Graph API calls."""
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json",
        }
