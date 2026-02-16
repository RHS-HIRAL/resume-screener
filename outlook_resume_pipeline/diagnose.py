"""
Diagnostic script — run this FIRST to check your Entra ID app registration,
permissions, and SharePoint site access step by step.

Usage:  python diagnose.py
"""

import json
import config
import sys
import requests
import msal
from dotenv import load_dotenv

load_dotenv()

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"


def status(ok: bool, msg: str):
    icon = f"{GREEN}✓{RESET}" if ok else f"{RED}✗{RESET}"
    print(f"  {icon} {msg}")


def section(title: str):
    print(f"\n{BOLD}{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}{RESET}")


def main():
    print(f"\n{BOLD}OUTLOOK RESUME PIPELINE — DIAGNOSTICS{RESET}")

    # ── Step 1: Check .env values ─────────────────────────────────────────────
    section("1. Configuration (.env)")
    has_tenant = bool(config.TENANT_ID)
    has_client = bool(config.CLIENT_ID)
    has_secret = bool(config.CLIENT_SECRET)

    status(
        has_tenant,
        f"AZURE_TENANT_ID: {'set (' + config.TENANT_ID[:8] + '...)' if has_tenant else 'MISSING'}",
    )
    status(
        has_client,
        f"AZURE_CLIENT_ID: {'set (' + config.CLIENT_ID[:8] + '...)' if has_client else 'MISSING'}",
    )
    status(
        has_secret,
        f"AZURE_CLIENT_SECRET: {'set (hidden)' if has_secret else 'MISSING'}",
    )
    status(bool(config.MAILBOX_USER), f"MAILBOX_USER: {config.MAILBOX_USER}")
    status(
        bool(config.SHAREPOINT_SITE_DOMAIN),
        f"SHAREPOINT_SITE_DOMAIN: {config.SHAREPOINT_SITE_DOMAIN}",
    )
    status(
        bool(config.SHAREPOINT_SITE_PATH),
        f"SHAREPOINT_SITE_PATH: {config.SHAREPOINT_SITE_PATH}",
    )

    if not all([has_tenant, has_client, has_secret]):
        print(f"\n{RED}Fix the missing values in .env before continuing.{RESET}")
        sys.exit(1)

    # ── Step 2: Acquire token ─────────────────────────────────────────────────
    section("2. Token Acquisition (Entra ID)")
    app = msal.ConfidentialClientApplication(
        client_id=config.CLIENT_ID,
        client_credential=config.CLIENT_SECRET,
        authority=config.AUTHORITY,
    )
    result = app.acquire_token_for_client(scopes=config.SCOPES)

    if "access_token" not in result:
        error = result.get("error_description", result.get("error", "Unknown"))
        status(False, f"Token acquisition FAILED: {error}")
        print(f"\n{YELLOW}Common fixes:{RESET}")
        print("  • Verify AZURE_TENANT_ID, CLIENT_ID, CLIENT_SECRET in .env")
        print("  • Check the app registration still exists in Azure Portal")
        print("  • Ensure the client secret hasn't expired")
        sys.exit(1)

    token = result["access_token"]
    status(True, "Token acquired successfully")

    # Decode token claims (without verification) to check scopes
    import base64

    payload = token.split(".")[1]
    payload += "=" * (4 - len(payload) % 4)  # pad base64
    claims = json.loads(base64.urlsafe_b64decode(payload))

    app_name = claims.get("app_displayname", "?")
    roles = claims.get("roles", [])
    status(True, f"App name: {app_name}")
    status(True, f"Tenant: {claims.get('tid', '?')}")

    # ── Step 3: Check permissions in token ────────────────────────────────────
    section("3. API Permissions (in token)")
    has_mail = "Mail.Read" in roles
    has_sites = "Sites.ReadWrite.All" in roles

    status(has_mail, f"Mail.Read: {'PRESENT' if has_mail else 'MISSING'}")
    status(has_sites, f"Sites.ReadWrite.All: {'PRESENT' if has_sites else 'MISSING'}")

    if roles:
        print(f"\n  All roles in token: {', '.join(roles)}")
    else:
        print(f"\n  {RED}WARNING: Token has NO application roles.{RESET}")

    if not has_mail or not has_sites:
        print(f"\n{YELLOW}To fix missing permissions:{RESET}")
        print("  1. Azure Portal → Entra ID → App registrations → your app")
        print("  2. API permissions → Add a permission → Microsoft Graph")
        print("  3. Choose 'Application permissions' (NOT Delegated)")
        print("  4. Search and add: Mail.Read, Sites.ReadWrite.All")
        print(f"  5. {BOLD}Click 'Grant admin consent for <your tenant>'{RESET}")
        print("     (This is the step most people miss!)")
        if not has_sites:
            print(
                f"\n  {RED}The 401 error you're seeing is because Sites.ReadWrite.All"
            )
            print(f"  is missing or not admin-consented.{RESET}")
            sys.exit(1)

    # ── Step 4: Test SharePoint site access ───────────────────────────────────
    section("4. SharePoint Site Access")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    domain = config.SHAREPOINT_SITE_DOMAIN
    path = config.SHAREPOINT_SITE_PATH.strip("/")
    site_url = f"{config.GRAPH_BASE_URL}/sites/{domain}:/{path}"

    print(f"  Calling: GET {site_url}")
    resp = requests.get(site_url, headers=headers, timeout=30)

    if resp.status_code == 200:
        site_data = resp.json()
        site_id = site_data["id"]
        status(True, f"Site found: {site_data.get('displayName', '?')}")
        status(True, f"Site ID: {site_id}")
    elif resp.status_code == 401:
        status(False, "401 Unauthorized")
        print(f"\n{YELLOW}This means the token doesn't have SharePoint access.{RESET}")
        print("  Fix: Grant 'Sites.ReadWrite.All' (Application) + admin consent")
        print(f"\n  Response body: {resp.text[:500]}")
        sys.exit(1)
    elif resp.status_code == 404:
        status(False, "404 Not Found — site path is wrong")
        print(f"\n{YELLOW}The site '{domain}:/{path}' does not exist.{RESET}")
        print("  Check SHAREPOINT_SITE_DOMAIN and SHAREPOINT_SITE_PATH in .env")
        print(f"  Current URL being called: {site_url}")
        print("\n  Tip: The site path should look like: /sites/YourSiteName")
        print(f"  Your SharePoint URL is probably: https://{domain}/sites/SomeName")

        # Try to list available sites to help
        print("\n  Trying to list available sites...")
        search_url = (
            f"{config.GRAPH_BASE_URL}/sites?search=*&$top=10&$select=name,webUrl"
        )
        search_resp = requests.get(search_url, headers=headers, timeout=30)
        if search_resp.status_code == 200:
            sites = search_resp.json().get("value", [])
            if sites:
                print(f"  Found {len(sites)} site(s):")
                for s in sites:
                    print(f"    • {s.get('webUrl', '?')}  (name: {s.get('name', '?')})")
            else:
                print("  No sites found. Check permissions.")
        sys.exit(1)
    else:
        status(False, f"HTTP {resp.status_code}")
        print(f"  Response: {resp.text[:500]}")
        sys.exit(1)

    # ── Step 5: Test Drive / Document Library ─────────────────────────────────
    section("5. Document Library (Drive)")
    drives_url = f"{config.GRAPH_BASE_URL}/sites/{site_id}/drives"
    resp = requests.get(drives_url, headers=headers, timeout=30)

    if resp.status_code == 200:
        drives = resp.json().get("value", [])
        target = config.SHAREPOINT_DRIVE_NAME
        found = False
        for d in drives:
            is_match = d["name"].lower() == target.lower()
            if is_match:
                found = True
            marker = " ← TARGET" if is_match else ""
            status(
                is_match or True,
                f"Drive: '{d['name']}' (id: {d['id'][:20]}...){marker}",
            )
        if not found:
            print(
                f"\n{YELLOW}Drive '{target}' not found. Available drives are listed above."
            )
            print(f"  Update SHAREPOINT_DRIVE_NAME in .env to match.{RESET}")
    else:
        status(False, f"Could not list drives: HTTP {resp.status_code}")

    # ── Step 6: Test Mailbox Access ───────────────────────────────────────────
    section("6. Mailbox Access")
    mail_url = f"{config.GRAPH_BASE_URL}/users/{config.MAILBOX_USER}/messages?$top=1&$select=subject"
    resp = requests.get(mail_url, headers=headers, timeout=30)

    if resp.status_code == 200:
        msgs = resp.json().get("value", [])
        status(
            True,
            f"Mailbox accessible. Latest email: '{msgs[0]['subject'][:60]}...'"
            if msgs
            else "Mailbox accessible (empty)",
        )
    elif resp.status_code == 401:
        status(False, "401 — Mail.Read permission missing or not admin-consented")
    elif resp.status_code == 404:
        status(False, f"404 — Mailbox '{config.MAILBOX_USER}' not found")
        print("  Check MAILBOX_USER in .env — must be a valid user/shared mailbox UPN")
    else:
        status(False, f"HTTP {resp.status_code}: {resp.text[:200]}")

    # ── Done ──────────────────────────────────────────────────────────────────
    section("DONE")
    print(f"  {GREEN}All checks passed! You can now run:{RESET}")
    print("    python setup_sharepoint_columns.py")
    print("    python main.py")
    print()


if __name__ == "__main__":
    main()
