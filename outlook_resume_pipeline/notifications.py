"""
Notifications — sends a summary card to Microsoft Teams
after each pipeline run so HR has visibility without checking SharePoint.
"""

import logging
from datetime import datetime

import requests

import config

logger = logging.getLogger(__name__)


def send_summary(
    results: dict,
    candidates_processed: list[dict],
) -> None:
    """
    Send a run summary to all configured channels.

    Args:
        results: {"success": int, "failed": int, "skipped_no_resume": int}
        candidates_processed: list of dicts with keys:
            name, email, job_id, job_role, status ("uploaded" | "failed" | "no_resume")
    """
    if config.TEAMS_WEBHOOK_URL:
        _send_teams(results, candidates_processed)


def _send_teams(results: dict, candidates: list[dict]) -> None:
    """
    Post an Adaptive Card to a Teams channel via an Incoming Webhook.

    Works with both:
      - Legacy Office 365 connectors (being retired)
      - New Workflows-based webhooks (recommended)

    For Workflows webhooks, the payload must be an Adaptive Card wrapped in
    an "attachments" array inside a "body" object.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    total = results["success"] + results["failed"] + results["skipped_no_resume"]

    # Build candidate rows for the table
    candidate_rows = []
    for c in candidates[:20]:  # cap at 20 to avoid oversized payloads
        status_emoji = {"uploaded": "✅", "failed": "❌", "no_resume": "⚠️"}.get(
            c["status"], "❓"
        )
        candidate_rows.append(
            {
                "type": "TableRow",
                "cells": [
                    {
                        "type": "TableCell",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": c.get("name", "—"),
                                "wrap": True,
                            }
                        ],
                    },
                    {
                        "type": "TableCell",
                        "items": [{"type": "TextBlock", "text": c.get("job_id", "—")}],
                    },
                    {
                        "type": "TableCell",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": c.get("job_role", "—"),
                                "wrap": True,
                            }
                        ],
                    },
                    {
                        "type": "TableCell",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": f"{status_emoji} {c['status']}",
                            }
                        ],
                    },
                ],
            }
        )

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
                            "text": f"📄 Resume Pipeline Summary — {now}",
                        },
                        {
                            "type": "ColumnSet",
                            "columns": [
                                _teams_stat_column(
                                    "Uploaded", str(results["success"]), "good"
                                ),
                                _teams_stat_column(
                                    "Failed", str(results["failed"]), "attention"
                                ),
                                _teams_stat_column(
                                    "No Resume",
                                    str(results["skipped_no_resume"]),
                                    "warning",
                                ),
                                _teams_stat_column("Total", str(total), "accent"),
                            ],
                        },
                        {
                            "type": "TextBlock",
                            "text": "**Candidates**",
                            "spacing": "Medium",
                        },
                        {
                            "type": "Table",
                            "columns": [
                                {"width": 2},
                                {"width": 1},
                                {"width": 2},
                                {"width": 1},
                            ],
                            "firstRowAsHeader": True,
                            "rows": [
                                {
                                    "type": "TableRow",
                                    "style": "accent",
                                    "cells": [
                                        {
                                            "type": "TableCell",
                                            "items": [
                                                {
                                                    "type": "TextBlock",
                                                    "text": "Name",
                                                    "weight": "Bolder",
                                                }
                                            ],
                                        },
                                        {
                                            "type": "TableCell",
                                            "items": [
                                                {
                                                    "type": "TextBlock",
                                                    "text": "Job ID",
                                                    "weight": "Bolder",
                                                }
                                            ],
                                        },
                                        {
                                            "type": "TableCell",
                                            "items": [
                                                {
                                                    "type": "TextBlock",
                                                    "text": "Role",
                                                    "weight": "Bolder",
                                                }
                                            ],
                                        },
                                        {
                                            "type": "TableCell",
                                            "items": [
                                                {
                                                    "type": "TextBlock",
                                                    "text": "Status",
                                                    "weight": "Bolder",
                                                }
                                            ],
                                        },
                                    ],
                                },
                                *candidate_rows,
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
            logger.info("Teams notification sent successfully.")
        else:
            logger.warning("Teams webhook returned %s: %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error("Failed to send Teams notification: %s", e)


def _teams_stat_column(label: str, value: str, style: str) -> dict:
    return {
        "type": "Column",
        "width": "stretch",
        "items": [
            {
                "type": "TextBlock",
                "text": value,
                "size": "ExtraLarge",
                "weight": "Bolder",
                "horizontalAlignment": "Center",
            },
            {
                "type": "TextBlock",
                "text": label,
                "horizontalAlignment": "Center",
                "spacing": "None",
                "isSubtle": True,
            },
        ],
    }
