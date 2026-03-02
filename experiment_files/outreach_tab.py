"""
outreach_tab.py — Streamlit Outreach Tab
Import this in alpha_app.py and call render_outreach_tab(api_url).

Displays all screened candidates from SQLite, lets you:
  • Filter by JD and minimum match score
  • Send JD + meeting invite to individual candidates
  • Bulk-send to all filtered shortlisted candidates
  • See real-time email status per candidate
"""

import requests
import streamlit as st


# ═══════════════════════════════════════════════════════════════════════════
# API HELPERS
# ═══════════════════════════════════════════════════════════════════════════


def _api(api_url: str, method: str, path: str, **kwargs):
    """Thin wrapper around requests — returns (data, error_str)."""
    url = api_url.rstrip("/") + path
    try:
        fn = getattr(requests, method)
        res = fn(url, timeout=60, **kwargs)
        res.raise_for_status()
        return res.json(), None
    except requests.exceptions.ConnectionError:
        return None, f"Cannot connect to API at {url}"
    except requests.exceptions.HTTPError as e:
        return None, f"API error {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        return None, str(e)


# ═══════════════════════════════════════════════════════════════════════════
# STATUS BADGE HTML
# ═══════════════════════════════════════════════════════════════════════════


def _status_badge(status: str | None) -> str:
    s = (status or "").lower()
    if s == "sent":
        return '<span style="background:rgba(16,185,129,0.15);color:#34d399;border:1px solid rgba(16,185,129,0.3);padding:2px 10px;border-radius:999px;font-size:0.72rem;font-weight:700;">✓ Sent</span>'
    if s == "failed":
        return '<span style="background:rgba(239,68,68,0.12);color:#f87171;border:1px solid rgba(239,68,68,0.3);padding:2px 10px;border-radius:999px;font-size:0.72rem;font-weight:700;">✗ Failed</span>'
    if s == "skipped":
        return '<span style="background:rgba(251,191,36,0.13);color:#fbbf24;border:1px solid rgba(251,191,36,0.3);padding:2px 10px;border-radius:999px;font-size:0.72rem;font-weight:700;">— Skipped</span>'
    return '<span style="background:rgba(99,102,241,0.1);color:#818cf8;border:1px solid rgba(99,102,241,0.2);padding:2px 10px;border-radius:999px;font-size:0.72rem;font-weight:700;">● Pending</span>'


def _score_color(score: int) -> str:
    if score >= 75:
        return "#34d399"
    if score >= 50:
        return "#fbbf24"
    return "#f87171"


# ═══════════════════════════════════════════════════════════════════════════
# MAIN TAB RENDERER — call this from alpha_app.py
# ═══════════════════════════════════════════════════════════════════════════


def render_outreach_tab(api_url: str) -> None:

    st.markdown(
        """
    <div style="padding:1.5rem 0 0.5rem 0;">
        <div style="font-family:'DM Serif Display',serif;font-size:1.8rem;color:#e8eaf2;">
            Candidate Outreach
        </div>
        <div style="font-size:0.85rem;color:#6b7280;margin-top:0.3rem;">
            Send JD + interview invite to shortlisted candidates sourced from Naukri
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # ── Load JDs from DB ──────────────────────────────────────────────────────
    jds_data, err = _api(api_url, "get", "/api/v1/jds")
    if err:
        st.error(f"❌ Could not load JDs: {err}")
        return
    if not jds_data:
        st.info("No screened candidates yet. Run 'Screen Resumes' first.")
        return

    # ── Filters ───────────────────────────────────────────────────────────────
    st.markdown(
        '<div class="rs-card" style="padding:1.2rem 1.6rem;">', unsafe_allow_html=True
    )
    filter_col1, filter_col2, filter_col3 = st.columns([2, 1, 2])

    with filter_col1:
        jd_options = {f"{j['role_name']} (ID {j['id']})": j["id"] for j in jds_data}
        jd_options_list = ["All JDs"] + list(jd_options.keys())
        selected_jd_label = st.selectbox(
            "Filter by Job Description", jd_options_list, key="outreach_jd_filter"
        )
        selected_jd_id = jd_options.get(selected_jd_label)  # None = All

    with filter_col2:
        min_score = st.number_input(
            "Min Match Score",
            min_value=0,
            max_value=100,
            value=60,
            step=5,
            key="outreach_min_score",
            help="Only show candidates with score ≥ this value",
        )

    with filter_col3:
        meeting_link = st.text_input(
            "Meeting / Calendly Link",
            placeholder="https://calendly.com/your-link",
            key="outreach_meeting_link",
            help="Included as a button in the email. Leave blank to skip.",
        )

    st.markdown("</div>", unsafe_allow_html=True)

    # ── Custom message (optional) ──────────────────────────────────────────────
    with st.expander(
        "✏️ Custom Message (optional — added to all outreach emails)", expanded=False
    ):
        custom_msg = st.text_area(
            "Message",
            placeholder="Add a personalised note that appears at the top of the email…",
            height=100,
            key="outreach_custom_msg",
            label_visibility="collapsed",
        )

    # ── Fetch candidates ──────────────────────────────────────────────────────
    params = {"min_score": int(min_score)}
    if selected_jd_id:
        params["jd_id"] = selected_jd_id

    candidates, err = _api(api_url, "get", "/api/v1/candidates", params=params)
    if err:
        st.error(f"❌ Could not load candidates: {err}")
        return

    if not candidates:
        st.info(f"No candidates found with score ≥ {min_score} for the selected JD.")
        return

    # ── Summary row ───────────────────────────────────────────────────────────
    total = len(candidates)
    sent = sum(1 for c in candidates if c.get("outreach_sent") == 1)
    pending = total - sent
    no_email = sum(1 for c in candidates if not (c.get("email") or "").strip())

    m1, m2, m3, m4 = st.columns(4)
    for col, label, val, color in [
        (m1, "Total Candidates", total, "#818cf8"),
        (m2, "Outreach Sent", sent, "#34d399"),
        (m3, "Pending", pending, "#fbbf24"),
        (m4, "No Email Found", no_email, "#f87171"),
    ]:
        with col:
            st.markdown(
                f"""
            <div style="text-align:center;background:#161a24;border:1px solid rgba(255,255,255,0.07);
                        border-radius:10px;padding:0.9rem 0.5rem;">
                <div style="font-size:1.8rem;font-weight:700;color:{color};">{val}</div>
                <div style="font-size:0.68rem;color:#6b7280;text-transform:uppercase;
                            letter-spacing:0.08em;font-weight:600;margin-top:0.2rem;">{label}</div>
            </div>""",
                unsafe_allow_html=True,
            )

    st.markdown("")

    # ── Bulk send button ──────────────────────────────────────────────────────
    bulk_targets = [
        c
        for c in candidates
        if (c.get("email") or "").strip() and c.get("outreach_sent") != 1
    ]

    bc1, bc2 = st.columns([2, 4])
    with bc1:
        bulk_clicked = st.button(
            f"📨 Bulk Send to {len(bulk_targets)} Pending Candidates",
            disabled=not bulk_targets,
            type="primary",
            use_container_width=True,
            key="bulk_send_btn",
            help="Sends JD + meeting link to all filtered candidates who haven't been contacted yet.",
        )

    if bulk_clicked and bulk_targets:
        ids = [c["id"] for c in bulk_targets]
        with st.spinner(f"Sending emails to {len(ids)} candidates…"):
            result, err = _api(
                api_url,
                "post",
                "/api/v1/send-outreach-bulk",
                json={
                    "candidate_ids": ids,
                    "meeting_link": meeting_link,
                    "custom_message": custom_msg or "",
                },
            )
        if err:
            st.error(f"❌ Bulk send failed: {err}")
        else:
            st.success(f"✅ Sent: {result['sent']}  |  ❌ Failed: {result['failed']}")
            for r in result["results"]:
                if r["status"] != "sent":
                    st.warning(
                        f"⚠ {r.get('name', '?')} ({r.get('email', '?')}): {r['message']}"
                    )
            st.rerun()

    st.markdown('<div class="rs-divider"></div>', unsafe_allow_html=True)

    # ── Candidate rows ────────────────────────────────────────────────────────
    st.markdown(
        f'<div style="font-size:0.75rem;color:#6b7280;font-weight:600;'
        f'text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.8rem;">'
        f"Showing {total} candidate(s)</div>",
        unsafe_allow_html=True,
    )

    for c in candidates:
        score = c.get("match_score", 0)
        name = c.get("full_name") or "Unknown"
        email = c.get("email") or ""
        phone = c.get("phone") or "—"
        role = c.get("current_title") or "—"
        org = c.get("current_company") or "—"
        exp = c.get("total_experience") or 0
        jd_title = c.get("role_name") or "—"
        status = c.get("outreach_sent")
        cid = c["id"]

        row_col1, row_col2, row_col3 = st.columns([3, 2, 1.2])

        with row_col1:
            st.markdown(
                f"""
            <div class="rs-card" style="padding:1rem 1.2rem;margin-bottom:0.5rem;">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div>
                        <div style="font-size:1rem;font-weight:600;color:#e8eaf2;">{name}</div>
                        <div style="font-size:0.8rem;color:#9ca3af;margin-top:2px;">{role} @ {org}</div>
                        <div style="font-size:0.78rem;color:#6b7280;margin-top:4px;">
                            📧 {email or '<em style="color:#ef4444">No email</em>'}
                            &nbsp;&nbsp;📱 {phone}
                            &nbsp;&nbsp;⏱ {exp} yrs
                        </div>
                        <div style="font-size:0.72rem;color:#4b5563;margin-top:6px;">JD: {jd_title}</div>
                    </div>
                    <div style="text-align:right;min-width:70px;">
                        <div style="font-size:2rem;font-weight:700;color:{_score_color(score)};">{score}</div>
                        <div style="font-size:0.62rem;color:#4b5563;text-transform:uppercase;">score</div>
                    </div>
                </div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with row_col2:
            st.markdown(
                f"""
            <div style="padding:1rem 0;">
                <div style="margin-bottom:0.5rem;">{_status_badge("sent" if status == 1 else "pending")}</div>
                {"<div style='font-size:0.75rem;color:#4b5563;margin-top:4px;'>Sent: " + c["outreach_sent_at"][:16].replace("T", " ") + "</div>" if c.get("outreach_sent_at") else ""}
                {"<div style='font-size:0.73rem;color:#818cf8;margin-top:4px;'>🔗 " + c["meeting_link"][:40] + "…</div>" if c.get("meeting_link") else ""}
            </div>
            """,
                unsafe_allow_html=True,
            )

        with row_col3:
            already_sent = status == 1
            btn_label = (
                "✉ Re-send" if already_sent else ("✉ Send" if email else "No Email")
            )
            btn_disabled = not bool(email)
            btn_key = f"send_btn_{cid}"

            if st.button(
                btn_label,
                key=btn_key,
                disabled=btn_disabled,
                use_container_width=True,
                help=f"Send JD + meeting link to {email}"
                if email
                else "No email address found for this candidate",
            ):
                with st.spinner(f"Sending to {email}…"):
                    result, err = _api(
                        api_url,
                        "post",
                        "/api/v1/send-outreach",
                        json={
                            "candidate_id": cid,
                            "meeting_link": meeting_link,
                            "custom_message": custom_msg or "",
                        },
                    )
                if err:
                    st.error(f"❌ {err}")
                else:
                    st.success(f"✅ Email sent to {email}")
                    st.rerun()

    st.markdown("")

    # ── SMTP config reminder ──────────────────────────────────────────────────
    with st.expander("⚙️  SMTP Configuration Reference", expanded=False):
        st.markdown("""
Add these to your `.env` file to enable email sending:

```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your@gmail.com
SMTP_PASSWORD=your_app_password          # Gmail App Password (not your login password)
SMTP_FROM_NAME=HR Team

MEETING_LINK=https://calendly.com/your-link   # Default meeting link (overridable above)
```

**Gmail setup:** Go to Google Account → Security → 2-Step Verification → App Passwords → Generate one for "Mail".
        """)
