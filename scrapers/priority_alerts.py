#!/usr/bin/env python3
"""
Priority alert emails for Pro subscribers.

Runs immediately after the daily scrape and sends same-day email
notifications to Pro users whose alert keywords match jobs posted today.
Basic users only receive the weekly digest; they are skipped here.

Required environment variables:
  SUPABASE_URL          — your project URL  (e.g. https://xxxx.supabase.co)
  SUPABASE_SERVICE_KEY  — service-role key  (bypasses RLS to read all prefs)
  RESEND_API_KEY        — key from resend.com
  FROM_EMAIL            — verified sender address (e.g. alerts@usgjobs.com)
  SITE_URL              — public website URL

Run:
  python3 scrapers/priority_alerts.py

GitHub Actions: see .github/workflows/update_jobs.yml (runs after deploy)
"""

import json, os, sys, re, urllib.request, urllib.error
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY", "")
RESEND_KEY   = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL   = os.environ.get("FROM_EMAIL", "alerts@usgjobs.com")
SITE_URL     = os.environ.get("SITE_URL", "https://carolinasalge.github.io/usg-jobs/").rstrip("/")

def _require(name, val):
    if not val:
        print(f"ERROR: {name} environment variable is not set.")
        sys.exit(1)

_require("SUPABASE_URL",         SUPABASE_URL)
_require("SUPABASE_SERVICE_KEY", SERVICE_KEY)
_require("RESEND_API_KEY",       RESEND_KEY)

# ── Helpers ───────────────────────────────────────────────────────────────────
def sb_get(path, params=""):
    """GET from Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{path}{'?' + params if params else ''}"
    req = urllib.request.Request(url, headers={
        "apikey":        SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=representation",
    })
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())

def resend_send(to_email, subject, html_body):
    """Send one email via Resend REST API."""
    payload = json.dumps({
        "from":    FROM_EMAIL,
        "to":      [to_email],
        "subject": subject,
        "html":    html_body,
    }).encode()
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=payload,
        headers={
            "Authorization": f"Bearer {RESEND_KEY}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return True, json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return False, body

# ── Load today's jobs ─────────────────────────────────────────────────────────
jobs_path = Path(__file__).parent.parent / "jobs.json"
if not jobs_path.exists():
    print(f"ERROR: jobs.json not found at {jobs_path}")
    sys.exit(1)

all_jobs   = json.loads(jobs_path.read_text()).get("jobs", [])
today_str  = datetime.now(timezone.utc).date().isoformat()   # "YYYY-MM-DD"

today_jobs = [j for j in all_jobs if (j.get("posted") or "") == today_str]
print(f"Jobs posted today ({today_str}): {len(today_jobs)} / {len(all_jobs)} total")

if not today_jobs:
    print("No jobs posted today — skipping priority alerts.")
    sys.exit(0)

# ── Load Pro subscribers with active alerts ───────────────────────────────────
# profiles table must have a `plan` column with value 'pro' for Pro users.
# alert_preferences table: user_id, keywords, min_salary, job_type, active
try:
    prefs = sb_get(
        "alert_preferences",
        "active=eq.true&select=user_id,keywords,min_salary,job_type"
    )
except Exception as e:
    print(f"ERROR fetching alert_preferences: {e}")
    sys.exit(1)

print(f"Active alert subscriptions: {len(prefs)}")
if not prefs:
    print("No active subscribers — done.")
    sys.exit(0)

# Fetch Pro user IDs from profiles table
try:
    pro_profiles = sb_get("profiles", "plan=eq.pro&select=id")
    pro_user_ids = {p["id"] for p in pro_profiles}
    print(f"Pro users: {len(pro_user_ids)}")
except Exception as e:
    print(f"WARNING: Could not fetch Pro profiles: {e}")
    pro_user_ids = set()

# Filter prefs to Pro subscribers only
pro_prefs = [p for p in prefs if p["user_id"] in pro_user_ids]
print(f"Pro subscribers with active alerts: {len(pro_prefs)}")

if not pro_prefs:
    print("No Pro subscribers with active alerts — done.")
    sys.exit(0)

# Fetch email addresses for Pro subscribers
try:
    url = f"{SUPABASE_URL}/auth/v1/admin/users?per_page=1000"
    req = urllib.request.Request(url, headers={
        "apikey":        SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
    })
    with urllib.request.urlopen(req, timeout=20) as r:
        auth_data = json.loads(r.read())
    user_email_map = {u["id"]: u["email"] for u in auth_data.get("users", [])}
except Exception as e:
    print(f"WARNING: Could not fetch user emails: {e}")
    user_email_map = {}

# ── Filter today's jobs per user and send ─────────────────────────────────────
sent = skipped = errors = 0

for pref in pro_prefs:
    uid   = pref["user_id"]
    email = user_email_map.get(uid)
    if not email:
        skipped += 1
        continue

    keywords = [k.strip().lower() for k in re.split(r"[,\s]+", pref.get("keywords") or "") if k.strip()]
    min_sal  = int(pref.get("min_salary") or 0)
    job_type = (pref.get("job_type") or "").strip().lower()

    def matches(job):
        if keywords:
            haystack = " ".join([
                job.get("title", ""), job.get("institution", ""),
                job.get("department", ""), job.get("summary", ""),
            ]).lower()
            if not any(kw in haystack for kw in keywords):
                return False
        if min_sal:
            nums = re.findall(r"\$\s*([\d,]+)", job.get("salary") or "")
            if nums:
                top = max(float(n.replace(",", "")) for n in nums)
                if top < min_sal:
                    return False
        if job_type and job_type != "any":
            if job_type not in (job.get("type") or "").lower():
                return False
        return True

    matched = [j for j in today_jobs if matches(j)]
    if not matched:
        skipped += 1
        continue

    # ── Build HTML alert email ────────────────────────────────────────────────
    count   = len(matched)
    subject = f"🔔 {count} new USG {'job' if count == 1 else 'jobs'} matching your alerts today"

    def job_card_html(j):
        sal    = f" &nbsp;·&nbsp; {j['salary']}" if j.get("salary") else ""
        dept   = f"<br><span style='color:#6b7280;font-size:13px;'>{j.get('department','')}</span>" if j.get("department") else ""
        closes = f"&nbsp; <span style='color:#dc2626;font-size:12px;'>Closes {j['closes']}</span>" if j.get("closes") else ""
        url    = j.get("view") or j.get("apply") or SITE_URL
        return f"""
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;margin-bottom:12px;background:#fff;">
          <a href="{url}" style="font-size:16px;font-weight:600;color:#1d4ed8;text-decoration:none;">{j.get('title','Untitled')}</a>
          <div style="margin-top:4px;color:#374151;font-size:13px;">
            {j.get('institution','')}{sal}{dept}
          </div>
          <div style="margin-top:6px;color:#6b7280;font-size:12px;">
            Posted today{closes}
            &nbsp;·&nbsp; {j.get('location','') or j.get('type','')}
          </div>
          {f'<p style="margin:8px 0 0;color:#4b5563;font-size:13px;line-height:1.5;">{j["summary"][:200]}…</p>' if j.get("summary") else ""}
          <a href="{url}" style="display:inline-block;margin-top:10px;padding:6px 14px;background:#1d4ed8;color:#fff;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none;">View &amp; Apply →</a>
        </div>"""

    cards_html = "\n".join(job_card_html(j) for j in matched[:10])  # cap at 10 for alerts
    more_note  = (
        f"<p style='color:#6b7280;font-size:13px;text-align:center;'>"
        f"…and {count - 10} more. <a href='{SITE_URL}'>See all on USG Jobs →</a></p>"
    ) if count > 10 else ""

    kw_label = ", ".join(keywords) if keywords else "all jobs"

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:600px;margin:32px auto;background:#f3f4f6;">

    <!-- Header -->
    <div style="background:#1d4ed8;padding:24px 28px;border-radius:10px 10px 0 0;">
      <a href="{SITE_URL}" style="font-size:20px;font-weight:700;color:#fff;text-decoration:none;">USG Jobs</a>
      <p style="margin:6px 0 0;color:#bfdbfe;font-size:14px;">
        ⚡ Same-day alert &nbsp;·&nbsp; Pro feature
      </p>
    </div>

    <!-- Body -->
    <div style="background:#fff;padding:24px 28px;">
      <p style="margin:0 0 6px;color:#111827;font-size:15px;">
        <strong>{count} new {'job' if count == 1 else 'jobs'}</strong> matching
        <em>{kw_label}</em> {'was' if count == 1 else 'were'} posted today:
      </p>
      <p style="margin:0 0 18px;color:#6b7280;font-size:13px;">
        You're receiving this because you're a Pro subscriber with active job alerts.
      </p>
      {cards_html}
      {more_note}
    </div>

    <!-- Footer -->
    <div style="padding:18px 28px;text-align:center;color:#9ca3af;font-size:12px;">
      <a href="{SITE_URL}" style="color:#6b7280;">Update alert preferences</a>
      &nbsp;·&nbsp;
      <a href="{SITE_URL}" style="color:#6b7280;">Unsubscribe</a>
    </div>

  </div>
</body>
</html>"""

    ok, result = resend_send(email, subject, html)
    if ok:
        sent += 1
        print(f"  ✓ Sent to {email}  ({count} jobs)")
    else:
        errors += 1
        print(f"  ✗ Failed for {email}: {result}")

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\nDone. Sent: {sent}  Skipped (no matches / no email / Basic plan): {skipped}  Errors: {errors}")
