#!/usr/bin/env python3
"""
Weekly email digest for USG Jobs.

Reads alert_preferences from Supabase, matches against jobs posted in the
last 7 days, and sends a personalised digest email via the Resend API.

Required environment variables:
  SUPABASE_URL          — your project URL  (e.g. https://xxxx.supabase.co)
  SUPABASE_SERVICE_KEY  — service-role key  (bypasses RLS to read all prefs)
  RESEND_API_KEY        — key from resend.com
  FROM_EMAIL            — verified sender address (e.g. digest@usgjobs.com)

Run:
  python3 scrapers/email_digest.py

GitHub Actions cron: see .github/workflows/weekly_digest.yml
"""

import json, os, sys, re, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY", "")
RESEND_KEY   = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL   = os.environ.get("FROM_EMAIL", "digest@usgjobs.com")
SITE_URL     = os.environ.get("SITE_URL", "https://carolinasalge.github.io/usg-jobs/")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

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

# ── Load recent jobs ──────────────────────────────────────────────────────────
jobs_path = Path(__file__).parent.parent / "jobs.json"
if not jobs_path.exists():
    print(f"ERROR: jobs.json not found at {jobs_path}")
    sys.exit(1)

all_jobs   = json.loads(jobs_path.read_text()).get("jobs", [])
cutoff     = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
cutoff_str = cutoff.date().isoformat()          # "YYYY-MM-DD"

recent_jobs = [j for j in all_jobs if (j.get("posted") or "") >= cutoff_str]
print(f"Jobs posted in the last {LOOKBACK_DAYS} days: {len(recent_jobs)} / {len(all_jobs)} total")

if not recent_jobs:
    print("No recent jobs — skipping digest.")
    sys.exit(0)

# ── Load alert preferences from Supabase ─────────────────────────────────────
try:
    prefs = sb_get("alert_preferences", "active=eq.true&select=user_id,keywords,min_salary,job_type")
except Exception as e:
    print(f"ERROR fetching alert_preferences: {e}")
    sys.exit(1)

print(f"Active alert subscriptions: {len(prefs)}")
if not prefs:
    print("No active subscribers — done.")
    sys.exit(0)

# Fetch email addresses for each user_id from auth.users via profiles table
user_ids = list({p["user_id"] for p in prefs})
try:
    # Fetch from auth admin endpoint to get emails
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

# Fetch subscription tiers from profiles table to determine daily vs weekly
try:
    id_list = ",".join(user_ids)
    profiles = sb_get("profiles", f"id=in.({id_list})&select=id,subscription_status,subscription_tier")
    user_tier_map = {}
    for p in profiles:
        status = p.get("subscription_status") or ""
        tier   = p.get("subscription_tier") or ""
        # Pro users (active subscription) get daily; everyone else gets weekly
        user_tier_map[p["id"]] = "pro" if status == "active" else "free"
except Exception as e:
    print(f"WARNING: Could not fetch subscription tiers: {e}")
    user_tier_map = {}

# Pre-compute cutoffs for each frequency
now_utc   = datetime.now(timezone.utc)
is_monday = now_utc.weekday() == 0   # 0 = Monday
cutoff_daily  = now_utc - timedelta(days=1)
cutoff_weekly = now_utc - timedelta(days=7)
print(f"Today is {'Monday' if is_monday else 'not Monday'} — free users {'will' if is_monday else 'will NOT'} receive digest today")

# ── Filter jobs per user and send ─────────────────────────────────────────────
sent = skipped = errors = 0

for pref in prefs:
    uid       = pref["user_id"]
    email     = user_email_map.get(uid)
    if not email:
        skipped += 1
        continue

    # Determine frequency based on subscription tier
    user_tier   = user_tier_map.get(uid, "free")
    is_pro      = user_tier == "pro"
    freq_label  = "daily" if is_pro else "weekly"

    # Free users only receive on Mondays; pro users receive every day
    if not is_pro and not is_monday:
        skipped += 1
        continue

    user_cutoff = cutoff_daily if is_pro else cutoff_weekly
    user_cutoff_str = user_cutoff.date().isoformat()

    # Jobs within this user's lookback window
    user_recent = [j for j in all_jobs if (j.get("posted") or "") >= user_cutoff_str]

    keywords  = [k.strip().lower() for k in re.split(r"[,\s]+", pref.get("keywords") or "") if k.strip()]
    min_sal   = int(pref.get("min_salary") or 0)
    job_type  = (pref.get("job_type") or "").strip().lower()

    def matches(job):
        # Keyword filter — match title, institution, department, summary
        if keywords:
            haystack = " ".join([
                job.get("title", ""), job.get("institution", ""),
                job.get("department", ""), job.get("summary", ""),
            ]).lower()
            if not any(kw in haystack for kw in keywords):
                return False
        # Minimum salary filter (rough: look for first dollar amount)
        if min_sal:
            nums = re.findall(r"\$\s*([\d,]+)", job.get("salary") or "")
            if nums:
                top = max(float(n.replace(",", "")) for n in nums)
                if top < min_sal:
                    return False
        # Job type filter
        if job_type and job_type != "any":
            if job_type not in (job.get("type") or "").lower():
                return False
        return True

    matched = [j for j in user_recent if matches(j)]
    if not matched:
        skipped += 1
        continue

    # ── Build HTML email ─────────────────────────────────────────────────────
    subject = f"USG Jobs {'Daily' if is_pro else 'Weekly'} Digest — {len(matched)} new {'job' if len(matched) == 1 else 'jobs'} matching your alerts"

    def job_card_html(j):
        sal   = f" &nbsp;·&nbsp; {j['salary']}" if j.get("salary") else ""
        dept  = f"<br><span style='color:#6b7280;font-size:13px;'>{j.get('department','')}</span>" if j.get("department") else ""
        closes = f"&nbsp; <span style='color:#dc2626;font-size:12px;'>Closes {j['closes']}</span>" if j.get("closes") else ""
        url   = j.get("view") or j.get("apply") or SITE_URL
        return f"""
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;margin-bottom:12px;background:#fff;">
          <a href="{url}" style="font-size:16px;font-weight:600;color:#1d4ed8;text-decoration:none;">{j.get('title','Untitled')}</a>
          <div style="margin-top:4px;color:#374151;font-size:13px;">
            {j.get('institution','')}{sal}{dept}
          </div>
          <div style="margin-top:6px;color:#6b7280;font-size:12px;">
            Posted {j.get('posted','')}{closes}
            &nbsp;·&nbsp; {j.get('location','') or j.get('type','')}
          </div>
          {f'<p style="margin:8px 0 0;color:#4b5563;font-size:13px;line-height:1.5;">{j["summary"][:200]}…</p>' if j.get("summary") else ""}
          <a href="{url}" style="display:inline-block;margin-top:10px;padding:6px 14px;background:#1d4ed8;color:#fff;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none;">View &amp; Apply →</a>
        </div>"""

    cards_html = "\n".join(job_card_html(j) for j in matched[:20])  # cap at 20 per email
    more_note  = f"<p style='color:#6b7280;font-size:13px;text-align:center;'>…and {len(matched)-20} more. <a href='{SITE_URL}'>See all on USG Jobs →</a></p>" if len(matched) > 20 else ""

    kw_label  = ", ".join(keywords) if keywords else "all jobs"
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:600px;margin:32px auto;background:#f3f4f6;">

    <!-- Header -->
    <div style="background:#1d4ed8;padding:24px 28px;border-radius:10px 10px 0 0;">
      <a href="{SITE_URL}" style="font-size:20px;font-weight:700;color:#fff;text-decoration:none;">USG Jobs</a>
      <p style="margin:6px 0 0;color:#bfdbfe;font-size:14px;">Your {freq_label} job digest</p>
    </div>

    <!-- Body -->
    <div style="background:#fff;padding:24px 28px;">
      <p style="margin:0 0 18px;color:#111827;font-size:15px;">
        We found <strong>{len(matched)} new {'job' if len(matched)==1 else 'jobs'}</strong>
        matching <em>{kw_label}</em> posted in the last {LOOKBACK_DAYS} days:
      </p>
      {cards_html}
      {more_note}
    </div>

    <!-- Footer -->
    <div style="padding:18px 28px;text-align:center;color:#9ca3af;font-size:12px;">
      You're receiving this because you set up job alerts on
      <a href="{SITE_URL}" style="color:#6b7280;">USG Jobs</a>.<br>
      <a href="{SITE_URL}" style="color:#6b7280;">Update your alert preferences</a>
      &nbsp;·&nbsp;
      <a href="{SITE_URL}" style="color:#6b7280;">Unsubscribe</a>
    </div>

  </div>
</body>
</html>"""

    ok, result = resend_send(email, subject, html)
    if ok:
        sent += 1
        print(f"  ✓ Sent to {email}  ({len(matched)} jobs)")
    else:
        errors += 1
        print(f"  ✗ Failed for {email}: {result}")

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\nDone. Sent: {sent}  Skipped (no matches / no email): {skipped}  Errors: {errors}")
