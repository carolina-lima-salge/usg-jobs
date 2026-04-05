"""
GA Tech Careers Scraper  —  OneHCM Portal  (SiteId=03000)
==========================================================
Targets Georgia Institute of Technology jobs on the USG OneHCM portal
via the GA Tech site entry point:

    https://careers.hprod.onehcm.usg.edu/psc/careers/CAREERS/HRMS/c/
    HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL?FOCUS=Applicant&SiteId=03000

SiteId=03000 is Georgia Tech's portal site ID — navigating to it
pre-loads the GA Tech context.  We then click "View All Jobs" and
scroll-load all job rows, exactly as scraper_onehcm.py does for other
USG institutions.

Strategy:
  ┌─ Phase 1 – Listing ────────────────────────────────────────┐
  │  1. Navigate to the GA Tech site entry URL (SiteId=03000). │
  │  2. Click "View All Jobs" to trigger the full job list.    │
  │  3. Scroll the lazy grid to load all rows.                 │
  └────────────────────────────────────────────────────────────┘
  ┌─ Phase 2 – Detail ─────────────────────────────────────────┐
  │  Fetch each job's detail page (requests + Playwright       │
  │  fallback).  Uses SiteId=03000 in all detail URLs.         │
  └────────────────────────────────────────────────────────────┘

Usage:
    python3 scraper_gatech.py                         # full run
    python3 scraper_gatech.py --listing-only          # listing only
    python3 scraper_gatech.py --details-from-listing  # details from JSON
    python3 scraper_gatech.py --debug                 # verbose

Output: gatech_jobs.csv / gatech_jobs.json
        (same column schema as onehcm_jobs.csv; merge_all.py reads gatech_jobs.csv)

Requirements:
    pip3 install playwright requests beautifulsoup4 lxml --break-system-packages
    playwright install chromium
"""

import csv, json, re, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock

import requests
from bs4 import BeautifulSoup

# ─── Config ──────────────────────────────────────────────────────────────────

BASE     = "https://careers.hprod.onehcm.usg.edu"
SITE_ID  = "03000"   # Georgia Institute of Technology portal site ID

# Entry point: opens GA Tech's portal context directly — no filter click needed
ENTRY_URL = (
    f"{BASE}/psc/careers/CAREERS/HRMS/c/"
    f"HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL"
    f"?FOCUS=Applicant&SiteId={SITE_ID}"
)

# After entry, "View All Jobs" navigates to the full job listing
SEARCH_URL = (
    f"{BASE}/psc/careers/CAREERS/HRMS/c/"
    "HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL"
    "?Page=HRS_APP_SCHJOB_FL&Action=U"
    f"&FOCUS=Applicant&SiteId={SITE_ID}"
)

DETAIL_URL_TEMPLATE = (
    f"{BASE}/psc/careers/CAREERS/HRMS/c/"
    "HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL"
    "?Page=HRS_APP_JBPST_FL&Action=U"
    f"&FOCUS=Applicant&SiteId={SITE_ID}"
    "&JobOpeningId={job_id}&PostingSeq=1"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}

SCROLL_PAUSE          = 2.5
DETAIL_DELAY          = 0.5
MAX_SCROLL_ATTEMPTS   = 60
DETAIL_WORKERS        = 3
DETAIL_RETRIES        = 2
DETAIL_FUTURE_TIMEOUT = 90
CHECKPOINT_EVERY      = 50

JSON_OUTPUT = "gatech_jobs.json"
CSV_OUTPUT  = "gatech_jobs.csv"
UTC   = timezone.utc
DEBUG        = "--debug" in sys.argv
FULL_REFRESH = "--full"  in sys.argv  # re-fetch every job; default is incremental

# ─── CSV columns (same as onehcm_jobs.csv for easy merging) ──────────────────

CSV_COLUMNS = [
    "job_id", "job_title", "institution", "department",
    "location", "posted_date", "full_part_time", "regular_temporary",
    "salary", "location_detail", "about_us", "job_summary", "responsibilities",
    "required_qualifications", "preferred_qualifications",
    "knowledge_skills_abilities", "other_information", "background_check",
    "extra_sections", "apply_link", "posting_url", "scraped_at",
]

SECTION_MAP = {
    "about us":                        "about_us",
    "job summary":                     "job_summary",
    "responsibilities":                "responsibilities",
    "required qualifications":         "required_qualifications",
    "preferred qualifications":        "preferred_qualifications",
    "knowledge, skills, & abilities":  "knowledge_skills_abilities",
    "knowledge, skills, abilities":    "knowledge_skills_abilities",
    "knowledge, skills and abilities": "knowledge_skills_abilities",
    "other information":               "other_information",
    "background check":                "background_check",
    # Salary labels used by various USG institutions
    "proposed salary":                 "salary",
    "salary range":                    "salary",
    "salary":                          "salary",
    "pay range":                       "salary",
    "shift/salary/benefits":           "salary",
    "compensation":                    "salary",
}
SKIP_SECTIONS = {"location", "usg core values", "equal employment opportunity"}

# ─── Fuzzy section-label patterns ─────────────────────────────────────────────
# Identical pattern list to scraper_onehcm.py — same OneHCM portal, same variants.
_SECTION_PATTERNS: list[tuple] = [
    (re.compile(r'(?:job|role|position)\s+(?:summary|description|overview)|'
                r'^overview$|^description$|^summary$', re.I),                   "job_summary"),
    (re.compile(r'(?:essential\s+)?(?:duties|responsibilities|functions)|'
                r'key\s+responsibilities|primary\s+duties', re.I),              "responsibilities"),
    (re.compile(r'(?:minimum|required)\s+(?:qualifications?|requirements?)|'
                r'^qualifications?$|^requirements?$', re.I),                    "required_qualifications"),
    (re.compile(r'preferred\s+(?:qualifications?|requirements?|experience)',
                re.I),                                                           "preferred_qualifications"),
    (re.compile(r'knowledge[,\s]+skills?[,\s&/]+abilit|ksa\b', re.I),          "knowledge_skills_abilities"),
    (re.compile(r'about\s+(?:us|the\s+(?:university|institution|department|'
                r'college|unit|school))', re.I),                                "about_us"),
    # Salary / pay / compensation
    (re.compile(r'salary(?:\s+range)?|advertised\s+salary|proposed\s+salary|'
                r'pay\s+(?:rate|range|grade|scale)|compensation(?:\s+range)?|'
                r'wage(?:s)?\b|shift/salary', re.I),                            "salary"),
    (re.compile(r'background\s+(?:check|investigation|screening)', re.I),       "background_check"),
    (re.compile(r'other\s+information|additional\s+information|'
                r'^benefits?$|supplemental\s+information', re.I),               "other_information"),
]


def resolve_section(raw_label: str):
    """Map a section label to a CSV column.  Exact SECTION_MAP first, then regex."""
    key = raw_label.strip().lower()
    if key in SECTION_MAP:
        return SECTION_MAP[key]
    for pat, field in _SECTION_PATTERNS:
        if pat.search(key):
            return field
    return None


# ─── Helpers ─────────────────────────────────────────────────────────────────

def clean(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()

def span_text(soup, span_id) -> str:
    el = soup.find("span", id=span_id)
    return clean(el.get_text(separator=" ")) if el else ""

# ─── Listing parser ───────────────────────────────────────────────────────────

def extract_listing_rows(soup) -> list[dict]:
    rows, i = [], 0
    while True:
        title = soup.find("span", id=f"SCH_JOB_TITLE${i}")
        if not title:
            break
        rows.append({
            "job_id":      clean(span_text(soup, f"HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID${i}")),
            "job_title":   clean(title.get_text(separator=" ")),
            "institution": clean(span_text(soup, f"HRS_BU_DESCR${i}")),
            "department":  clean(span_text(soup, f"HRS_APP_JBSCH_I_HRS_DEPT_DESCR${i}")),
            "location":    clean(span_text(soup, f"LOCATION${i}")),
            "posted_date": clean(span_text(soup, f"SCH_OPENED${i}")),
        })
        i += 1
    return rows

# ─── Detail parser ────────────────────────────────────────────────────────────

def parse_detail(html: str, job_id: str, card: dict) -> dict:
    soup = BeautifulSoup(html, "lxml")
    job  = {k: "" for k in CSV_COLUMNS}
    job.update({
        "job_id":       card.get("job_id", job_id),
        "job_title":    card.get("job_title", ""),
        "institution":  card.get("institution", "Georgia Institute of Technology"),
        "department":   card.get("department", ""),
        "location":     card.get("location", ""),
        "posted_date":  card.get("posted_date", ""),
        "posting_url":  DETAIL_URL_TEMPLATE.format(job_id=job_id),
        "apply_link":   DETAIL_URL_TEMPLATE.format(job_id=job_id),
        "scraped_at":   datetime.now(UTC).isoformat(),
    })
    title_val = span_text(soup, "HRS_SCH_WRK2_POSTING_TITLE")
    if title_val: job["job_title"] = title_val
    jid = span_text(soup, "HRS_SCH_WRK2_HRS_JOB_OPENING_ID")
    if jid: job["job_id"] = jid
    job["location_detail"]   = span_text(soup, "HRS_SCH_WRK_HRS_DESCRLONG")
    job["full_part_time"]    = span_text(soup, "HRS_SCH_WRK_HRS_FULL_PART_TIME")
    job["regular_temporary"] = span_text(soup, "HRS_SCH_WRK_HRS_REG_TEMP")

    extra_parts = []
    for i in range(30):
        lbl_el = soup.find("span", id=f"HRS_SCH_WRK_DESCR100${i}lbl")
        if not lbl_el: break
        label   = clean(lbl_el.get_text(separator=" "))
        lbl_key = label.lower()
        cnt_el  = soup.find("span", id=f"HRS_SCH_PSTDSC_DESCRLONG${i}")
        content = clean(cnt_el.get_text(separator=" ")) if cnt_el else ""
        if not content or lbl_key in SKIP_SECTIONS: continue
        col = resolve_section(lbl_key)
        if col:
            job[col] = content
        else:
            extra_parts.append(f"[{label}] {content}")
    if extra_parts:
        job["extra_sections"] = " || ".join(extra_parts)
    return job

# ─── PeopleSoft navigation helpers ───────────────────────────────────────────

def _wait_for_ajax(page, timeout_ms: int = 30_000):
    try:
        page.wait_for_function(
            "() => { const w=document.getElementById('WAIT_win0'); "
            "return w && w.style.display!=='none' && w.style.visibility!=='hidden'; }",
            timeout=5_000
        )
    except Exception:
        pass
    try:
        page.wait_for_function(
            "() => { "
            "const w=document.getElementById('WAIT_win0'); "
            "const p=document.getElementById('processing'); "
            "const wOk=!w||w.style.display==='none'||w.style.visibility==='hidden'; "
            "const pOk=!p||p.style.display==='none'||p.style.visibility==='hidden'; "
            "return wOk&&pOk; }",
            timeout=timeout_ms
        )
    except Exception:
        try:
            page.wait_for_timeout(3000)
        except Exception:
            pass


def _click_view_all_jobs(page) -> bool:
    """
    On the GA Tech entry page, find and click the "View All Jobs" button/link.
    Returns True if clicked, False if not found (may already be on job list).
    """
    # PeopleSoft renders "View All Jobs" with a few different patterns
    selectors = [
        "a:has-text('View All Jobs')",
        "button:has-text('View All Jobs')",
        "a:has-text('View all jobs')",
        "span:has-text('View All Jobs')",
        "[id*='VIEW_ALL']",
        "[id*='HRS_SCH_WRK_FLU_HRS_SEARCH_BTN']",   # Search/View All button
        "#HRS_SCH_WRK_FLU_HRS_SEARCH_BTN",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                print(f"  Found 'View All Jobs' via: {sel}", flush=True)
                loc.click()
                return True
        except Exception:
            pass

    # JS fallback — look for any link whose text mentions "view" and "job"
    try:
        result = page.evaluate("""
            () => {
                const RE = /view\\s+all\\s+jobs/i;
                for (const el of document.querySelectorAll('a, button, span, div')) {
                    const t = (el.innerText || el.textContent || '').trim();
                    if (RE.test(t) && t.length < 60) {
                        el.click();
                        return t;
                    }
                }
                return null;
            }
        """)
        if result:
            print(f"  Clicked via JS: {result!r}", flush=True)
            return True
    except Exception:
        pass

    return False


def scroll_to_load_all(page, expected_count: int, label: str) -> list[dict]:
    """Scroll the PeopleSoft lazy-load grid until all rows are collected."""
    seen_ids, all_rows = set(), []

    GRID_SEL = (
        ".ps_scrollable_v[id*='HRS_AGNT_RSLT_I'], "
        "[id*='HRS_AGNT_RSLT_I'][id*='grid'], "
        ".ps_box-grid-list"
    )

    def harvest(soup):
        added = 0
        for r in extract_listing_rows(soup):
            if r["job_id"] and r["job_id"] not in seen_ids:
                seen_ids.add(r["job_id"])
                all_rows.append(r)
                added += 1
        return added

    try:
        harvest(BeautifulSoup(page.content(), "lxml"))
        print(f"    Initial rows: {len(all_rows)} / {expected_count}", flush=True)
    except Exception as exc:
        print(f"    ⚠ Initial page.content() failed ({exc}); returning empty.", flush=True)
        return all_rows

    grid_box = None
    for sel in GRID_SEL.split(", "):
        try:
            locs = page.locator(sel.strip()).all()
        except Exception:
            locs = []
        if locs:
            try:
                grid_box = locs[0].bounding_box()
                if grid_box:
                    break
            except Exception:
                pass
    if not grid_box:
        vp = page.viewport_size or {"width": 1280, "height": 900}
        grid_box = {"x": vp["width"] * 0.6, "y": vp["height"] * 0.5,
                    "width": vp["width"] * 0.3, "height": vp["height"] * 0.4}

    cx = grid_box["x"] + grid_box["width"] / 2
    cy = grid_box["y"] + grid_box["height"] / 2
    try:
        page.mouse.move(cx, cy)
    except Exception:
        pass  # mouse positioning is best-effort

    stall_count = 0
    for attempt in range(MAX_SCROLL_ATTEMPTS):
        last_idx = max(0, len(all_rows) - 1)
        try:
            page.evaluate(
                f"document.querySelector('[id=\"HRS_AGNT_RSLT_I$0_row_{last_idx}\"]"
                f", #HRS_AGNT_RSLT_I\\\\$0_row_{last_idx}')?.scrollIntoView()"
            )
        except Exception:
            pass
        try:
            page.mouse.wheel(0, 600)
            page.wait_for_timeout(int(SCROLL_PAUSE * 1000))
            added = harvest(BeautifulSoup(page.content(), "lxml"))
        except Exception as exc:
            print(f"    ⚠ Scroll interrupted ({exc}); "
                  f"keeping {len(all_rows)} / {expected_count} rows collected so far.",
                  flush=True)
            break
        print(f"    Scroll {attempt+1}: +{added}  →  total {len(all_rows)} / {expected_count}",
              flush=True)

        if added == 0:
            stall_count += 1
            if stall_count >= 3:
                if len(all_rows) >= expected_count:
                    print(f"    ✓ All {expected_count} rows loaded.")
                else:
                    print(f"    Stalled at {len(all_rows)} (expected {expected_count}).")
                break
        else:
            stall_count = 0

    return all_rows


def _read_total_count(page) -> int:
    """Try to read the total job count shown on the listing page."""
    try:
        soup = BeautifulSoup(page.content(), "lxml")
    except Exception:
        return 500   # safe upper bound if page is unavailable
    for span in soup.find_all("span"):
        sid = span.get("id", "").upper()
        if "COUNT" in sid or "TOTAL" in sid or "RESULT" in sid:
            m = re.search(r"(\d+)", clean(span.get_text()))
            if m:
                return int(m.group(1))
    # Fallback: count job title spans already in DOM
    i = 0
    while soup.find("span", id=f"SCH_JOB_TITLE${i}"):
        i += 1
    return i if i > 0 else 500   # 500 as safe upper bound


def collect_gatech_jobs(page) -> list[dict]:
    """
    Full listing collection:
      1. Navigate to the GA Tech SiteId entry URL.
      2. Click "View All Jobs".
      3. Wait for job results to render.
      4. Scroll-load all rows.
    """
    print(f"\nStep 1 — Loading GA Tech entry URL (SiteId={SITE_ID}) …", flush=True)
    print(f"  {ENTRY_URL}")
    try:
        page.goto(ENTRY_URL, wait_until="load", timeout=90_000)
    except Exception as exc:
        print(f"  ⚠ Failed to load entry URL ({exc}); trying SEARCH_URL …", flush=True)
        try:
            page.goto(SEARCH_URL, wait_until="load", timeout=90_000)
        except Exception:
            print("  ⚠ Failed to load SEARCH_URL too; returning empty.", flush=True)
            return []
    try:
        page.wait_for_timeout(3000)
    except Exception:
        pass

    # Try to click "View All Jobs" from the entry/landing page
    print("\nStep 2 — Clicking 'View All Jobs' …", flush=True)
    clicked = _click_view_all_jobs(page)

    if clicked:
        _wait_for_ajax(page, timeout_ms=30_000)
        try:
            page.wait_for_selector('span[id^="SCH_JOB_TITLE"]', timeout=30_000)
        except Exception:
            pass
        try:
            page.wait_for_timeout(2000)
        except Exception:
            pass
    else:
        # Maybe we're already on the job listing (SEARCH_URL pattern)
        print("  'View All Jobs' not found — checking if job list is already showing …",
              flush=True)
        # Check if job titles are in the DOM already
        try:
            page.wait_for_selector('span[id^="SCH_JOB_TITLE"]', timeout=10_000)
            print("  Job list is already visible.", flush=True)
        except Exception:
            # Navigate directly to the search URL with SiteId
            print(f"  Navigating directly to SEARCH_URL …", flush=True)
            try:
                page.goto(SEARCH_URL, wait_until="load", timeout=90_000)
            except Exception:
                pass
            try:
                page.wait_for_selector('span[id^="SCH_JOB_TITLE"]', timeout=30_000)
            except Exception:
                pass
            try:
                page.wait_for_timeout(2000)
            except Exception:
                pass

    # Read total job count for progress display
    total = _read_total_count(page)
    print(f"\nStep 3 — Scrolling to collect all {total} GA Tech jobs …", flush=True)

    return scroll_to_load_all(page, total, "Georgia Institute of Technology")


# ─── Detail fetching ──────────────────────────────────────────────────────────

def _make_session(cookie_jar: dict) -> requests.Session:
    s = requests.Session()
    for name, value in cookie_jar.items():
        s.cookies.set(name, value)
    return s


def fetch_detail_requests(session: requests.Session, job_id: str) -> str | None:
    url = DETAIL_URL_TEMPLATE.format(job_id=job_id)
    for attempt in range(1, DETAIL_RETRIES + 2):
        try:
            r = session.get(url, headers=HEADERS, timeout=(10, 25))
            if r.status_code == 200 and len(r.text) > 10_000:
                if "HRS_SCH_WRK2_POSTING_TITLE" in r.text or "HRS_SCH_PSTDSC_DESCRLONG" in r.text:
                    return r.text
                if DEBUG: print(f"    {job_id}: no content spans (attempt {attempt})")
                break
            elif r.status_code == 429:
                time.sleep(5 * attempt)
            else:
                if DEBUG: print(f"    {job_id}: HTTP {r.status_code}")
                if attempt <= DETAIL_RETRIES:
                    time.sleep(2 * attempt)
        except Exception as e:
            if DEBUG: print(f"    {job_id} attempt {attempt}: {e}")
            if attempt <= DETAIL_RETRIES:
                time.sleep(2 * attempt)
    return None


def fetch_detail_playwright(page, job_id: str) -> str | None:
    url = DETAIL_URL_TEMPLATE.format(job_id=job_id)

    def _try_fetch() -> str | None:
        try:
            page.goto(url, wait_until="load", timeout=60_000)
            try:
                page.wait_for_selector(
                    "#HRS_SCH_WRK2_POSTING_TITLE, #HRS_SCH_PSTDSC_DESCRLONG\\$0",
                    timeout=20_000,
                )
            except Exception:
                page.wait_for_timeout(3000)
            html = page.content()
            if "HRS_SCH_WRK2_POSTING_TITLE" in html or "HRS_SCH_PSTDSC_DESCRLONG" in html:
                return html
        except Exception as e:
            if DEBUG: print(f"    playwright fetch error {job_id}: {e}")
        return None

    # First attempt
    html = _try_fetch()
    if html:
        return html

    # Session may have expired — navigate back to ENTRY_URL to refresh it, then retry
    if DEBUG: print(f"    {job_id}: refreshing session and retrying …")
    try:
        page.goto(ENTRY_URL, wait_until="load", timeout=60_000)
        page.wait_for_timeout(3000)
    except Exception as e:
        if DEBUG: print(f"    session refresh failed: {e}")
        return None

    return _try_fetch()


def _load_already_fetched(csv_path: str) -> set[str]:
    """Return all job_ids already in the output CSV (incremental: skip all existing)."""
    p = Path(csv_path)
    if not p.exists():
        return set()
    done = set()
    try:
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                jid = row.get("job_id", "").strip()
                if jid:
                    done.add(jid)
    except Exception:
        pass
    return done


def _checkpoint(all_jobs: list, csv_path: str, lock: RLock):
    with lock:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_jobs)


def fetch_all_details(listing_cards, cookie_jar, pw_page, output_csv, output_json):
    already_done = _load_already_fetched(output_csv)
    if FULL_REFRESH:
        print("  Full refresh mode (--full): ignoring existing CSV, re-fetching all jobs.")
        already_done = set()
    elif already_done:
        print(f"  Incremental: {len(already_done)} existing jobs will be kept — fetching new only.")

    prior_rows = {}
    if Path(output_csv).exists() and already_done:
        try:
            with open(output_csv, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    jid = row.get("job_id", "").strip()
                    if jid in already_done:
                        prior_rows[jid] = row
        except Exception:
            pass

    pending  = [c for c in listing_cards if c.get("job_id", "") not in already_done]
    all_jobs = list(prior_rows.values())
    pw_queue = []
    lock     = RLock()
    cp_count = [0]
    done     = [0]
    total    = len(pending)

    print(f"  {total} jobs to fetch  ({len(already_done)} already done)\n")

    def fetch_one(card):
        jid     = card.get("job_id", "")
        session = _make_session(cookie_jar)
        html    = fetch_detail_requests(session, jid)
        time.sleep(DETAIL_DELAY)
        return jid, html

    print(f"  Phase A — requests ({DETAIL_WORKERS} workers) …")
    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
        futures = {pool.submit(fetch_one, card): card for card in pending}
        for future in as_completed(futures):
            card   = futures[future]
            job_id = card.get("job_id", "")
            html   = None
            try:
                _, html = future.result(timeout=DETAIL_FUTURE_TIMEOUT)
            except TimeoutError:
                print(f"  [!] {job_id} timed out → Playwright", flush=True)
            except Exception as e:
                print(f"  [!] worker error {job_id}: {e}", flush=True)

            done[0] += 1
            n = done[0]

            if html:
                try:
                    job = parse_detail(html, job_id, card)
                except Exception as e:
                    print(f"  [{n}/{total}] ! parse error {job_id}: {e}", flush=True)
                    job = {k: "" for k in CSV_COLUMNS}
                    job.update(card)
                    job.setdefault("institution", "Georgia Institute of Technology")
                    job["posting_url"] = DETAIL_URL_TEMPLATE.format(job_id=job_id)
                    job["apply_link"]  = job["posting_url"]
                    job["scraped_at"]  = datetime.now(UTC).isoformat()
                print(f"  [{n}/{total}] ✓ {job_id}  "
                      f"quals={'YES' if job.get('required_qualifications') else '---'}",
                      flush=True)
                with lock:
                    all_jobs.append(job)
                    cp_count[0] += 1
                    if cp_count[0] % CHECKPOINT_EVERY == 0:
                        try:
                            _checkpoint(all_jobs, output_csv, lock)
                            print(f"    ── checkpoint ({len(all_jobs)} jobs) ──")
                        except Exception as e:
                            print(f"    !! checkpoint failed: {e}")
            else:
                print(f"  [{n}/{total}] ✗ {job_id} → Playwright", flush=True)
                with lock:
                    pw_queue.append(card)

    if pw_queue and pw_page is not None:
        print(f"\n  Phase B — Playwright fallback for {len(pw_queue)} jobs …")
        for i, card in enumerate(pw_queue, 1):
            job_id = card.get("job_id", "")
            print(f"  [PW {i}/{len(pw_queue)}] {job_id} …", end=" ", flush=True)
            html = fetch_detail_playwright(pw_page, job_id)
            job  = {k: "" for k in CSV_COLUMNS}
            job.update(card)
            job.setdefault("institution", "Georgia Institute of Technology")
            job["posting_url"] = DETAIL_URL_TEMPLATE.format(job_id=job_id)
            job["apply_link"]  = job["posting_url"]
            job["scraped_at"]  = datetime.now(UTC).isoformat()
            if html:
                try:
                    job = parse_detail(html, job_id, card)
                except Exception:
                    pass
                print(f"✓  quals={'YES' if job.get('required_qualifications') else '---'}")
            else:
                print("✗ FAILED — stub")
            all_jobs.append(job)
            cp_count[0] += 1
            if cp_count[0] % CHECKPOINT_EVERY == 0:
                _checkpoint(all_jobs, output_csv, lock)
            time.sleep(DETAIL_DELAY)

    elif pw_queue:
        for card in pw_queue:
            job_id = card.get("job_id", "")
            job    = {k: "" for k in CSV_COLUMNS}
            job.update(card)
            job.setdefault("institution", "Georgia Institute of Technology")
            job["posting_url"] = DETAIL_URL_TEMPLATE.format(job_id=job_id)
            job["apply_link"]  = job["posting_url"]
            job["scraped_at"]  = datetime.now(UTC).isoformat()
            all_jobs.append(job)

    return all_jobs


# ─── Save ─────────────────────────────────────────────────────────────────────

def _save(jobs, json_path, csv_path):
    Path(json_path).write_text(
        json.dumps(jobs, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(jobs)
    print(f"Saved {len(jobs)} records → {json_path}, {csv_path}")


# ─── Browser factory ─────────────────────────────────────────────────────────

def _make_browser(p):
    browser = p.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = browser.new_context(
        user_agent=HEADERS["User-Agent"],
        viewport={"width": 1280, "height": 900},
    )
    context.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )
    return browser, context


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    listing_only         = "--listing-only"         in sys.argv
    details_from_listing = "--details-from-listing" in sys.argv

    from playwright.sync_api import sync_playwright

    print("=" * 60)
    print("GA Tech Careers Scraper  —  OneHCM  (SiteId=03000)")
    print("=" * 60)
    print(f"Entry URL: {ENTRY_URL}\n")

    # ── Mode: --details-from-listing ─────────────────────────────────────────
    if details_from_listing:
        listing_src = "gatech_listings.json"
        if not Path(listing_src).exists():
            print(f"ERROR: {listing_src} not found. Run with --listing-only first.")
            sys.exit(1)
        listing_cards = json.loads(Path(listing_src).read_text(encoding="utf-8"))
        print(f"Loaded {len(listing_cards)} listing cards from {listing_src}")

        with sync_playwright() as p:
            browser, context = _make_browser(p)
            page = context.new_page()
            print("Warming up session …")
            page.goto(ENTRY_URL, wait_until="load", timeout=90_000)
            page.wait_for_timeout(3000)
            cookie_jar = {c["name"]: c["value"] for c in context.cookies()}
            all_jobs = fetch_all_details(listing_cards, cookie_jar, page,
                                         CSV_OUTPUT, JSON_OUTPUT)
            browser.close()

        _save(all_jobs, JSON_OUTPUT, CSV_OUTPUT)
        print(f"\nDone!  {len(all_jobs)} GA Tech jobs saved.")
        return

    # ── Normal mode ───────────────────────────────────────────────────────────
    with sync_playwright() as p:
        browser, context = _make_browser(p)
        page = context.new_page()

        # Phase 1: collect listing
        listing_cards = collect_gatech_jobs(page)

        print(f"\n{'='*60}")
        print(f"Listing complete: {len(listing_cards)} GA Tech jobs found")

        if listing_only:
            browser.close()
            _save(listing_cards, "gatech_listings.json",
                  "gatech_listings.csv")
            print("  →  gatech_listings.json")
            print("  →  gatech_listings.csv")
            return

        if not listing_cards:
            print("ERROR: No jobs found in listing phase. Aborting.")
            browser.close()
            sys.exit(1)

        # Phase 2: fetch details
        cookie_jar = {c["name"]: c["value"] for c in context.cookies()}
        print(f"\nFetching details for {len(listing_cards)} jobs …")
        print(f"Workers: {DETAIL_WORKERS}  |  Checkpoint every: {CHECKPOINT_EVERY}\n")

        all_jobs = fetch_all_details(listing_cards, cookie_jar, page,
                                     CSV_OUTPUT, JSON_OUTPUT)
        browser.close()

    _save(all_jobs, JSON_OUTPUT, CSV_OUTPUT)
    print(f"\n{'='*60}")
    print(f"Done!  {len(all_jobs)} GA Tech jobs saved.")
    print(f"  →  {JSON_OUTPUT}")
    print(f"  →  {CSV_OUTPUT}")


if __name__ == "__main__":
    main()
