"""
USG OneHCM Careers Scraper  —  careers.hprod.onehcm.usg.edu
=============================================================
Covers 23 USG institutions (all except UGA, GA Tech, Georgia State).

Strategy:
  ┌─ Phase 1 – Listing ────────────────────────────────────────┐
  │  Iterate through each of the 23 institution filter         │
  │  checkboxes one at a time.  For each institution:          │
  │    1. Click its filter → page reloads showing only that    │
  │       institution's jobs (no 999-cap since count < 999)    │
  │    2. Mouse-wheel-scroll the ps_scrollable grid to load    │
  │       all rows (50 per batch, lazy-loaded)                 │
  │    3. Extract all job cards                                 │
  │    4. Click "Clear Search" → reset for next institution    │
  └────────────────────────────────────────────────────────────┘
  ┌─ Phase 2 – Detail ─────────────────────────────────────────┐
  │  For each job ID construct the direct PeopleSoft URL and   │
  │  fetch with requests (sharing session cookies from the     │
  │  Playwright context).  Falls back to Playwright if the     │
  │  request-based fetch fails.                                │
  └────────────────────────────────────────────────────────────┘

Usage:
    python3 scraper_onehcm.py                # full run
    python3 scraper_onehcm.py --test         # parse saved onehcm_detail.html
    python3 scraper_onehcm.py --listing-only # listing cards only, no details
    python3 scraper_onehcm.py --debug        # verbose output

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

BASE = "https://careers.hprod.onehcm.usg.edu"

SEARCH_URL = (
    f"{BASE}/psc/careers/CAREERS/HRMS/c/"
    "HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL"
    "?Page=HRS_APP_SCHJOB_FL&Action=U"
)

DETAIL_URL_TEMPLATE = (
    f"{BASE}/psc/careers/CAREERS/HRMS/c/"
    "HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL"
    "?Page=HRS_APP_JBPST_FL&Action=U"
    "&FOCUS=Applicant&SiteId=1"
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

SCROLL_PAUSE   = 2.5   # seconds to wait after each scroll event
DETAIL_DELAY   = 0.5   # seconds between detail fetches (reduced; concurrent workers spread load)
MAX_SCROLL_ATTEMPTS = 40  # max scroll attempts per institution (safety limit)
DETAIL_WORKERS = 3     # parallel workers for requests-based detail fetches
DETAIL_RETRIES = 2     # retry count before handing off to Playwright
DETAIL_FUTURE_TIMEOUT = 90   # seconds before a stuck worker future is abandoned
CHECKPOINT_EVERY = 50  # save incremental CSV every N jobs

JSON_OUTPUT = "onehcm_jobs.json"
CSV_OUTPUT  = "onehcm_jobs.csv"
UTC = timezone.utc
DEBUG        = "--debug" in sys.argv
FULL_REFRESH = "--full"  in sys.argv  # re-fetch every job; default is incremental

# ─── CSV columns ─────────────────────────────────────────────────────────────

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

# ─── Helpers ─────────────────────────────────────────────────────────────────

def clean(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()

def span_text(soup, span_id) -> str:
    el = soup.find("span", id=span_id)
    return clean(el.get_text(separator=" ")) if el else ""

# ─── Listing parser ──────────────────────────────────────────────────────────

def extract_listing_rows(soup) -> list[dict]:
    """Extract all job rows currently visible in the page HTML."""
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

def count_rows(soup) -> int:
    i = 0
    while soup.find("span", id=f"SCH_JOB_TITLE${i}"):
        i += 1
    return i

# ─── Detail parser ───────────────────────────────────────────────────────────

def parse_detail(html: str, job_id: str, card: dict) -> dict:
    soup = BeautifulSoup(html, "lxml")
    job  = {k: "" for k in CSV_COLUMNS}
    job.update({
        "job_id":       card.get("job_id", job_id),
        "job_title":    card.get("job_title", ""),
        "institution":  card.get("institution", ""),
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
        col = SECTION_MAP.get(lbl_key)
        if col:
            job[col] = content
        else:
            extra_parts.append(f"[{label}] {content}")
    if extra_parts:
        job["extra_sections"] = " || ".join(extra_parts)
    return job

# ─── Playwright listing collection ───────────────────────────────────────────

def scroll_to_load_all(page, expected_count: int, inst_name: str) -> list[dict]:
    """
    Scroll the ps_scrollable grid with mouse-wheel events until no new rows
    appear.  Returns all collected job cards.
    """
    seen_ids, all_rows = set(), []

    # Locate the scrollable grid container
    GRID_SEL = (
        ".ps_scrollable_v[id*='HRS_AGNT_RSLT_I'], "
        "[id*='HRS_AGNT_RSLT_I'][id*='grid'], "
        ".ps_box-grid-list"
    )

    def harvest(soup):
        rows = extract_listing_rows(soup)
        added = 0
        for r in rows:
            if r["job_id"] and r["job_id"] not in seen_ids:
                seen_ids.add(r["job_id"])
                all_rows.append(r)
                added += 1
        return added

    # Initial harvest
    harvest(BeautifulSoup(page.content(), "lxml"))
    print(f"    Initial rows: {len(all_rows)} / {expected_count}", flush=True)

    # NOTE: do NOT early-exit here even if initial count >= expected.
    # The DOM may still contain stale rows from the previous institution;
    # we always scroll to confirm all rows are genuinely loaded.

    # Find grid bounding box to position the mouse over it
    grid_box = None
    for sel in GRID_SEL.split(", "):
        locs = page.locator(sel.strip()).all()
        if locs:
            try:
                grid_box = locs[0].bounding_box()
                if grid_box:
                    break
            except Exception:
                pass

    if not grid_box:
        # Fall back to center of viewport
        vp = page.viewport_size or {"width": 1280, "height": 900}
        grid_box = {"x": vp["width"] * 0.6, "y": vp["height"] * 0.5,
                    "width": vp["width"] * 0.3, "height": vp["height"] * 0.4}

    cx = grid_box["x"] + grid_box["width"] / 2
    cy = grid_box["y"] + grid_box["height"] / 2
    page.mouse.move(cx, cy)

    stall_count = 0
    for attempt in range(MAX_SCROLL_ATTEMPTS):
        # Scroll the last visible row into view first
        last_idx = len(all_rows) - 1
        try:
            page.evaluate(
                f"document.querySelector('[id=\"HRS_AGNT_RSLT_I$0_row_{last_idx}\"]"
                f", #HRS_AGNT_RSLT_I\\\\$0_row_{last_idx}')?.scrollIntoView()"
            )
        except Exception:
            pass

        # Also fire wheel events over the grid
        page.mouse.wheel(0, 600)
        page.wait_for_timeout(int(SCROLL_PAUSE * 1000))

        added = harvest(BeautifulSoup(page.content(), "lxml"))
        print(f"    Scroll {attempt+1}: +{added} rows  →  total {len(all_rows)} / {expected_count}",
              flush=True)

        if added == 0:
            stall_count += 1
            # Require 3 consecutive stalls to confirm all rows are loaded.
            # Also accept early finish if count matches expected.
            if stall_count >= 3:
                if len(all_rows) >= expected_count:
                    print(f"    ✓ All {expected_count} rows loaded.")
                else:
                    print(f"    Stalled at {len(all_rows)} rows (expected {expected_count}).")
                break
        else:
            stall_count = 0
            if len(all_rows) >= expected_count:
                # One more scroll to make sure there's truly nothing left
                if stall_count >= 1:
                    print(f"    ✓ All {expected_count} rows loaded.")
                    break

    return all_rows


def _expand_more_filters(page):
    """
    Click any 'More' expander links in the facet panel so that ALL institution
    filter checkboxes are present in the DOM.  Must be called after every
    page.goto(SEARCH_URL) because the page resets to its collapsed state on
    each full reload.
    """
    for more_sel in ["a[id='PTS_MORE$0']", "a[id^='PTS_MORE']"]:
        try:
            more_btns = page.locator(more_sel).all()
            for btn in more_btns:
                if btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(1000)
                    # Wait for the new filter labels to appear in the DOM
                    try:
                        page.wait_for_function(
                            """() => {
                                const w = document.getElementById('WAIT_win0');
                                const p = document.getElementById('processing');
                                const wOk = !w || w.style.display === 'none' || w.style.visibility === 'hidden';
                                const pOk = !p || p.style.display === 'none' || p.style.visibility === 'hidden';
                                return wOk && pOk;
                            }""",
                            timeout=15_000
                        )
                    except Exception:
                        page.wait_for_timeout(1500)
        except Exception:
            pass


def _discover_institutions(page) -> list[dict]:
    """
    Read institution filter labels from the current page HTML.
    Also clicks any 'More' expander so all institutions are visible.
    Returns list of {idx, name, count}.
    """
    # Expand 'More' facet sections so all institution labels are in the DOM
    _expand_more_filters(page)

    soup = BeautifulSoup(page.content(), "lxml")
    institutions = []
    i = 0
    while True:
        # Try both label and div for the institution name
        lbl = soup.find("label", id=f"PTS_SELECT_LBL${i}")
        if not lbl:
            lbl = soup.find("div", id=f"win0divPTS_SELECT${i}")
        if not lbl:
            break
        text  = clean(lbl.get_text())
        m     = re.search(r"\((\d+)\)$", text)
        count = int(m.group(1)) if m else 0
        name  = re.sub(r"\s*\(\d+\)$", "", text).strip()
        # Only collect institutions (first facet group — skip job-type filters)
        # Job-type facets appear after index 22; stop when name looks like a
        # category instead of a school name
        institutions.append({"idx": i, "name": name, "count": count})
        i += 1
        if i > 50:  # safety limit
            break
    # Keep only the Business Unit facet group (institution names, not job types)
    # The institution group ends when names start looking like job categories
    # (e.g., "Academic Faculty", "Full-Time").  Stop at first obvious category.
    JOB_CATEGORY_KEYWORDS = {
        "academic faculty", "academic services", "administration",
        "administrative", "athletics", "campus services", "clinical",
        "communications", "computer", "facilities", "faculty", "finance",
        "graduate assistant", "human resources", "juvenile", "legal",
        "library", "management", "marketing", "nursing", "operations",
        "plant", "public safety", "research", "services", "staff",
        "student", "temporary", "work-study", "full-time", "part-time",
        "regular", "no value",
    }
    filtered = []
    for inst in institutions:
        lower = inst["name"].lower()
        if any(lower.startswith(kw) for kw in JOB_CATEGORY_KEYWORDS):
            break
        filtered.append(inst)
    return filtered


def collect_all_jobs_by_institution(page) -> list[dict]:
    """
    Iterate through each institution filter checkbox, collect all jobs per
    institution, then clear the filter before moving to the next one.
    """
    all_jobs: list[dict] = []
    seen_ids: set[str]   = set()

    institutions = _discover_institutions(page)

    print(f"\nFound {len(institutions)} institution filters.")
    for inst in institutions:
        print(f"  [{inst['idx']:2d}] {inst['name']} ({inst['count']} jobs)")

    print()

    for inst in institutions:
        idx   = inst["idx"]
        name  = inst["name"]
        count = inst["count"]

        print(f"\n── {name}  ({count} jobs) ──────────────────")

        # Click the institution filter checkbox
        clicked = False
        for ctrl_sel in [
            f"#win0divPTS_SELECTctrl\\${idx}",
            f"#PTS_SELECT\\${idx}",
            f"label[id='PTS_SELECT_LBL\\${idx}']",
        ]:
            try:
                ctrl = page.locator(ctrl_sel).first
                if ctrl.count() > 0 and ctrl.is_visible():
                    ctrl.click()
                    clicked = True
                    break
            except Exception:
                pass
        if not clicked:
            try:
                page.evaluate(
                    f"doRadioOuterClick(document.getElementById('PTS_SELECT${idx}'))"
                )
                clicked = True
            except Exception as e:
                print(f"  Filter click failed for all methods: {e} — skipping")
                continue

        # ── Wait for PeopleSoft AJAX filter to complete ──────────────────────
        # Filter clicks are XHR (no navigation), so wait_for_load_state is useless.
        # PeopleSoft shows #WAIT_win0 during AJAX and hides it on completion.
        # Step 1: wait for the overlay to appear (AJAX started)
        try:
            page.wait_for_function(
                """() => {
                    const w = document.getElementById('WAIT_win0');
                    return w && w.style.display !== 'none' && w.style.visibility !== 'hidden';
                }""",
                timeout=5_000
            )
        except Exception:
            pass  # overlay too fast to catch — that's OK

        # Step 2: wait for the overlay to disappear (AJAX complete)
        try:
            page.wait_for_function(
                """() => {
                    const w = document.getElementById('WAIT_win0');
                    const p = document.getElementById('processing');
                    const wOk = !w || w.style.display === 'none' || w.style.visibility === 'hidden';
                    const pOk = !p || p.style.display === 'none' || p.style.visibility === 'hidden';
                    return wOk && pOk;
                }""",
                timeout=30_000
            )
        except Exception:
            page.wait_for_timeout(3000)

        # Step 3: confirm job rows are in the DOM
        try:
            page.wait_for_selector('span[id^="SCH_JOB_TITLE"]', timeout=15_000)
        except Exception:
            pass
        page.wait_for_timeout(1000)

        # Scroll to load all rows for this institution
        inst_rows = scroll_to_load_all(page, count, name)

        # Merge into global list (dedup)
        added = 0
        for r in inst_rows:
            if r["job_id"] and r["job_id"] not in seen_ids:
                seen_ids.add(r["job_id"])
                all_jobs.append(r)
                added += 1
        print(f"  Collected {added} new jobs  |  Running total: {len(all_jobs)}")

        # Clear the filter: reload the search page fresh so facets reset cleanly
        page.goto(SEARCH_URL, wait_until="load", timeout=90_000)
        try:
            page.wait_for_selector(
                'label[id^="PTS_SELECT_LBL"], span[id^="SCH_JOB_TITLE"]',
                timeout=30_000
            )
        except Exception:
            pass
        page.wait_for_timeout(1000)
        # Re-expand "More" so ALL institution filter checkboxes (indices 10+)
        # are back in the DOM — the page collapses the facet list on every reload.
        _expand_more_filters(page)

    return all_jobs

# ─── Detail fetching ──────────────────────────────────────────────────────────

def _make_session(cookie_jar: dict) -> requests.Session:
    """Create a fresh requests.Session pre-loaded with Playwright cookies."""
    s = requests.Session()
    for name, value in cookie_jar.items():
        s.cookies.set(name, value)
    return s


def fetch_detail_requests(session: requests.Session, job_id: str) -> str | None:
    url = DETAIL_URL_TEMPLATE.format(job_id=job_id)
    for attempt in range(1, DETAIL_RETRIES + 2):   # 1 try + DETAIL_RETRIES retries
        try:
            r = session.get(url, headers=HEADERS, timeout=(10, 25))
            if r.status_code == 200 and len(r.text) > 10_000:
                if "HRS_SCH_WRK2_POSTING_TITLE" in r.text or "HRS_SCH_PSTDSC_DESCRLONG" in r.text:
                    return r.text
                # Got a page but no content spans — likely JS-rendered; don't retry
                if DEBUG: print(f"    requests {job_id}: page OK but no content spans (attempt {attempt})")
                break
            elif r.status_code == 429:
                wait = 5 * attempt
                if DEBUG: print(f"    requests {job_id}: 429 rate-limited, waiting {wait}s")
                time.sleep(wait)
            else:
                if DEBUG: print(f"    requests {job_id}: HTTP {r.status_code} (attempt {attempt})")
                if attempt <= DETAIL_RETRIES:
                    time.sleep(2 * attempt)
        except Exception as e:
            if DEBUG: print(f"    requests failed {job_id} (attempt {attempt}): {e}")
            if attempt <= DETAIL_RETRIES:
                time.sleep(2 * attempt)
    return None


def fetch_detail_playwright(page, job_id: str) -> str | None:
    url = DETAIL_URL_TEMPLATE.format(job_id=job_id)
    try:
        page.goto(url, wait_until="load", timeout=60_000)
        # Wait for PeopleSoft detail content to appear (AJAX renders after load)
        try:
            page.wait_for_selector(
                "#HRS_SCH_WRK2_POSTING_TITLE, #HRS_SCH_PSTDSC_DESCRLONG\\$0",
                timeout=20_000
            )
        except Exception:
            page.wait_for_timeout(2000)
        html = page.content()
        if "HRS_SCH_WRK2_POSTING_TITLE" in html or "HRS_SCH_PSTDSC_DESCRLONG" in html:
            return html
    except Exception as e:
        if DEBUG: print(f"    playwright failed {job_id}: {e}")
    return None


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
    """Thread-safe incremental CSV save. Uses RLock so it's safe to call from
    within an already-locked block (reentrant)."""
    with lock:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_jobs)


def fetch_all_details(
    listing_cards: list[dict],
    cookie_jar: dict,
    pw_page,          # Playwright page for fallback (may be None)
    output_csv: str,
    output_json: str,
) -> list[dict]:
    """
    Fetch detail pages for every card in listing_cards.

    Strategy:
      • Try requests first (5 concurrent workers, each with its own session).
      • If a job's requests fetch returns None, queue it for Playwright fallback
        (sequential, since Playwright is single-threaded).
      • Save checkpoint CSV every CHECKPOINT_EVERY jobs.
      • Skips job IDs already present with detail content in output_csv (resume).
    """
    already_done = _load_already_fetched(output_csv)
    if FULL_REFRESH:
        print("  Full refresh mode (--full): ignoring existing CSV, re-fetching all jobs.")
        already_done = set()
    elif already_done:
        print(f"  Incremental: {len(already_done)} existing jobs will be kept — fetching new only.")

    # Pre-populate results with any previously fetched rows so they're included
    # in the final output even if we skip re-fetching them.
    prior_rows: dict[str, dict] = {}
    if Path(output_csv).exists() and already_done:
        try:
            with open(output_csv, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    jid = row.get("job_id", "").strip()
                    if jid in already_done:
                        prior_rows[jid] = row
        except Exception:
            pass

    pending = [c for c in listing_cards if c.get("job_id", "") not in already_done]
    print(f"  {len(pending)} jobs to fetch  ({len(already_done)} already done)\n")

    all_jobs: list[dict] = list(prior_rows.values())
    pw_queue: list[dict] = []   # cards that requests couldn't fetch
    lock = RLock()              # reentrant — safe to call _checkpoint from within lock
    checkpoint_counter = [0]

    def fetch_one(card: dict) -> tuple[str, str | None]:
        """Fetch a single detail page via requests; returns (job_id, html|None)."""
        job_id = card.get("job_id", "")
        session = _make_session(cookie_jar)
        html = fetch_detail_requests(session, job_id)
        time.sleep(DETAIL_DELAY)
        return job_id, html

    total = len(pending)
    fetched_count = [0]

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
                print(f"  [!] {job_id} worker timed out after {DETAIL_FUTURE_TIMEOUT}s → Playwright", flush=True)
            except Exception as e:
                print(f"  [!] worker error {job_id}: {e}", flush=True)

            fetched_count[0] += 1
            n = fetched_count[0]

            if html:
                try:
                    job = parse_detail(html, job_id, card)
                except Exception as e:
                    print(f"  [{n}/{total}] ! parse error {job_id}: {e} — saving stub", flush=True)
                    job = {k: "" for k in CSV_COLUMNS}
                    job.update(card)
                    job["posting_url"] = DETAIL_URL_TEMPLATE.format(job_id=job_id)
                    job["apply_link"]  = job["posting_url"]
                    job["scraped_at"]  = datetime.now(UTC).isoformat()
                has_qual = bool(job.get("required_qualifications"))
                print(f"  [{n}/{total}] ✓ {job_id}  quals={'YES' if has_qual else '---'}", flush=True)
                with lock:
                    all_jobs.append(job)
                    checkpoint_counter[0] += 1
                    if checkpoint_counter[0] % CHECKPOINT_EVERY == 0:
                        try:
                            _checkpoint(all_jobs, output_csv, lock)
                            print(f"    ── checkpoint saved ({len(all_jobs)} jobs) ──")
                        except Exception as e:
                            print(f"    !! checkpoint write failed: {e}")
            else:
                print(f"  [{n}/{total}] ✗ {job_id}  → queued for Playwright", flush=True)
                with lock:
                    pw_queue.append(card)

    # ── Phase B: Playwright fallback for failed requests ─────────────────────
    if pw_queue and pw_page is not None:
        print(f"\n  Phase B — Playwright fallback for {len(pw_queue)} jobs …")
        for i, card in enumerate(pw_queue, 1):
            job_id = card.get("job_id", "")
            print(f"  [PW {i}/{len(pw_queue)}] {job_id} …", end=" ", flush=True)
            html = fetch_detail_playwright(pw_page, job_id)
            if html:
                try:
                    job = parse_detail(html, job_id, card)
                except Exception as e:
                    print(f"! parse error: {e} — saving stub")
                    job = {k: "" for k in CSV_COLUMNS}
                    job.update(card)
                    job["posting_url"] = DETAIL_URL_TEMPLATE.format(job_id=job_id)
                    job["apply_link"]  = job["posting_url"]
                    job["scraped_at"]  = datetime.now(UTC).isoformat()
                all_jobs.append(job)
                has_qual = bool(job.get("required_qualifications"))
                print(f"✓  quals={'YES' if has_qual else '---'}")
            else:
                print("✗ FAILED — saving stub")
                job = {k: "" for k in CSV_COLUMNS}
                job.update(card)
                job["posting_url"] = DETAIL_URL_TEMPLATE.format(job_id=job_id)
                job["apply_link"]  = job["posting_url"]
                job["scraped_at"]  = datetime.now(UTC).isoformat()
                all_jobs.append(job)

            checkpoint_counter[0] += 1
            if checkpoint_counter[0] % CHECKPOINT_EVERY == 0:
                _checkpoint(all_jobs, output_csv, lock)
                print(f"    ── checkpoint saved ({len(all_jobs)} jobs) ──")
            time.sleep(DETAIL_DELAY)

    elif pw_queue:
        # No Playwright page — save stubs for failures
        print(f"\n  {len(pw_queue)} jobs could not be fetched (no Playwright fallback).")
        for card in pw_queue:
            job_id = card.get("job_id", "")
            job = {k: "" for k in CSV_COLUMNS}
            job.update(card)
            job["posting_url"] = DETAIL_URL_TEMPLATE.format(job_id=job_id)
            job["apply_link"]  = job["posting_url"]
            job["scraped_at"]  = datetime.now(UTC).isoformat()
            all_jobs.append(job)

    return all_jobs

# ─── Test mode ────────────────────────────────────────────────────────────────

def run_test():
    detail_file = Path("onehcm_detail.html")
    if not detail_file.exists():
        print("ERROR: onehcm_detail.html not found.")
        sys.exit(1)
    html = detail_file.read_text(encoding="utf-8")
    if not html.strip():
        print("ERROR: onehcm_detail.html is empty.")
        sys.exit(1)
    fake_card = {"job_id": "295413", "job_title": "", "institution": "Kennesaw State University",
                 "department": "CSE-Grants", "location": "Marietta, Georgia", "posted_date": ""}
    job = parse_detail(html, "295413", fake_card)
    print("=== PARSE TEST: job 295413 ===\n")
    for col in CSV_COLUMNS:
        val  = job.get(col, "")
        mark = "✓" if val else "✗"
        preview = (val[:80] + "…") if len(val) > 80 else val
        print(f"  {mark} {col:<35} {repr(preview)}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if "--test" in sys.argv:
        run_test()
        return

    listing_only        = "--listing-only"        in sys.argv
    details_from_listing = "--details-from-listing" in sys.argv

    from playwright.sync_api import sync_playwright

    print("=" * 60)
    print("USG OneHCM Scraper — Institution-by-Institution Mode")
    print("=" * 60)

    # ── Mode: details only from a previously saved listing JSON ───────────────
    if details_from_listing:
        listing_src = "onehcm_listings_only.json"
        if not Path(listing_src).exists():
            print(f"ERROR: {listing_src} not found. Run with --listing-only first.")
            sys.exit(1)
        listing_cards = json.loads(Path(listing_src).read_text(encoding="utf-8"))
        print(f"\nLoaded {len(listing_cards)} listing cards from {listing_src}")
        print(f"Output → {JSON_OUTPUT} / {CSV_OUTPUT}")
        print(f"Workers: {DETAIL_WORKERS}  |  Checkpoint every: {CHECKPOINT_EVERY} jobs\n")

        # We still need a browser for the Playwright fallback
        with sync_playwright() as p:
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
            page = context.new_page()
            # Warm up the session so PeopleSoft issues valid cookies
            print("Warming up session (loading search page for cookies) …")
            page.goto(SEARCH_URL, wait_until="load", timeout=90_000)
            try:
                page.wait_for_selector('span[id^="SCH_JOB_TITLE"]', timeout=45_000)
            except Exception:
                pass
            page.wait_for_timeout(2000)

            cookie_jar = {c["name"]: c["value"] for c in context.cookies()}

            all_jobs = fetch_all_details(
                listing_cards, cookie_jar, page, CSV_OUTPUT, JSON_OUTPUT
            )
            browser.close()

        _save(all_jobs, JSON_OUTPUT, CSV_OUTPUT)
        print(f"\n{'='*60}")
        print(f"Done!  {len(all_jobs)} jobs saved.")
        print(f"  →  {JSON_OUTPUT}")
        print(f"  →  {CSV_OUTPUT}")
        return

    # ── Normal mode: Phase 1 (listing) [+ Phase 2 (details)] ─────────────────
    all_jobs: list[dict] = []

    with sync_playwright() as p:
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
        page = context.new_page()

        # ── Phase 1: Listing ──────────────────────────────────────────────────
        print(f"\nLoading search page …")
        page.goto(SEARCH_URL, wait_until="load", timeout=90_000)

        # PeopleSoft fires a second AJAX search after the page settles.
        # Wait explicitly for the institution facet labels OR the first job title
        # to appear in the DOM before we try to read anything.
        print("Waiting for search results to render …")
        try:
            page.wait_for_selector(
                'label[id^="PTS_SELECT_LBL"], span[id^="SCH_JOB_TITLE"]',
                timeout=60_000
            )
        except Exception:
            # If the page shows a "Search Jobs" button first, click it
            print("  Results not auto-loaded — trying to click Search Jobs …")
            try:
                page.locator("#HRS_SCH_WRK_FLU_HRS_SEARCH_BTN").click()
                page.wait_for_selector(
                    'label[id^="PTS_SELECT_LBL"], span[id^="SCH_JOB_TITLE"]',
                    timeout=60_000
                )
            except Exception as e:
                print(f"  WARNING: Could not trigger search results: {e}")
        page.wait_for_timeout(2000)   # extra settle time

        listing_cards = collect_all_jobs_by_institution(page)
        print(f"\n{'='*60}")
        print(f"Listing complete: {len(listing_cards)} total unique jobs")

        if listing_only:
            browser.close()
            _save(listing_cards, "onehcm_listings_only.json", "onehcm_listings_only.csv")
            return

        # ── Phase 2: Details ──────────────────────────────────────────────────
        cookie_jar = {c["name"]: c["value"] for c in context.cookies()}
        print(f"\nFetching details for {len(listing_cards)} jobs …")
        print(f"Workers: {DETAIL_WORKERS}  |  Checkpoint every: {CHECKPOINT_EVERY} jobs\n")

        all_jobs = fetch_all_details(
            listing_cards, cookie_jar, page, CSV_OUTPUT, JSON_OUTPUT
        )
        browser.close()

    _save(all_jobs, JSON_OUTPUT, CSV_OUTPUT)
    print(f"\n{'='*60}")
    print(f"Done!  {len(all_jobs)} jobs saved.")
    print(f"  →  {JSON_OUTPUT}")
    print(f"  →  {CSV_OUTPUT}")


def _save(jobs, json_path, csv_path):
    Path(json_path).write_text(
        json.dumps(jobs, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(jobs)
    print(f"Saved {len(jobs)} records → {json_path}, {csv_path}")


if __name__ == "__main__":
    main()
