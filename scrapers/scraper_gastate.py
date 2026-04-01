"""
Georgia State University Careers Scraper
=========================================
Covers four job portals:

  Portal 1 — Taleo (staff/admin, career section 2)
    https://gsu.taleo.net/careersection/2/jobsearch.ftl?lang=en

  Portal 2 — Taleo PanthTemps (student/temp workers)
    https://gsu.taleo.net/careersection/panthtemps/moresearch.ftl?...

  Portal 3 — Faculty Careers (position_type_id=3, e.g. Tenure Track)
    https://facultycareers.gsu.edu/postings/search?...position_type_id[]=3

  Portal 4 — Faculty Careers (position_type_id=4, e.g. Non-Tenure Track)
    https://facultycareers.gsu.edu/postings/search?...position_type_id[]=4

Output: gastate_jobs.csv / gastate_jobs.json
        (same column schema as onehcm_jobs.csv for easy merging)

Usage:
    python3 scraper_gastate.py                  # all portals
    python3 scraper_gastate.py --taleo-only     # Taleo portals only
    python3 scraper_gastate.py --faculty-only   # Faculty portals only
    python3 scraper_gastate.py --debug          # verbose

Requirements:
    pip3 install playwright requests beautifulsoup4 lxml --break-system-packages
    playwright install chromium
"""

import csv, json, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup

# ─── Config ──────────────────────────────────────────────────────────────────

TALEO_BASE       = "https://gsu.taleo.net"
TALEO_STAFF_URL  = f"{TALEO_BASE}/careersection/2/jobsearch.ftl?lang=en"
TALEO_TEMPS_URL  = (
    f"{TALEO_BASE}/careersection/panthtemps/moresearch.ftl"
    "?lang=en&employeestatus=4&radiusType=K&searchExpanded=false&radius=1&portal=101430233"
)

FACULTY_BASE     = "https://facultycareers.gsu.edu"
# No position_type_id filter — catches all types (tenure-track, NTT, visiting, etc.)
FACULTY_ALL_URL  = (
    f"{FACULTY_BASE}/postings/search"
    "?utf8=%E2%9C%93&query=&query_v0_posted_at_date=&commit=Search"
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

SCROLL_PAUSE    = 2.0
PAGE_DELAY      = 1.5   # seconds between page fetches
DETAIL_DELAY    = 0.8
CHECKPOINT_EVERY = 50

JSON_OUTPUT = "gastate_jobs.json"
CSV_OUTPUT  = "gastate_jobs.csv"
UTC   = timezone.utc
DEBUG        = "--debug" in sys.argv
DEBUG_DETAIL = "--debug-detail" in sys.argv or DEBUG

# ─── CSV columns (same as onehcm_jobs.csv) ───────────────────────────────────

CSV_COLUMNS = [
    "job_id", "job_title", "institution", "department",
    "location", "posted_date", "full_part_time", "regular_temporary",
    "location_detail", "about_us", "job_summary", "responsibilities",
    "required_qualifications", "preferred_qualifications",
    "knowledge_skills_abilities", "other_information", "background_check",
    "salary", "extra_sections", "apply_link", "posting_url", "scraped_at",
]

SECTION_MAP = {
    "job summary":                     "job_summary",
    "overview":                        "job_summary",
    "position summary":                "job_summary",
    "summary":                         "job_summary",
    "description":                     "job_summary",
    "position description":            "job_summary",
    "about us":                        "about_us",
    "about georgia state":             "about_us",
    "about gsu":                       "about_us",
    "responsibilities":                "responsibilities",
    "duties":                          "responsibilities",
    "essential duties":                "responsibilities",
    "essential functions":             "responsibilities",
    "key responsibilities":            "responsibilities",
    "qualifications":                  "required_qualifications",
    "required qualifications":         "required_qualifications",
    "minimum qualifications":          "required_qualifications",
    "minimum requirements":            "required_qualifications",
    "requirements":                    "required_qualifications",
    "preferred qualifications":        "preferred_qualifications",
    "preferred requirements":          "preferred_qualifications",
    "knowledge, skills, & abilities":  "knowledge_skills_abilities",
    "knowledge, skills, abilities":    "knowledge_skills_abilities",
    "knowledge, skills and abilities": "knowledge_skills_abilities",
    "skills":                          "knowledge_skills_abilities",
    "other information":               "other_information",
    "additional information":          "other_information",
    "benefits":                        "other_information",
    "background check":                "background_check",
    "background investigation":        "background_check",
    "salary":                          "salary",
    "salary range":                    "salary",
    "proposed salary":                 "salary",
    "pay range":                       "salary",
    "compensation":                    "salary",
}

# ─── Helpers ─────────────────────────────────────────────────────────────────

def clean(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()

def empty_job(institution: str = "Georgia State University") -> dict:
    job = {k: "" for k in CSV_COLUMNS}
    job["institution"] = institution
    job["scraped_at"]  = datetime.now(UTC).isoformat()
    return job

def _checkpoint(jobs: list, csv_path: str):
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(jobs)

def _save(jobs: list, json_path: str, csv_path: str):
    Path(json_path).write_text(
        json.dumps(jobs, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    _checkpoint(jobs, csv_path)
    print(f"Saved {len(jobs)} records → {json_path}, {csv_path}")

FULL_REFRESH = "--full" in sys.argv  # re-fetch every job; default is incremental

def _load_already_fetched(csv_path: str) -> set[str]:
    """Return all job_ids already in the output CSV (incremental: skip all existing)."""
    if FULL_REFRESH:
        return set()
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

def _load_existing_jobs(csv_path: str) -> dict:
    """Load all existing rows from the CSV keyed by job_id, for merging."""
    if FULL_REFRESH:
        return {}
    p = Path(csv_path)
    if not p.exists():
        return {}
    existing = {}
    try:
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                jid = row.get("job_id", "").strip()
                if jid:
                    existing[jid] = row
    except Exception:
        pass
    return existing

def parse_html_sections(html_or_text: str) -> dict:
    """
    Parse a job description HTML/text into structured sections.
    Returns a dict with keys matching CSV_COLUMNS content fields.
    """
    soup = BeautifulSoup(html_or_text, "lxml")
    result = {k: "" for k in [
        "about_us", "job_summary", "responsibilities",
        "required_qualifications", "preferred_qualifications",
        "knowledge_skills_abilities", "other_information",
        "background_check", "extra_sections",
    ]}

    sections = []
    current_heading = ""
    current_chunks  = []

    def flush():
        nonlocal current_heading, current_chunks
        text = clean(" ".join(current_chunks))
        if text:
            sections.append((current_heading, text))
        current_heading = ""
        current_chunks  = []

    for el in soup.find_all(["h1","h2","h3","h4","p","ul","ol","li","div","br","strong","b"]):
        tag  = el.name
        text = clean(el.get_text(separator=" "))

        if tag in ("h1","h2","h3","h4"):
            flush()
            current_heading = text
        elif tag in ("strong","b"):
            # Bold text as heading only if short and alone
            if len(text) < 80 and text.endswith(":"):
                flush()
                current_heading = text.rstrip(":")
            elif text:
                current_chunks.append(text)
        elif text:
            current_chunks.append(text)
    flush()

    if not sections:
        result["job_summary"] = clean(soup.get_text(separator=" "))
        return result

    extra_parts = []
    for heading, content in sections:
        key = heading.lower().strip(": ")
        col = SECTION_MAP.get(key)
        if col:
            if result[col]:
                result[col] += " " + content
            else:
                result[col] = content
        elif not heading:
            result["job_summary"] = (result["job_summary"] + " " + content).strip()
        else:
            extra_parts.append(f"[{heading}] {content}")

    if extra_parts:
        result["extra_sections"] = " || ".join(extra_parts)

    return result

# ─── Portal 1 & 2: Taleo ─────────────────────────────────────────────────────

def _taleo_dump_debug(page, tag: str):
    """Save full page HTML to a debug file and print diagnostics."""
    html = page.content()
    fname = f"debug_taleo_{tag}.html"
    Path(fname).write_text(html, encoding="utf-8")
    print(f"    [debug] Full HTML saved → {fname}  ({len(html)} bytes)", flush=True)
    print(f"    [debug] Current URL: {page.url}", flush=True)
    # List all frames
    for i, fr in enumerate(page.frames):
        print(f"    [debug] Frame[{i}]: {fr.url}", flush=True)
    # Print forms found on the page
    forms = page.evaluate("""
        () => Array.from(document.querySelectorAll('form')).map(f => ({
            action: f.action, method: f.method, id: f.id,
            inputs: Array.from(f.querySelectorAll('input,button')).map(e => ({
                tag: e.tagName, type: e.type, name: e.name,
                id: e.id, value: e.value.slice(0,40)
            }))
        }))
    """)
    print(f"    [debug] Forms found: {len(forms)}", flush=True)
    for j, f in enumerate(forms):
        print(f"    [debug]   form[{j}] action={f['action']} method={f['method']}", flush=True)
        for inp in f["inputs"]:
            print(f"    [debug]     {inp['tag']} type={inp['type']} name={inp['name']} "
                  f"id={inp['id']} value={inp['value']!r}", flush=True)


def _taleo_submit_search(page, label: str = "Taleo"):
    """
    Both jobsearch.ftl AND moresearch.ftl are Taleo search forms.
    Clicking the Search button (with all fields empty) returns ALL jobs.
    Must be called after page load before any job rows will appear.
    """
    print(f"    {label}: submitting search form …", flush=True)

    if DEBUG:
        _taleo_dump_debug(page, label.replace(" ", "_") + "_before")

    # ── Strategy 1: Click a real <input type="submit"> button ─────────────────
    # IMPORTANT: Try actual submit inputs BEFORE any <a> tags to avoid
    # accidentally clicking navigation links that contain the word "Search".
    for sel in [
        "input[type='submit'][value='Search']",
        "input[type='submit'][value*='Search']",
        "input[type='submit'][id='btnSearch']",
        "input[type='submit'][id*='btnSearch']",
        "input[type='submit'][id*='search' i]",
        "input[type='submit'][name*='search' i]",
        "button[type='submit'][id*='search' i]",
        "button[type='submit']",
        "input[type='submit']",           # ANY submit — last resort before JS
    ]:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                loc.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(2000)
                print(f"    Search form submitted (selector: {sel}).", flush=True)
                if DEBUG:
                    _taleo_dump_debug(page, label.replace(" ", "_") + "_after")
                return
        except Exception:
            pass

    # ── Strategy 2: JavaScript form.submit() — most reliable ──────────────────
    # Directly submits the form that looks like a job search form.
    print(f"    Trying JS form.submit() …", flush=True)
    try:
        submitted = page.evaluate("""
            () => {
                const forms = Array.from(document.querySelectorAll('form'));
                // Find the best candidate: form whose action URL contains 'jobsearch' or 'moresearch'
                // or any form that contains a submit button
                for (const priority of ['jobsearch', 'moresearch', 'ftl', '']) {
                    for (const f of forms) {
                        const action = (f.action || '').toLowerCase();
                        if (!priority || action.includes(priority)) {
                            try { f.submit(); return f.action || '(submitted)'; }
                            catch(e) { return null; }
                        }
                    }
                }
                return null;
            }
        """)
        if submitted:
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(2500)
            print(f"    JS form.submit() → {submitted}", flush=True)
            if DEBUG:
                _taleo_dump_debug(page, label.replace(" ", "_") + "_after_js")
            return
    except Exception as e:
        print(f"    JS form.submit() failed: {e}", flush=True)

    print(f"    WARNING: could not submit search form for {label}.", flush=True)
    if DEBUG:
        _taleo_dump_debug(page, label.replace(" ", "_") + "_failed")


def _taleo_next_page(page) -> bool:
    """
    Click the Taleo 'Next' pagination link.

    CONFIRMED STRUCTURE (from debug HTML analysis):
    - Active Next link:   <span class="pagerlink"><a id="...Next" onclick="ftlPager_doNext(...)">Next</a></span>
    - Disabled Prev link: <span class="pagerlinkoff"><a ...>Previous</a></span>
    - The span class distinguishes active vs disabled — "pagerlinkoff" = disabled.

    We click the <a> inside span.pagerlink whose text is "Next", using JS to
    bypass any Playwright visibility issues.
    """
    # Strategy 1: JS click — most reliable for Taleo's onclick-based links
    try:
        clicked = page.evaluate("""
            () => {
                // Find all span.pagerlink (active, not disabled) elements
                const spans = document.querySelectorAll('span.pagerlink');
                for (const span of spans) {
                    const a = span.querySelector('a');
                    if (!a) continue;
                    const txt = a.textContent.trim().toLowerCase();
                    if (txt === 'next') {
                        a.click();
                        return 'js-next:' + a.id;
                    }
                }
                // Fallback: any <a> with onclick containing ftlPager_doNext
                const allA = document.querySelectorAll('a[onclick*="ftlPager_doNext"]');
                if (allA.length > 0) {
                    allA[0].click();
                    return 'js-doNext:' + allA[0].id;
                }
                return null;
            }
        """)
        if clicked:
            if DEBUG:
                print(f"    [next-page] {clicked}", flush=True)
            page.wait_for_timeout(int(PAGE_DELAY * 1000) + 1500)
            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass
            page.wait_for_timeout(1500)
            return True
    except Exception as e:
        if DEBUG:
            print(f"    [next-page] JS click failed: {e}", flush=True)

    # Strategy 2: Playwright locator on span.pagerlink > a (visible Next)
    try:
        loc = page.locator("span.pagerlink a", has_text="Next").first
        if loc.count() > 0:
            loc.click(timeout=5_000)
            page.wait_for_timeout(int(PAGE_DELAY * 1000) + 1500)
            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass
            page.wait_for_timeout(1500)
            return True
    except Exception:
        pass

    return False


def _taleo_parse_ftlrow(row_el, career_section: str) -> dict | None:
    """
    Parse one Taleo FTL job listing row (tr.ftlrow) into a card dict.

    CONFIRMED STRUCTURE (from debug HTML analysis of gsu.taleo.net):
    - Row element: <tr class="ftlcopy ftlrow" id="requisitionListInterface.ID*.row">
    - Internal ID (for URL): <div class="iconcontentpanel" id="NUMERIC_ID">
      where NUMERIC_ID is the actual Taleo requisition ID used in jobdetail.ftl URLs.
    - Title: <a id="*reqTitleLink*">Job Title</a>
    - Location: <span id="*reqBasicLocation*">Atlanta Campus</span>
    - Dept: span that follows the "Department Name" label span
    - Posted date: <span id="*reqPostingDate*">MM/DD/YY</span>
    - Contest number: <span id="*reqContestNumberValue*">26000417</span>
      (display ID — NOT the URL parameter)

    Detail URL: https://gsu.taleo.net/careersection/{section}/jobdetail.ftl?job={id}&lang=en
    """
    # The numeric div id IS the requisition ID used in the job detail URL
    num_div = row_el.find("div", id=re.compile(r"^\d+$"))
    if not num_div:
        return None  # This is a template row, not a real data row
    req_id = num_div.get("id", "")
    if not req_id:
        return None

    # Build the job detail URL
    detail_url = (
        f"{TALEO_BASE}/careersection/{career_section}/jobdetail.ftl?job={req_id}&lang=en"
    )

    # Title from the reqTitleLink anchor
    title_a = row_el.find("a", id=re.compile(r"reqTitleLink", re.I))
    title = clean(title_a.get_text()) if title_a else ""

    # Location from reqBasicLocation span
    loc_span = row_el.find("span", id=re.compile(r"reqBasicLocation", re.I))
    location = clean(loc_span.get_text()) if loc_span else ""

    # Posted date from reqPostingDate span
    date_span = row_el.find("span", id=re.compile(r"reqPostingDate\b", re.I))
    posted_date = clean(date_span.get_text()) if date_span else ""

    # Department: the span that comes after the "Department Name" label span.
    # The label span's sibling pattern: [label "Department Name"][":"][value span]
    # We find it by scanning for the span whose text is "Department Name" and
    # taking the next non-punctuation sibling span's text.
    department = ""
    dept_label = row_el.find("span", string=re.compile(r"^Department\s*Name$", re.I))
    if dept_label:
        # Walk next siblings to find the value span
        for sib in dept_label.find_next_siblings("span"):
            txt = sib.get_text(strip=True)
            if txt and txt not in (":", "-", "|"):
                department = clean(txt)
                break

    if not title:
        return None  # Skip blank rows

    return {
        "job_id": req_id,
        "url": detail_url,
        "title": title,
        "department": department,
        "location": location,
        "posted_date": posted_date,
    }


def _taleo_has_next_page(soup: BeautifulSoup) -> bool:
    """
    Return True if an active (non-disabled) Next pagination link exists.

    CONFIRMED STRUCTURE:
    - Active:   <span class="pagerlink"><a onclick="ftlPager_doNext(...)">Next</a></span>
    - Disabled: <span class="pagerlinkoff"><a ...>Previous</a></span>
    - Label:    <span class="pagerlabel">Jobs - Page X out of Y</span>

    We first check the pagerlabel to parse X and Y.  If X < Y, there are more
    pages.  This is the most reliable check; the span.pagerlink approach can give
    false positives when the Next element appears inside callout/search panels.
    """
    # Primary check: parse "Page X out of Y" label
    label_el = soup.find("span", class_="pagerlabel")
    if label_el:
        txt = label_el.get_text()
        m = re.search(r"Page\s+(\d+)\s+out\s+of\s+(\d+)", txt, re.I)
        if m:
            current, total = int(m.group(1)), int(m.group(2))
            return current < total

    # Fallback: look for span.pagerlink containing a "Next" link with ftlPager_doNext onclick
    for span in soup.find_all("span", class_="pagerlink"):
        a = span.find("a", onclick=re.compile(r"ftlPager_doNext", re.I))
        if a and a.get_text(strip=True).lower() == "next":
            return True

    return False


def _taleo_collect_links(page, search_url: str, section_name: str) -> list[dict]:
    """
    Collect Taleo job listing cards using Playwright.

    KEY FINDINGS (from analysis of saved debug HTML files):

    1. INITIAL PAGE LOAD ALREADY HAS DATA
       When Playwright navigates to jobsearch.ftl or moresearch.ftl and waits
       for networkidle, the first page of job results is ALREADY rendered in the
       main frame HTML — no "Search" button click is needed.  The Search button
       is for applying filters; with no filters, all jobs show immediately.

    2. ROW CLASS IS 'ftlrow', NOT 'tr.odd'/'tr.even'
       Taleo Enterprise (this version) uses <tr class="ftlcopy ftlrow"> for data
       rows.  Template/placeholder rows have class "ftlrow" but NO numeric div ID.
       Real data rows have a <div class="iconcontentpanel" id="NUMERIC_ID"> where
       NUMERIC_ID is the requisition ID for jobdetail.ftl URLs.

    3. JOB DETAIL URL PATTERN
       https://gsu.taleo.net/careersection/{section}/jobdetail.ftl?job={numericID}&lang=en
       where {section} is '2' for Staff or 'panthtemps' for PanthTemps.

    4. PAGINATION
       Active Next: <span class="pagerlink"><a onclick="ftlPager_doNext(...)">Next</a></span>
       Disabled:    <span class="pagerlinkoff"><a>Previous</a></span>
       Click via JS: document.querySelector('span.pagerlink a').click() if text == 'Next'
    """
    # Extract career section from URL (e.g. "2" or "panthtemps")
    m = re.search(r"careersection/([^/]+)/", search_url)
    career_section = m.group(1) if m else "2"
    label = "PanthTemps" if career_section == "panthtemps" else "Taleo Staff"

    if page is None:
        print(f"  [{label}] Playwright not available — skipping.", flush=True)
        return []

    print(f"\n  [{label}] Loading {search_url} …", flush=True)
    page.goto(search_url, wait_until="networkidle", timeout=60_000)
    page.wait_for_timeout(3000)

    if DEBUG:
        _taleo_dump_debug(page, f"{label.replace(' ','_')}_loaded")

    links    = []
    seen     = set()
    page_num = 1

    while True:
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
        added = 0

        # Parse real data rows — must have BOTH 'ftlcopy' AND 'ftlrow' classes.
        # Rows with only 'ftlrow' (no 'ftlcopy') are template/callout rows with
        # no actual job data even if they contain a numeric-id div.
        for row in soup.find_all("tr", class_=lambda c: c and "ftlrow" in c and "ftlcopy" in c):
            card = _taleo_parse_ftlrow(row, career_section)
            if card and card["url"] not in seen:
                seen.add(card["url"])
                card["section"] = section_name
                links.append(card)
                added += 1

        print(f"  [{label}] Page {page_num}: +{added}  total {len(links)}", flush=True)

        if added == 0:
            print(f"  [{label}] No data rows on page {page_num}. "
                  f"{'(Expected — done)' if page_num > 1 else 'Run --debug for HTML.'} ",
                  flush=True)
            if page_num == 1 and DEBUG:
                _taleo_dump_debug(page, f"{label.replace(' ','_')}_empty_p1")
            break

        # Check for next page
        if not _taleo_has_next_page(soup):
            break

        if not _taleo_next_page(page):
            print(f"  [{label}] Next button click failed — stopping pagination.", flush=True)
            break

        page_num += 1

    print(f"  {section_name}: {len(links)} job links across {page_num} page(s)",
          flush=True)
    return links


def _taleo_parse_detail(html: str, card: dict) -> dict:
    """
    Parse a Taleo Enterprise job detail page into structured fields.

    Taleo detail page anatomy:
      • #requisitionDescriptionInterface  — outer wrapper div (most common)
      • #jobDescriptionInterface          — alternate outer wrapper
      • span.jobTitle or h1              — job title
      • table.descriptionStrap           — metadata label/value table
        (labels in <th> or left-side <td>, values in right-side <td>)
      • div with id/class containing "jobDescription" — description body
    """
    soup = BeautifulSoup(html, "lxml")
    job  = empty_job("Georgia State University")

    # ── Identity ───────────────────────────────────────────────────────────────
    job["job_id"]      = card.get("job_id", "")
    job["posting_url"] = card.get("url", "")
    job["apply_link"]  = card.get("url", "")

    # ── Title ──────────────────────────────────────────────────────────────────
    # Always use the title from the listing card — it's always correct (scraped
    # from reqTitleLink on the search results page).  The Taleo detail page h1
    # says "Job Description" (the portal section header), NOT the actual title.
    job["job_title"] = card.get("title", "")
    if not job["job_title"]:
        # Fallback: try span.jobTitle but skip generic "Job Description" h1
        for sel in ["span.jobTitle", ".jobTitle", "h1.jobTitle"]:
            el = soup.select_one(sel)
            if el:
                t = clean(el.get_text())
                if t and not re.match(r'^Job\s+Description\b', t, re.I):
                    job["job_title"] = t
                    break

    # ── Metadata table (table.descriptionStrap or generic th/td pairs) ─────────
    # Taleo metadata tables typically look like:
    #   <tr><th>Organization</th><td>College of Arts & Sciences</td></tr>
    # but sometimes both cells are <td>, with the first acting as a label.
    def _read_meta_tables(soup):
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = clean(cells[0].get_text()).lower().rstrip(": ")
            value = clean(cells[1].get_text())
            if not value or not label:
                continue
            yield label, value

    for label, value in _read_meta_tables(soup):
        if any(k in label for k in ("department", "organization", "college", "unit", "school")):
            if not job["department"]: job["department"] = value
        elif any(k in label for k in ("location", "city", "address", "campus")):
            if not job["location"]: job["location"] = value
        elif "position type" in label or ("type" in label and "position" in label):
            if not job["full_part_time"]: job["full_part_time"] = value
        elif any(k in label for k in ("regular", "temporary", "employee status", "job status")):
            if not job["regular_temporary"]: job["regular_temporary"] = value
        elif any(k in label for k in ("posted", "open date", "opening date")):
            if not job["posted_date"]: job["posted_date"] = value
        elif any(k in label for k in ("closing", "close date", "deadline")):
            extra = f"Close date: {value}"
            job["other_information"] = (
                (job["other_information"] + "  " + extra).strip()
                if job["other_information"] else extra
            )

    # Fall back to card metadata
    if not job["department"]:  job["department"]  = card.get("department", "")
    if not job["location"]:    job["location"]    = card.get("location", "")
    if not job["posted_date"]: job["posted_date"] = card.get("posted_date", "")

    # ── Description body ───────────────────────────────────────────────────────
    # Taleo Enterprise stores the job description in a label/value table where
    # the label (th or first td) contains section names like "Description",
    # "Qualifications", etc., and the value (second td) contains the content.
    # After JS runs, these values are populated.  We try structured extraction
    # from that table first, then fall back to container-div parsing.

    # Strategy A: extract description fields from the label/value table
    # (handles the most common Taleo Enterprise layout after JS renders)
    table_sections = {k: "" for k in [
        "about_us", "job_summary", "responsibilities",
        "required_qualifications", "preferred_qualifications",
        "knowledge_skills_abilities", "other_information",
        "background_check", "extra_sections",
    ]}
    extra_table_parts = []
    found_table_content = False

    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        label = clean(cells[0].get_text()).lower().rstrip(": ")
        # Get the full content of the value cell (preserve structure for parse_html_sections)
        value_cell = cells[1]
        value_text = clean(value_cell.get_text(separator=" "))
        if not value_text or not label:
            continue

        # Skip metadata labels already handled above
        if any(k in label for k in ("department", "organization", "college", "unit", "school",
                                     "location", "city", "address", "campus", "position type",
                                     "regular", "temporary", "employee status", "posted",
                                     "open date", "closing", "deadline")):
            continue

        col = SECTION_MAP.get(label)
        if col:
            found_table_content = True
            # Parse the HTML content of the cell for richer structure
            cell_sections = parse_html_sections(str(value_cell))
            if cell_sections.get(col):
                table_sections[col] = (table_sections[col] + " " + cell_sections[col]).strip()
            else:
                table_sections[col] = (table_sections[col] + " " + value_text).strip()
        elif value_text:
            # Unknown label with content — store in extra_sections
            found_table_content = True
            extra_table_parts.append(f"[{label.title()}] {value_text}")

    if extra_table_parts:
        existing = table_sections.get("extra_sections", "")
        table_sections["extra_sections"] = (
            (existing + " || " + " || ".join(extra_table_parts)).strip(" || ")
        )

    # ── Strategy A result ─────────────────────────────────────────────────────
    if found_table_content:
        job.update(table_sections)

        # Strip the Taleo metadata prefix that appears at the start of the
        # description cell content.  Taleo Enterprise prepends a text block:
        #   "[Job Title]  [Department]  [College]  Georgia State University"
        # before the actual description paragraph.  Strip up to and including
        # the institution name so we get the real description text.
        raw_summary = job.get("job_summary", "")
        if raw_summary:
            # Remove leading card-title text (exact prefix match after clean())
            card_title = card.get("title", "")
            if card_title and raw_summary.lower().startswith(card_title.lower()):
                raw_summary = raw_summary[len(card_title):].lstrip()
            # Remove everything up to "Georgia State University " (metadata header)
            gsu_m = re.search(r'Georgia State University\s+', raw_summary)
            if gsu_m and gsu_m.start() < 400:
                raw_summary = raw_summary[gsu_m.end():].strip()
            if raw_summary and len(raw_summary) > 50:
                job["job_summary"] = raw_summary

        return job

    # ── Strategy B: use #maincontent (confirmed present in Taleo detail pages) ─
    # Diagnostic confirmed: after JS renders, job content is inside #maincontent.
    # #job_menubar inside it holds navigation links — strip that first.
    # The remaining HTML contains bold section headers (<b>Description:</b> etc.)
    # which parse_html_sections correctly identifies as section boundaries.
    NAV_MARKER = "Beginning of the main content section"

    content_el = soup.find(id="maincontent")
    if content_el:
        # Strip navigation menubar
        for junk_id in ("job_menubar",):
            junk = content_el.find(id=junk_id)
            if junk:
                junk.decompose()
        sections = parse_html_sections(str(content_el))
    else:
        # Strategy C: other known container IDs
        desc_el = (
            soup.find(id="jobDescriptionInterface") or
            soup.find(id="jobDescriptionText") or
            soup.find(id=re.compile(r"jobDesc|job_desc|jobDescriptionText", re.I)) or
            soup.find(class_=re.compile(r"jobDescriptionText|jobDescription\b", re.I)) or
            soup.find(id="requisitionDescriptionInterface") or
            soup.find(id=re.compile(r"requisitionDesc", re.I))
        )
        if desc_el:
            sections = parse_html_sections(str(desc_el))
        else:
            h1 = soup.find("h1")
            if h1:
                parts = [str(s) for s in h1.find_all_next(["p","ul","ol","h2","h3","h4","div"])]
                sections = parse_html_sections(" ".join(parts))
            else:
                sections = parse_html_sections(str(soup))

    # Strip the accessibility nav-marker that appears at the start of #maincontent
    for key in ("job_summary", "about_us"):
        val = sections.get(key, "")
        if val.startswith(NAV_MARKER):
            sections[key] = re.sub(
                r"^Beginning of the main content section\.?\s*", "", val
            ).strip()

    # Only apply sections if they contain real content (not just nav-junk)
    has_real_content = any(
        v and NAV_MARKER not in v and len(v) > 80
        for v in sections.values()
        if isinstance(v, str)
    )
    if has_real_content:
        job.update(sections)

    # ── Taleo-specific post-processing ────────────────────────────────────────
    # Taleo's page title is "Job Description - [Title] - [ID]", which doesn't
    # match any SECTION_MAP key, so the job description content lands in
    # extra_sections as "[Job Description ...]".  Meanwhile, the Taleo session
    # header bar (Welcome, Sign In, etc.) lands in job_summary as nav-junk.
    # Fix: extract the [Job Description] block from extra_sections, strip the
    # redundant title prefix, and promote it to job_summary.
    _NAV_SIGNS = (
        "Welcome. You are not signed in",
        "This service is set to disconnect",
        "You have been signed out",
    )
    extra      = job.get("extra_sections", "")
    current_js = job.get("job_summary", "")

    summary_is_junk = (
        not current_js or
        any(s in current_js[:400] for s in _NAV_SIGNS)
    )

    if summary_is_junk and extra:
        m = re.search(
            r'\[Job Description[^\]]*\]\s*(.+?)(?=\s*\|\|\s*\[|\Z)',
            extra, re.DOTALL
        )
        if m:
            desc_text = clean(m.group(1))
            # Strip "Job Description [Title] - [ID]" header at the start
            desc_text = re.sub(
                r'^Job Description\s+.+?\s*[-–]\s*\d+\s*', '', desc_text
            ).strip()
            # Strip leading "Description :" label (Taleo uses "Description :")
            dm = re.search(r'Description\s*:\s*', desc_text)
            if dm and dm.start() < 150:
                desc_text = desc_text[dm.end():].strip()
            if desc_text and len(desc_text) > 50:
                job["job_summary"] = desc_text
            # Remove the [Job Description ...] block from extra_sections
            job["extra_sections"] = re.sub(
                r'\[Job Description[^\]]*\]\s*.+?(?=\s*\|\|\s*\[|\Z)',
                '', extra, flags=re.DOTALL
            ).strip().strip('| ')
    elif any(s in current_js[:400] for s in _NAV_SIGNS):
        job["job_summary"] = ""

    # Strip Taleo metadata prefix from job_summary (Strategy B path).
    # Strategy A already does this before its early return; this covers
    # any job that falls through to Strategy B / post-processing.
    raw_summary = job.get("job_summary", "")
    if raw_summary:
        card_title = card.get("title", "")
        if card_title and raw_summary.lower().startswith(card_title.lower()):
            raw_summary = raw_summary[len(card_title):].lstrip()
        gsu_m = re.search(r'Georgia State University\s+', raw_summary)
        if gsu_m and gsu_m.start() < 400:
            raw_summary = raw_summary[gsu_m.end():].strip()
        if raw_summary and len(raw_summary) > 50:
            job["job_summary"] = raw_summary

    return job


def _taleo_fetch_detail(page, card: dict) -> dict:
    """
    Fetch a Taleo job detail page and parse it.

    IMPORTANT: Taleo detail pages at jobdetail.ftl are JavaScript-rendered.
    requests.get() returns an HTML shell with ALL field values empty (the JS
    hasn't run yet to populate them).  The shell is large enough (>5 KB) that
    a simple size check cannot distinguish it from a real page.  We must use
    Playwright so that JavaScript executes and fills in the job content.

    Strategy:
      1. Playwright (primary) — navigate, wait for networkidle + content element
      2. requests (fallback) — only if Playwright is unavailable
    """
    url  = card["url"]
    html = None

    # ── Playwright (primary — JS populates the field values) ─────────────────
    # IMPORTANT: Taleo has constant background polling so "networkidle" NEVER
    # fires.  Use wait_until="load" (DOM ready) and then a fixed wait for JS
    # to render the job content.  Always read page.content() even if goto
    # raises a timeout — the content is already rendered by then.
    if page is not None:
        try:
            try:
                page.goto(url, wait_until="load", timeout=30_000)
            except Exception as _goto_err:
                if DEBUG:
                    print(f"    [detail] goto timeout (OK): {_goto_err}", flush=True)
            # Fixed wait for Taleo JS to populate the job fields
            page.wait_for_timeout(5_000)
            html = page.content()
            if DEBUG:
                print(f"    [detail] playwright OK ({len(html)} bytes)", flush=True)

            # On first job, save the detail HTML for selector debugging
            if DEBUG_DETAIL:
                _debug_detail_path = Path("debug_taleo_detail_sample.html")
                if not _debug_detail_path.exists():
                    _debug_detail_path.write_text(html, encoding="utf-8")
                    print(f"    [debug-detail] Saved → {_debug_detail_path}", flush=True)
        except Exception as e:
            if DEBUG:
                print(f"    [detail] playwright error {url}: {e}", flush=True)

    # ── requests (fallback — only if Playwright unavailable) ─────────────────
    if html is None:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and len(r.text) > 5_000:
                html = r.text
                if DEBUG:
                    print(f"    [detail] requests fallback ({len(html)} bytes)", flush=True)
        except Exception as e:
            if DEBUG:
                print(f"    [detail] requests error: {e}", flush=True)

    if html:
        try:
            return _taleo_parse_detail(html, card)
        except Exception as e:
            if DEBUG: print(f"    [detail] parse error {url}: {e}", flush=True)

    job = empty_job("Georgia State University")
    job.update({
        "job_id":      card.get("job_id", ""),
        "job_title":   card.get("title", ""),
        "department":  card.get("department", ""),
        "location":    card.get("location", ""),
        "posted_date": card.get("posted_date", ""),
        "posting_url": url,
        "apply_link":  url,
    })
    return job


def scrape_taleo(page, urls: list[tuple[str,str]]) -> list[dict]:
    """
    Scrape all Taleo portals.
    urls: list of (search_url, section_label) tuples.
    """
    print("\n" + "="*60)
    print("Portals 1 & 2 — Taleo (gsu.taleo.net)")
    print("="*60)

    all_cards = []
    seen_ids  = set()

    for search_url, label in urls:
        cards = _taleo_collect_links(page, search_url, label)
        for c in cards:
            if c["job_id"] not in seen_ids:
                seen_ids.add(c["job_id"])
                all_cards.append(c)

    print(f"\n  Total unique Taleo job links: {len(all_cards)}")
    print(f"  Fetching detail pages …\n")

    already_done = _load_already_fetched(CSV_OUTPUT)
    jobs = []

    for i, card in enumerate(all_cards, 1):
        jid = card.get("job_id", "")
        if jid in already_done:
            print(f"  [{i}/{len(all_cards)}] skip {jid} (already done)", flush=True)
            continue

        print(f"  [{i}/{len(all_cards)}] {card['title'][:55]} …", end=" ", flush=True)
        job = _taleo_fetch_detail(page, card)
        jobs.append(job)
        print("✓", flush=True)

        if i % CHECKPOINT_EVERY == 0:
            _checkpoint(jobs, CSV_OUTPUT)
            print(f"    ── checkpoint ({i} jobs) ──")
        time.sleep(DETAIL_DELAY)

    return jobs


# ─── Portals 3 & 4: Faculty Careers (Interfolio / HigherEdJobs-style) ────────

def _faculty_parse_listing_page(html: str, label: str, seen: set) -> list[dict]:
    """
    Parse one page of Interfolio faculty postings.

    Interfolio (facultycareers.gsu.edu) listing HTML structure:
      <div class="row posting">
        <div class="col-sm-12">
          <h3 class="posting-title">
            <a href="/postings/12345">Position Title</a>
          </h3>
          <div class="posting-category-department">Department Name</div>
          <div class="posting-category-location">Atlanta, Georgia</div>
        </div>
      </div>

    Also handles alternate structures where the title link appears in other
    container divs (e.g. .posting-block, .job-listing).
    """
    soup = BeautifulSoup(html, "lxml")
    cards = []

    # ── Primary: div.row.posting containers ───────────────────────────────────
    for container in soup.select("div.row.posting, div.posting-block, article.posting"):
        # Title link — prefer h3.posting-title > a, then any a[href*="/postings/"]
        a = (container.select_one("h3.posting-title a[href*='/postings/']") or
             container.select_one("h2.posting-title a[href*='/postings/']") or
             container.select_one("a[href*='/postings/']"))
        if not a:
            continue

        href = a["href"]
        full_url = href if href.startswith("http") else urljoin(FACULTY_BASE, href)
        m = re.search(r"/postings/(\d+)", full_url)
        if not m or full_url in seen:
            continue
        job_id = m.group(1)
        seen.add(full_url)

        title = clean(a.get_text())

        # Department: .posting-category-department or similar
        dept_el = (
            container.select_one(".posting-category-department") or
            container.select_one("[class*='department']") or
            container.select_one("[class*='unit']") or
            container.select_one("[class*='college']")
        )
        department = clean(dept_el.get_text()) if dept_el else ""

        # Location: .posting-category-location or default
        loc_el = (
            container.select_one(".posting-category-location") or
            container.select_one("[class*='location']") or
            container.select_one("[class*='campus']")
        )
        location = clean(loc_el.get_text()) if loc_el else "Atlanta, Georgia"

        cards.append({
            "job_id": job_id, "url": full_url,
            "title": title, "department": department,
            "location": location, "posted_date": "",
            "section": label,
        })

    # ── Fallback: any link to /postings/NNN not caught above ──────────────────
    if not cards:
        for a in soup.find_all("a", href=re.compile(r"/postings/\d+")):
            href = a["href"]
            full_url = href if href.startswith("http") else urljoin(FACULTY_BASE, href)
            m = re.search(r"/postings/(\d+)", full_url)
            if not m or full_url in seen:
                continue
            # Skip pagination/nav links (very short text like "1", "2", "Next")
            txt = clean(a.get_text())
            if not txt or re.fullmatch(r"[\d›»<>]+", txt) or txt.lower() in ("next", "prev", "previous"):
                continue
            job_id = m.group(1)
            seen.add(full_url)

            # Try to extract department from parent element
            parent = a.find_parent(["div", "li", "article", "tr"])
            department = ""
            if parent:
                dept_el = parent.find(class_=re.compile(r"depart|unit|college|school", re.I))
                if dept_el:
                    department = clean(dept_el.get_text())

            cards.append({
                "job_id": job_id, "url": full_url,
                "title": txt, "department": department,
                "location": "Atlanta, Georgia", "posted_date": "",
                "section": label,
            })

    return cards


def _faculty_has_next_page(soup: BeautifulSoup, current_page: int) -> bool:
    """
    Return True if a next-page link exists in this Interfolio listing page.
    Interfolio uses:  <a rel="next" href="...?page=N">Next</a>
    or numbered page links like <a href="?page=3">3</a>
    """
    # rel="next"
    if soup.find("a", rel="next"):
        return True
    # link whose text is "Next" (case-insensitive)
    if soup.find("a", string=re.compile(r"^\s*next\s*$", re.I)):
        return True
    # link to page N+1
    if soup.find("a", href=re.compile(rf"[?&]page={current_page + 1}")):
        return True
    return False


def _faculty_collect_links_requests(search_url: str, label: str) -> list[dict]:
    """
    Collect Interfolio faculty job posting links using requests + BeautifulSoup.
    Paginates via `?page=N`.
    """
    links    = []
    seen     = set()
    session  = requests.Session()
    session.headers.update(HEADERS)
    page_num = 1

    while True:
        url = search_url if page_num == 1 else f"{search_url}&page={page_num}"
        if DEBUG: print(f"    Fetching page {page_num}: {url}", flush=True)
        try:
            r = session.get(url, timeout=30)
            if r.status_code != 200:
                print(f"  HTTP {r.status_code} on page {page_num} — stopping.", flush=True)
                break
        except Exception as e:
            print(f"  Request error on page {page_num}: {e} — stopping.", flush=True)
            break

        soup  = BeautifulSoup(r.text, "lxml")
        cards = _faculty_parse_listing_page(r.text, label, seen)
        links.extend(cards)
        print(f"    {label} page {page_num}: +{len(cards)}  total {len(links)}", flush=True)

        if not _faculty_has_next_page(soup, page_num) or not cards:
            break
        page_num += 1
        time.sleep(PAGE_DELAY)

    return links


def _faculty_collect_links_playwright(page, search_url: str, label: str) -> list[dict]:
    """
    Playwright fallback for Interfolio listing collection.
    Used when requests-based collection returns nothing (e.g. JS-rendered content).
    """
    print(f"  Using Playwright for {label} …", flush=True)
    page.goto(search_url, wait_until="load", timeout=60_000)
    page.wait_for_timeout(3000)

    links    = []
    seen     = set()
    page_num = 1

    while True:
        html  = page.content()
        soup  = BeautifulSoup(html, "lxml")
        cards = _faculty_parse_listing_page(html, label, seen)
        links.extend(cards)
        print(f"    {label} page {page_num}: +{len(cards)}  total {len(links)}", flush=True)

        if not _faculty_has_next_page(soup, page_num) or not cards:
            break

        # Click Next
        next_clicked = False
        for nxt_sel in [
            "a[rel='next']",
            "a:has-text('Next')",
            f"a[href*='page={page_num + 1}']",
        ]:
            try:
                loc = page.locator(nxt_sel).first
                if loc.count() > 0 and loc.is_visible():
                    loc.click()
                    page.wait_for_load_state("load")
                    page.wait_for_timeout(2000)
                    next_clicked = True
                    page_num += 1
                    break
            except Exception:
                pass
        if not next_clicked:
            break

    return links


def _faculty_parse_detail(html: str, card: dict) -> dict:
    """
    Parse an Interfolio faculty job detail page into structured fields.

    Interfolio detail page anatomy:
      • h1.posting-header or h1  — job title
      • dl.dl-horizontal (or dl)  — metadata key/value pairs (dt → dd)
      • div.posting-description  — main job description body
      • div.posting-requirements — requirements body (sometimes separate)
      • div#apply-button-top a   — apply link
    """
    soup = BeautifulSoup(html, "lxml")
    job  = empty_job("Georgia State University")

    job["job_id"]      = card.get("job_id", "")
    job["posting_url"] = card.get("url", "")
    job["apply_link"]  = card.get("url", "")
    job["department"]  = card.get("department", "")
    job["location"]    = card.get("location", "Atlanta, Georgia")

    # ── Title ──────────────────────────────────────────────────────────────────
    for sel in [
        "h1.posting-header",
        "h1.job-title",
        ".posting-header",
        "h1",
        ".posting-title",
    ]:
        el = soup.select_one(sel)
        if el:
            t = clean(el.get_text())
            if t:
                job["job_title"] = t
                break
    if not job["job_title"]:
        job["job_title"] = card.get("title", "")

    # ── Metadata: dl/dt/dd pairs ───────────────────────────────────────────────
    # Interfolio puts metadata in <dl> with <dt> labels and <dd> values,
    # sometimes with class="dl-horizontal".
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        for dt in dts:
            label = clean(dt.get_text()).lower().rstrip(": ")
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            val = clean(dd.get_text())
            if not val:
                continue
            if any(k in label for k in ("department", "unit", "college", "school", "division")):
                if not job["department"]: job["department"] = val
            elif any(k in label for k in ("location", "campus", "city")):
                if not job["location"]: job["location"] = val
            elif any(k in label for k in ("position type", "appointment type", "rank", "type")):
                if not job["full_part_time"]: job["full_part_time"] = val
            elif any(k in label for k in ("open date", "posted", "opening")):
                if not job["posted_date"]: job["posted_date"] = val
            elif any(k in label for k in ("close date", "closing", "deadline")):
                extra = f"Close date: {val}"
                job["other_information"] = (
                    (job["other_information"] + "  " + extra).strip()
                    if job["other_information"] else extra
                )

    # ── Apply link ─────────────────────────────────────────────────────────────
    apply_el = (
        soup.select_one("#apply-button-top a[href]") or
        soup.select_one("a[href*='/apply']") or
        soup.find("a", string=re.compile(r"^\s*apply\s*$", re.I))
    )
    if apply_el and apply_el.get("href"):
        href = apply_el["href"]
        job["apply_link"] = href if href.startswith("http") else urljoin(FACULTY_BASE, href)

    # ── Description body ───────────────────────────────────────────────────────
    # Collect all content divs and combine them for section parsing.
    # Order: posting-description first, then posting-requirements.
    desc_parts = []
    for sel in [
        ".posting-description",
        "#posting-description",
        ".job-description",
        ".posting-requirements",
        "#posting-requirements",
    ]:
        el = soup.select_one(sel)
        if el:
            desc_parts.append(str(el))

    if not desc_parts:
        # Generic fallback — everything after the <h1>
        h1 = soup.find("h1")
        if h1:
            desc_parts = [str(s) for s in h1.find_all_next(["div","p","ul","ol","h2","h3","h4"])]
        else:
            desc_parts = [str(soup)]

    sections = parse_html_sections(" ".join(desc_parts))
    job.update(sections)
    return job


def _faculty_fetch_detail(page, card: dict, use_playwright: bool = False) -> dict:
    """Fetch and parse a faculty job detail page."""
    url = card["url"]
    html = None

    if not use_playwright:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and len(r.text) > 5_000:
                html = r.text
        except Exception as e:
            if DEBUG: print(f"    requests failed {url}: {e}")

    if html is None and page is not None:
        try:
            page.goto(url, wait_until="load", timeout=45_000)
            page.wait_for_timeout(2000)
            html = page.content()
        except Exception as e:
            if DEBUG: print(f"    playwright failed {url}: {e}")

    if html:
        try:
            return _faculty_parse_detail(html, card)
        except Exception as e:
            if DEBUG: print(f"    parse error {url}: {e}")

    job = empty_job("Georgia State University")
    job.update({
        "job_id":      card.get("job_id", ""),
        "job_title":   card.get("title", ""),
        "department":  card.get("department", ""),
        "location":    card.get("location", "Atlanta, Georgia"),
        "posting_url": url,
        "apply_link":  url,
    })
    return job


def scrape_faculty(page, portal_urls: list[tuple[str,str]]) -> list[dict]:
    """
    Scrape all faculty career portals.
    portal_urls: list of (search_url, label) tuples.
    """
    print("\n" + "="*60)
    print("Portals 3 & 4 — Faculty Careers (facultycareers.gsu.edu)")
    print("="*60)

    all_cards = []
    seen_ids  = set()

    for search_url, label in portal_urls:
        print(f"\n  Collecting listings: {label} …", flush=True)

        # Try requests first
        cards = _faculty_collect_links_requests(search_url, label)

        # If requests found nothing, try Playwright
        if not cards and page is not None:
            cards = _faculty_collect_links_playwright(page, search_url, label)

        for c in cards:
            if c["job_id"] not in seen_ids:
                seen_ids.add(c["job_id"])
                all_cards.append(c)

    print(f"\n  Total unique faculty job links: {len(all_cards)}")
    print(f"  Fetching detail pages …\n")

    already_done = _load_already_fetched(CSV_OUTPUT)
    jobs = []

    for i, card in enumerate(all_cards, 1):
        jid = card.get("job_id", "")
        if jid in already_done:
            print(f"  [{i}/{len(all_cards)}] skip {jid} (already done)", flush=True)
            continue

        print(f"  [{i}/{len(all_cards)}] {card['title'][:55]} …", end=" ", flush=True)
        job = _faculty_fetch_detail(page, card)
        jobs.append(job)
        print("✓", flush=True)

        if i % CHECKPOINT_EVERY == 0:
            _checkpoint(jobs, CSV_OUTPUT)
            print(f"    ── checkpoint ({i} jobs) ──")
        time.sleep(DETAIL_DELAY)

    return jobs


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
    taleo_only   = "--taleo-only"   in sys.argv
    faculty_only = "--faculty-only" in sys.argv

    print("=" * 60)
    print("Georgia State University Careers Scraper")
    if FULL_REFRESH:
        print("  Mode: FULL REFRESH (--full) — re-fetching all jobs")
    else:
        print("  Mode: INCREMENTAL — keeping existing jobs, fetching new only")
    print("=" * 60)

    # Load all existing jobs first so we can merge after scraping.
    # This prevents previously-fetched jobs from disappearing when they're skipped.
    existing_jobs: dict = _load_existing_jobs(CSV_OUTPUT)
    if existing_jobs:
        print(f"  Loaded {len(existing_jobs)} existing jobs from {CSV_OUTPUT}")

    all_jobs: list[dict] = []

    taleo_portals = [
        (TALEO_STAFF_URL,  "Taleo Staff (section 2)"),
        (TALEO_TEMPS_URL,  "Taleo PanthTemps"),
    ]
    faculty_portals = [
        (FACULTY_ALL_URL, "Faculty (all position types)"),
    ]

    # ── Try to start Playwright (used as fallback for detail pages) ────────────
    # Taleo listing collection is now fully requests-based (no Playwright needed).
    # Playwright is only used as a fallback for detail pages that require JS.
    browser = None
    context = None
    page    = None
    pw_ctx  = None

    try:
        from playwright.sync_api import sync_playwright as _spw
        pw_ctx  = _spw().__enter__()
        browser, context = _make_browser(pw_ctx)
        page = context.new_page()
        print("  Playwright browser started (will be used for JS-heavy detail pages).")
    except Exception as e:
        print(f"  NOTE: Playwright unavailable ({e}).")
        print("  Listing collection uses requests; details also tried via requests.")
        print("  (Install playwright + run 'playwright install chromium' for JS fallback.)")

    try:
        if not faculty_only:
            taleo_jobs = scrape_taleo(page, taleo_portals)
            all_jobs.extend(taleo_jobs)
            print(f"\n  Taleo total: {len(taleo_jobs)} jobs")

        if not taleo_only:
            faculty_jobs = scrape_faculty(page, faculty_portals)
            all_jobs.extend(faculty_jobs)
            print(f"\n  Faculty total: {len(faculty_jobs)} jobs")
    finally:
        if browser:
            try:
                browser.close()
            except Exception:
                pass
        if pw_ctx:
            try:
                pw_ctx.__exit__(None, None, None)
            except Exception:
                pass

    # Merge: start with all existing jobs, then add/overwrite with newly fetched ones.
    # New jobs take precedence (in case a posting was updated).
    merged: dict = {**existing_jobs}
    new_count = 0
    for job in all_jobs:
        jid = job.get("job_id", "").strip()
        if jid and jid not in existing_jobs:
            new_count += 1
        if jid:
            merged[jid] = job
    final_jobs = list(merged.values())

    _save(final_jobs, JSON_OUTPUT, CSV_OUTPUT)
    print(f"\n{'='*60}")
    print(f"Done!  {len(final_jobs)} total Georgia State jobs saved "
          f"({new_count} new, {len(existing_jobs)} carried over).")

    # Summary by portal section
    section_counts: dict[str, int] = {}
    for j in all_jobs:
        sec = j.get("extra_sections", "") or j.get("posting_url", "unknown")
        # Try to attribute by posting_url domain / path
        url = j.get("posting_url", "")
        if "taleo.net" in url and "panthtemps" in url:
            sec = "Taleo PanthTemps"
        elif "taleo.net" in url:
            sec = "Taleo Staff"
        elif "facultycareers" in url:
            sec = "Faculty Careers"
        else:
            sec = "Unknown"
        section_counts[sec] = section_counts.get(sec, 0) + 1

    for sec_label, count in sorted(section_counts.items()):
        print(f"  {sec_label}: {count} jobs")
    print(f"  →  {JSON_OUTPUT}")
    print(f"  →  {CSV_OUTPUT}")


if __name__ == "__main__":
    main()
