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

# Known GSU college / unit names that are NOT job titles.
# These appear as navigation link text in the server-rendered HTML when the
# Interfolio portal requires JavaScript and the requests scraper only gets the
# skeleton page.  If a card title matches one of these, the scrape is broken.
_GSU_UNIT_NAMES = {
    "perimeter college",
    "robinson college of business",
    "andrew young school of policy studies",
    "college of arts & sciences",
    "college of arts and sciences",
    "college of education & human development",
    "college of law",
    "graduate studies",
    "honors college",
    "institute for biomedical sciences",
    "nek college of public health",
    "school of public health",
    "georgia state university",
    "georgia state",
    "gsu",
}


def _faculty_collect_ids_from_html(html: str, label: str, seen: set) -> list[dict]:
    """
    Ultra-reliable fallback: extract /postings/<id> IDs with regex.

    Doesn't depend on CSS structure at all. Creates minimal cards with only
    the job_id and URL. The detail-fetching step fills in all real data via
    the confirmed-working /postings/<id>.json API.
    """
    ids = re.findall(r'/postings/(\d+)', html)
    cards = []
    for pid in dict.fromkeys(ids):   # preserve order, deduplicate
        if pid in seen:
            continue
        seen.add(pid)
        cards.append({
            "job_id":      pid,
            "url":         urljoin(FACULTY_BASE, f"/postings/{pid}"),
            "title":       "",           # filled in by detail JSON fetch
            "department":  "",
            "location":    "Atlanta, Georgia",
            "posted_date": "",
            "section":     label,
        })
    return cards


def _faculty_cards_look_valid(cards: list[dict]) -> bool:
    """
    Return True if the list of cards looks like real job listings.

    Signs that the scraper received broken (JS-gated) HTML:
      • No cards at all.
      • More than half the cards share the same title.
      • Any card title matches a known GSU college / unit name.
      • All card titles are very short (≤ 3 words) — navigation snippet.

    Cards with empty titles (from ID-only extraction) are always treated
    as valid — the detail-fetch step will populate their real titles.
    """
    if not cards:
        return False

    # ID-only cards (empty titles) are valid by definition
    if all(not card.get("title") for card in cards):
        return True

    # Check for known unit names
    for card in cards:
        if card.get("title", "").strip().lower() in _GSU_UNIT_NAMES:
            return False

    # Check for low title diversity (>50% identical)
    from collections import Counter
    title_counts = Counter(card.get("title", "") for card in cards)
    most_common_count = title_counts.most_common(1)[0][1]
    if most_common_count > len(cards) * 0.5:
        return False

    # Check for suspiciously short titles (navigation remnants)
    short_titles = sum(
        1 for card in cards if len(card.get("title", "").split()) <= 2
    )
    if short_titles > len(cards) * 0.5:
        return False

    return True

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


def _faculty_collect_links_json(search_url: str, label: str) -> list[dict]:
    """
    Collect Interfolio faculty job listings via the JSON API.

    Tries two endpoints in order:
      1. /postings.json?page=N&per_page=50  — simple list-all endpoint, most reliable
      2. /postings/search.json?<original_qs>&page=N&per_page=50  — search-based endpoint

    Both return:  {"postings": [...], "meta": {"total_count": N, ...}}
    Each posting has keys: id, position_title, department_name, location,
    open_date, close_date, position_type, ...

    Returns a list of card dicts in the same format used by
    _faculty_parse_listing_page so the rest of the pipeline is unchanged.
    """
    session = requests.Session()
    session.headers.update({**HEADERS, "Accept": "application/json"})

    per = 50  # Interfolio default page size

    # Build two candidate base URLs to try
    if "?" in search_url:
        base_part, qs = search_url.split("?", 1)
        search_json_base = base_part + ".json?" + qs
    else:
        search_json_base = search_url + ".json?"
        qs = ""

    candidate_bases = [
        (f"{FACULTY_BASE}/postings.json", "postings.json"),
        (search_json_base.rstrip("?&"), "search.json"),
    ]

    for json_url_base, api_label in candidate_bases:
        cards  = []
        page   = 1
        failed = False

        print(f"    Trying {api_label} …", flush=True)
        while True:
            sep = "&" if "?" in json_url_base else "?"
            url = f"{json_url_base}{sep}page={page}&per_page={per}"
            try:
                r = session.get(url, timeout=30)
                print(f"    {api_label} page {page}: HTTP {r.status_code}", flush=True)
                if r.status_code != 200:
                    failed = True
                    break
                data = r.json()
            except Exception as e:
                print(f"    {api_label} error: {e}", flush=True)
                failed = True
                break

            postings = data.get("postings", [])
            if not postings:
                print(f"    {api_label} page {page}: empty postings array — done", flush=True)
                break

            for p in postings:
                pid   = str(p.get("id", ""))
                title = (
                    p.get("position_title") or
                    p.get("title") or
                    p.get("name") or ""
                ).strip()
                dept  = (
                    p.get("department_name") or
                    p.get("department") or
                    p.get("college_name") or ""
                ).strip()
                loc    = (p.get("location") or "Atlanta, Georgia").strip()
                posted = (p.get("open_date") or p.get("posted_date") or "").strip()
                url_path = f"/postings/{pid}"
                full_url  = urljoin(FACULTY_BASE, url_path)

                if not pid or not title:
                    continue

                cards.append({
                    "job_id":      pid,
                    "url":         full_url,
                    "title":       title,
                    "department":  dept,
                    "location":    loc,
                    "posted_date": posted,
                    "section":     label,
                })

            print(f"    {api_label} page {page}: +{len(postings)}  total {len(cards)}", flush=True)

            meta        = data.get("meta", {})
            total_count = meta.get("total_count", meta.get("total", 0))
            if total_count and len(cards) >= total_count:
                break
            if len(postings) < per:
                break
            page += 1
            time.sleep(PAGE_DELAY)

        if not failed and cards:
            print(f"    {api_label} succeeded: {len(cards)} listings found.", flush=True)
            return cards

        if failed:
            print(f"    {api_label} failed — trying next endpoint …", flush=True)
        else:
            print(f"    {api_label} returned 0 listings — trying next endpoint …", flush=True)

    return []


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
    Paginates via ?page=N.  Falls back to regex ID extraction if CSS parsing
    finds nothing (the page may be JS-gated but IDs may still appear in markup).
    """
    links    = []
    seen     = set()
    all_html = []
    session  = requests.Session()
    session.headers.update(HEADERS)
    page_num = 1

    while True:
        url = search_url if page_num == 1 else f"{search_url}&page={page_num}"
        try:
            r = session.get(url, timeout=30)
            if r.status_code != 200:
                print(f"    requests HTTP {r.status_code} on page {page_num} — stopping.", flush=True)
                break
        except Exception as e:
            print(f"    requests error on page {page_num}: {e} — stopping.", flush=True)
            break

        all_html.append(r.text)
        soup  = BeautifulSoup(r.text, "lxml")
        cards = _faculty_parse_listing_page(r.text, label, seen)
        links.extend(cards)
        print(f"    requests page {page_num}: CSS found {len(cards)} cards  total {len(links)}", flush=True)

        if not _faculty_has_next_page(soup, page_num) or not cards:
            break
        page_num += 1
        time.sleep(PAGE_DELAY)

    # If CSS parsing found nothing, try ID extraction as fallback
    if not links and all_html:
        print("    CSS parsing found 0 cards — trying regex ID extraction …", flush=True)
        id_seen: set = set()
        for html in all_html:
            links.extend(_faculty_collect_ids_from_html(html, label, id_seen))
        print(f"    Regex extracted {len(links)} posting IDs from HTML", flush=True)

    return links


def _faculty_collect_links_playwright(page, search_url: str, label: str) -> list[dict]:
    """
    Playwright fallback for Interfolio listing collection.
    Uses regex ID extraction instead of CSS selectors so it works regardless
    of how the page renders — the detail JSON API fills in all real job data.
    """
    print(f"  Using Playwright for {label} (regex ID extraction) …", flush=True)
    try:
        page.goto(search_url, wait_until="networkidle", timeout=60_000)
    except Exception:
        page.goto(search_url, wait_until="load", timeout=60_000)
    page.wait_for_timeout(5000)   # give JS time to inject listings

    seen     = set()
    links    = []
    page_num = 1

    while True:
        html  = page.content()
        cards = _faculty_collect_ids_from_html(html, label, seen)
        links.extend(cards)
        print(f"    Playwright page {page_num}: +{len(cards)} IDs  total {len(links)}", flush=True)

        # Try to go to next page
        soup = BeautifulSoup(html, "lxml")
        if not _faculty_has_next_page(soup, page_num):
            break

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
                    page.wait_for_timeout(3000)
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
    # If the detail page returned a college/unit name as the title (e.g. the
    # page's first h1 is a breadcrumb like "Perimeter College"), discard it and
    # fall back to the correct title already scraped from the listing card.
    if not job["job_title"] or job["job_title"].strip().lower() in _GSU_UNIT_NAMES:
        job["job_title"] = card.get("title", "")

    # ── Metadata: dl/dt/dd pairs AND ul.posting-categories li items ────────────
    # Interfolio uses two common structures:
    #   A) <dl> with <dt> label / <dd> value pairs
    #   B) <ul class="posting-categories"> with <li> containing label+value spans

    def _apply_meta(label: str, val: str):
        """Map a label/value pair into the job dict."""
        label = label.lower().rstrip(": ")
        if not val:
            return
        if any(k in label for k in ("department", "unit", "college", "school", "division")):
            if not job["department"]: job["department"] = val
        elif any(k in label for k in ("location", "campus", "city")):
            if not job["location"]: job["location"] = val
        elif any(k in label for k in ("position type", "appointment type", "rank", "type")):
            if not job["full_part_time"]: job["full_part_time"] = val
        elif any(k in label for k in ("salary", "compensation", "pay range", "stipend")):
            if not job["salary"]: job["salary"] = val
        elif any(k in label for k in ("open date", "posted", "opening")):
            if not job["posted_date"]: job["posted_date"] = val
        elif any(k in label for k in ("close date", "closing", "deadline")):
            extra = f"Close date: {val}"
            job["other_information"] = (
                (job["other_information"] + "  " + extra).strip()
                if job["other_information"] else extra
            )

    # Structure A: dl/dt/dd
    for dl in soup.find_all("dl"):
        for dt in dl.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if dd:
                _apply_meta(clean(dt.get_text()), clean(dd.get_text()))

    # Structure B: ul.posting-categories > li with two child spans (label + value)
    for ul in soup.select("ul.posting-categories, ul[class*='posting-cat'], ul[class*='job-meta']"):
        for li in ul.find_all("li"):
            spans = li.find_all("span")
            if len(spans) >= 2:
                _apply_meta(clean(spans[0].get_text()), clean(spans[1].get_text()))
            elif len(spans) == 1:
                # Some portals use <strong> label + trailing text
                strong = li.find(["strong", "b"])
                if strong:
                    label_text = clean(strong.get_text())
                    val_text = clean(li.get_text().replace(strong.get_text(), ""))
                    _apply_meta(label_text, val_text)

    # Structure C: generic label/value divs (e.g. <div class="field-label">Salary</div>)
    for div in soup.select("div[class*='field'], div[class*='meta'], div[class*='posting-info']"):
        label_el = div.select_one("[class*='label'], [class*='key'], strong, b, dt")
        value_el = div.select_one("[class*='value'], [class*='val'], dd, span:not([class*='label'])")
        if label_el and value_el:
            _apply_meta(clean(label_el.get_text()), clean(value_el.get_text()))

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


def _faculty_fetch_detail_json(card: dict) -> dict | None:
    """
    Try to fetch full job details from the Interfolio JSON API.

    Interfolio exposes /postings/{id}.json which returns structured data
    for a single posting — no JS, no Playwright needed.

    Returns a populated job dict on success, None on failure.
    """
    job_id = card.get("job_id", "")
    if not job_id:
        return None
    json_url = f"{FACULTY_BASE}/postings/{job_id}.json"
    try:
        r = requests.get(
            json_url,
            headers={**HEADERS, "Accept": "application/json"},
            timeout=20,
        )
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception:
        return None

    # Interfolio wraps the posting under a "posting" key
    p = data.get("posting") or data
    if not isinstance(p, dict):
        return None

    title = (
        p.get("position_title") or p.get("title") or
        p.get("name") or card.get("title", "")
    ).strip()
    # If the individual JSON returned a college/unit name, prefer the listing
    # card's title (which came from the search-results page and is reliable).
    if title.lower() in _GSU_UNIT_NAMES:
        title = card.get("title", "").strip()
    if not title:
        return None

    # Pull description from body / description fields
    desc_html = (
        p.get("description") or p.get("body") or
        p.get("job_description") or ""
    )
    sections = parse_html_sections(desc_html) if desc_html else {}

    job = empty_job("Georgia State University")
    job.update({
        "job_id":      job_id,
        "job_title":   title,
        "department":  (p.get("department_name") or p.get("department") or
                        p.get("college_name") or card.get("department", "")).strip(),
        "location":    (p.get("location") or card.get("location", "Atlanta, Georgia")).strip(),
        "posted_date": (p.get("open_date") or p.get("posted_date") or
                        card.get("posted_date", "")).strip(),
        "full_part_time": (p.get("position_type") or p.get("appointment_type") or "").strip(),
        "salary":      (p.get("salary") or p.get("salary_range") or
                        p.get("compensation") or p.get("stipend") or "").strip(),
        "posting_url": card["url"],
        "apply_link":  card["url"],
    })
    job.update(sections)
    return job


def _faculty_fetch_detail(page, card: dict, use_playwright: bool = False) -> dict:
    """Fetch and parse a faculty job detail page.

    Strategy (fastest / most reliable first):
      1. Interfolio JSON API  (/postings/{id}.json) — no JS, instant
      2. requests HTML        — works if server-rendered, fast
      3. Playwright           — JS-rendered fallback, slow but thorough
      4. Card-data fallback   — title + date from the search listing
    """
    url = card["url"]
    html = None

    # ── 1. Try JSON API first — no JS needed, most reliable ───────────────────
    json_job = _faculty_fetch_detail_json(card)
    if json_job:
        if DEBUG: print(f"    JSON API detail OK", flush=True)
        return json_job

    # ── 2. Try requests HTML ───────────────────────────────────────────────────
    def _html_is_garbled(h: str) -> bool:
        """Return True if the HTML looks like a JS-gated shell with no real content.

        Interfolio pages always start with 'Toggle navigation' in the nav bar
        even when fully rendered — so we can't use that as the sole signal.
        A skeleton shell page has very little text (< 800 chars total).
        """
        soup = BeautifulSoup(h, "lxml")
        return len(soup.get_text().strip()) < 800

    if not use_playwright:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and len(r.text) > 2_000:
                if _html_is_garbled(r.text):
                    if DEBUG:
                        print(f"    requests HTML is garbled (JS-gated) — trying Playwright")
                else:
                    html = r.text
        except Exception as e:
            if DEBUG: print(f"    requests failed {url}: {e}")

    # ── 3. Playwright fallback ─────────────────────────────────────────────────
    # Playwright always gets the fully rendered page — never treat it as garbled.
    if html is None and page is not None:
        try:
            page.goto(url, wait_until="networkidle", timeout=45_000)
            page.wait_for_timeout(3000)
            html = page.content()
            if len(html) < 2_000:
                if DEBUG:
                    print(f"    Playwright returned almost-empty page — skipping {url}")
                html = None
        except Exception as e:
            if DEBUG: print(f"    playwright failed {url}: {e}")

    # Save first detail page HTML for debugging selector issues
    if html and not Path("debug_faculty_detail_sample.html").exists():
        try:
            Path("debug_faculty_detail_sample.html").write_text(html, encoding="utf-8")
            print(f"    [debug] saved faculty detail HTML sample", flush=True)
        except Exception:
            pass

    if html:
        try:
            job = _faculty_parse_detail(html, card)
            # Back-fill posted_date from card if HTML didn't provide one
            if not job.get("posted_date") and card.get("posted_date"):
                job["posted_date"] = card["posted_date"]
            return job
        except Exception as e:
            if DEBUG: print(f"    parse error {url}: {e}")

    # ── 4. Card-data fallback — always returns something ──────────────────────
    job = empty_job("Georgia State University")
    job.update({
        "job_id":      card.get("job_id", ""),
        "job_title":   card.get("title", ""),
        "department":  card.get("department", ""),
        "location":    card.get("location", "Atlanta, Georgia"),
        "posted_date": card.get("posted_date", ""),   # ← preserve date from JSON listing
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

        # 1. Try Interfolio JSON API (most reliable — returns structured data, no JS)
        cards = _faculty_collect_links_json(search_url, label)

        if cards:
            print(f"  JSON API returned {len(cards)} cards.", flush=True)
        else:
            # 2. Fall back to requests + BeautifulSoup HTML parsing
            print(f"  JSON API returned nothing — trying requests …", flush=True)
            cards = _faculty_collect_links_requests(search_url, label)

        # If requests returned suspicious/broken data (e.g. JS-gated page returned
        # navigation text as titles), fall back to Playwright.
        if not _faculty_cards_look_valid(cards):
            if not cards:
                print(f"  Requests returned no cards — trying Playwright …", flush=True)
            else:
                print(
                    f"  Requests returned {len(cards)} cards but they look invalid "
                    f"(likely JS-gated page). Discarding and trying Playwright …",
                    flush=True,
                )
                cards = []  # discard the bad cards
            if page is not None:
                cards = _faculty_collect_links_playwright(page, search_url, label)
            else:
                print(
                    "  WARNING: Playwright not available — faculty scrape may be incomplete.",
                    flush=True,
                )

        for c in cards:
            if c["job_id"] not in seen_ids:
                # Skip any card whose title is a known college/unit name — these
                # are Interfolio organisational entries, not real job postings.
                card_title = c.get("title", "").strip().lower()
                if card_title and card_title in _GSU_UNIT_NAMES:
                    print(f"  [skip card] '{c['title']}' — unit name, not a job title",
                          flush=True)
                    continue
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
        # The listing card title (from the search-results page) is ALWAYS the
        # most reliable source — the detail page repeatedly returns college/unit
        # names (e.g. "Perimeter College") in its title fields.  Override
        # unconditionally whenever the card has a valid, non-unit title.
        card_title = card.get("title", "").strip()
        if card_title and card_title.lower() not in _GSU_UNIT_NAMES:
            job["job_title"] = card_title
        # Only skip if we genuinely have no usable title at all
        if not job.get("job_title") or job.get("job_title", "").strip().lower() in _GSU_UNIT_NAMES:
            print(f"SKIP — no valid title", flush=True)
            continue
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
