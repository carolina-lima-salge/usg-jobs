"""
UGA Jobs Scraper  —  ugajobsearch.com (PeopleAdmin)
====================================================
Scrapes all open job postings from the University of Georgia's
HR portal at https://www.ugajobsearch.com/postings/search

HOW IT WORKS:
  PeopleAdmin serves fully server-rendered HTML — plain requests +
  BeautifulSoup, no browser needed.

  Detail pages use simple <th>/<td> table rows for every field:
    <tr><th>Working Title</th><td>Sous Chef</td></tr>
  We walk every <tr> on the page, map the <th> label to the correct
  output column, and collect the <td> value.

  Duties/Responsibilities rows repeat; we collect and join them all.

Usage:
    python3 scraper.py               # full run
    python3 scraper.py --debug       # also saves first listing + detail HTML

Output:
    uga_jobs.json
    uga_jobs.csv
"""

import json, csv, re, sys, time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

BASE       = "https://www.ugajobsearch.com"
SEARCH_URL = f"{BASE}/postings/search"
ATOM_URL   = f"{BASE}/postings/all_jobs.atom"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE,
}

PAGE_DELAY   = 1.0
DETAIL_DELAY = 1.5
JSON_OUTPUT  = "uga_jobs.json"
CSV_OUTPUT   = "uga_jobs.csv"
UTC          = timezone.utc
DEBUG        = "--debug" in sys.argv
FULL_REFRESH = "--full"  in sys.argv  # re-fetch every job; default is incremental

# Every field we want to capture, in output order
CSV_COLUMNS = [
    "posting_number",
    "working_title",
    "department",
    "about_department",
    "department_website",
    "posting_type",
    "retirement_plan",
    "employment_type",
    "benefits_eligibility",
    "full_part_time",
    "work_schedule",
    "schedule_info",
    "salary",
    "posting_date",
    "open_until_filled",
    "close_date",
    "proposed_start_date",
    "special_instructions",
    "location",
    "classification_title",
    "flsa",
    "fte",
    "minimum_qualifications",
    "preferred_qualifications",
    "position_summary",
    "knowledge_skills_abilities",
    "physical_demands",
    "driving_required",
    "position_of_trust",
    "financial_responsibility",
    "p_card_required",
    "children_interaction",
    "security_access",
    "duties_responsibilities",
    "contact_name",
    "contact_email",
    "contact_phone",
    "apply_link",
    "posting_url",
    "scraped_at",
]

# Map every <th> label (lowercased, stripped) → CSV column key
# Fields we deliberately skip (boilerplate): About the University, EEO, USG Core Values,
# Credit/Background check policy — these are identical on every posting.
LABEL_MAP = {
    "posting number":                                           "posting_number",
    "working title":                                            "working_title",
    "department":                                               "department",
    "about the college/unit/department":                        "about_department",
    "college/unit/department website":                          "department_website",
    "posting type":                                             "posting_type",
    "retirement plan":                                          "retirement_plan",
    "employment type":                                          "employment_type",
    "benefits eligibility":                                     "benefits_eligibility",
    "full/part time":                                           "full_part_time",
    "work schedule":                                            "work_schedule",
    "additional schedule information":                          "schedule_info",
    "advertised salary":                                        "salary",
    "posting date":                                             "posting_date",
    "job posting date":                                         "posting_date",
    "open until filled":                                        "open_until_filled",
    "closing date":                                             "close_date",
    "proposed starting date":                                   "proposed_start_date",
    "special instructions to applicants":                       "special_instructions",
    "location of vacancy":                                      "location",
    "classification title":                                     "classification_title",
    "flsa":                                                     "flsa",
    "fte":                                                      "fte",
    "minimum qualifications":                                   "minimum_qualifications",
    "preferred qualifications":                                 "preferred_qualifications",
    "position summary":                                         "position_summary",
    "knowledge, skills, abilities and/or competencies":         "knowledge_skills_abilities",
    "physical demands":                                         "physical_demands",
    "is driving a responsibility of this position?":            "driving_required",
    "is this a position of trust?":                             "position_of_trust",
    "does this position have operation, access, or control of financial resources?":
                                                                "financial_responsibility",
    "does this position require a p-card?":                     "p_card_required",
    "is having a p-card an essential function of this position?": "p_card_required",
    "does this position have direct interaction or care of children under the age of 18 or direct patient care?":
                                                                "children_interaction",
    "does this position have security access (e.g., public safety, it security, personnel records, patient records, or access to chemicals and medications)":
                                                                "security_access",
    "recruitment contact name":                                 "contact_name",
    "recruitment contact email":                                "contact_email",
    "recruitment contact phone":                                "contact_phone",
}

# ── Fuzzy label patterns ───────────────────────────────────────────────────────
# Ordered list of (compiled_regex, csv_field).  Used as a fallback when the
# exact LABEL_MAP lookup misses — catches label variations like
# "Job Posting Date" vs "Posting Date" vs "Date Posted", etc.
# Each regex is matched against the cleaned, lowercased, colon-stripped label.
_LABEL_PATTERNS: list[tuple] = [
    # Posting date
    (re.compile(r'(?:job\s+)?posting\s+date|date\s+(?:of\s+)?post(?:ed|ing)|'
                r'position\s+post(?:ed|ing)\s+date|date\s+posted', re.I),
     "posting_date"),
    # Close / deadline
    (re.compile(r'clos(?:e|ing)\s+date|application\s+(?:deadline|close(?:\s+date)?)|'
                r'\bdeadline\b|closes\s+(?:on\b|date)', re.I),
     "close_date"),
    # Salary / pay / compensation / wage
    (re.compile(r'advertised\s+salary|salary(?:\s+range)?|pay\s+(?:rate|range|grade|scale)|'
                r'compensation(?:\s+range)?|\bwage(?:s)?\b', re.I),
     "salary"),
    # Proposed start date
    (re.compile(r'proposed\s+start(?:ing)?\s+date|expected\s+start(?:\s+date)?|'
                r'anticipated\s+start(?:\s+date)?|start\s+date', re.I),
     "proposed_start_date"),
    # Open until filled
    (re.compile(r'open\s+until\s+fill(?:ed)?', re.I),
     "open_until_filled"),
    # Posting type
    (re.compile(r'^posting\s+type$|^position\s+type$|^job\s+type$', re.I),
     "posting_type"),
    # Posting number / ID
    (re.compile(r'posting\s+(?:number|#|no\.?|id)\b', re.I),
     "posting_number"),
    # Working title
    (re.compile(r'(?:working|job|position)\s+title|title\s+of\s+(?:the\s+)?position', re.I),
     "working_title"),
    # Department  ("dept" OR "department" — share "dep" prefix but diverge at 4th char)
    (re.compile(r'^dep(?:t|artment)(?:\s+name)?$', re.I),
     "department"),
    # About department
    (re.compile(r'about\s+the\s+(?:college|unit|department|dept)|'
                r'college[/\s]+unit[/\s]+department', re.I),
     "about_department"),
    # Department website
    (re.compile(r'(?:college|unit|dept|department)\s+(?:web\s*site|url|link)', re.I),
     "department_website"),
    # Employment type
    (re.compile(r'employment\s+type|type\s+of\s+(?:employment|position)', re.I),
     "employment_type"),
    # Retirement plan
    (re.compile(r'retirement\s+plan', re.I),
     "retirement_plan"),
    # Benefits eligibility
    (re.compile(r'benefits?\s+eligib', re.I),
     "benefits_eligibility"),
    # Full / part time — matches "full/part time", "Full Time / Part Time", "Full-Time/Part-Time"
    (re.compile(r'full[\s\-]*time[\s/\-]+part[\s\-]*time|'
                r'full[/\s\-]+part[/\s\-]+time|'
                r'employment\s+(?:status|basis)\b', re.I),
     "full_part_time"),
    # Work schedule
    (re.compile(r'work\s+schedule|hours?\s+(?:per\s+week|schedule)|schedule\s+type', re.I),
     "work_schedule"),
    # Additional schedule info
    (re.compile(r'additional\s+schedule|schedule\s+information', re.I),
     "schedule_info"),
    # Location
    (re.compile(r'location\s+of\s+vacanc|vacanc[y]\s+location|'
                r'work(?:ing)?\s+location|job\s+location|^location$', re.I),
     "location"),
    # Classification title
    (re.compile(r'classification\s+title|job\s+class(?:ification)?', re.I),
     "classification_title"),
    # FLSA
    (re.compile(r'^flsa(?:\s+status)?$', re.I),
     "flsa"),
    # FTE
    (re.compile(r'^fte(?:\s*[:\-].*)?$', re.I),
     "fte"),
    # Minimum qualifications
    (re.compile(r'minimum\s+(?:qualifications?|requirements?)|'
                r'required\s+qualifications?|minimum\s+education', re.I),
     "minimum_qualifications"),
    # Preferred qualifications
    (re.compile(r'preferred\s+(?:qualifications?|requirements?|experience)', re.I),
     "preferred_qualifications"),
    # Position summary / description
    (re.compile(r'position\s+summary|job\s+summary|position\s+description|'
                r'job\s+description|role\s+summary', re.I),
     "position_summary"),
    # Knowledge, skills, abilities
    (re.compile(r'knowledge[,\s]+skills?[,\s&/]+abilit|ksa\b', re.I),
     "knowledge_skills_abilities"),
    # Physical demands
    (re.compile(r'physical\s+(?:demands?|requirements?|effort)', re.I),
     "physical_demands"),
    # Special instructions
    (re.compile(r'special\s+instructions?(?:\s+to\s+applicants?)?', re.I),
     "special_instructions"),
    # Driving
    (re.compile(r'driv(?:ing|e)\s+(?:a\s+)?responsibilit', re.I),
     "driving_required"),
    # Position of trust
    (re.compile(r'position\s+of\s+trust', re.I),
     "position_of_trust"),
    # Financial responsibility
    (re.compile(r'financial\s+resource|operation.*access.*financial|'
                r'financial\s+responsibilit', re.I),
     "financial_responsibility"),
    # P-card
    (re.compile(r'p[\s\-]?card', re.I),
     "p_card_required"),
    # Children / patient interaction
    (re.compile(r'direct\s+(?:interaction|care).*child|child.*direct\s+(?:interaction|care)|'
                r'direct\s+patient\s+care', re.I),
     "children_interaction"),
    # Security access
    (re.compile(r'security\s+access|public\s+safety.*it\s+security|'
                r'personnel\s+records|access\s+to\s+chemicals', re.I),
     "security_access"),
    # Contact name / email / phone  (order matters — name before email/phone)
    (re.compile(r'(?:recruitment\s+)?contact\s+(?:person\s+)?(?:name|person)(?!\s+(?:email|phone))',
                re.I),
     "contact_name"),
    (re.compile(r'(?:recruitment\s+)?contact\s+e?[-\s]?mail', re.I),
     "contact_email"),
    (re.compile(r'(?:recruitment\s+)?contact\s+(?:phone|telephone|tel\.?\b)', re.I),
     "contact_phone"),
]


def resolve_label(label: str):
    """Map a cleaned, lowercased, colon-stripped <th> label to a CSV column.

    Tries exact LABEL_MAP first (O(1), preserves all existing behaviour),
    then falls through to _LABEL_PATTERNS for fuzzy / variant matching.
    Returns None if no match.
    """
    # 1. Exact lookup — fastest path, covers every known label
    if label in LABEL_MAP:
        return LABEL_MAP[label]
    # 2. Regex patterns — handles variants, typos, extra words
    for pat, field in _LABEL_PATTERNS:
        if pat.search(label):
            return field
    return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()

def get(session: requests.Session, url: str) -> requests.Response | None:
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        print(f"  !! GET {url} → {e}")
        return None


# ── Phase 1: collect all job IDs via Atom feed (one request) ─────────────────
#
# UGA's PeopleAdmin exposes /postings/all_jobs.atom which returns every open
# posting in a single Atom XML feed — much faster than paginating the HTML
# search results page by page.  Falls back to HTML pagination if the feed
# fails or returns no entries.

NS = "{http://www.w3.org/2005/Atom}"

def collect_listing_ids(session: requests.Session) -> list[dict]:
    cards = _collect_via_atom(session)
    if cards:
        return cards
    print("  Atom feed empty or failed — falling back to HTML pagination")
    return _collect_via_html(session)


def _collect_via_atom(session: requests.Session) -> list[dict]:
    """Fetch all posting IDs from the Atom feed in a single HTTP request."""
    print(f"  Fetching Atom feed: {ATOM_URL}")
    r = get(session, ATOM_URL)
    if not r:
        return []
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        print(f"  !! Atom XML parse error: {e}")
        return []

    cards = []
    seen  = set()
    for entry in root.findall(f"{NS}entry"):
        # <id> = tag:ugajobsearch.com,2005:/postings/473770
        id_tag = entry.findtext(f"{NS}id", "")
        pid_m  = re.search(r"/postings/(\d+)", id_tag)
        if not pid_m:
            continue
        pid = pid_m.group(1)
        if pid in seen:
            continue
        seen.add(pid)

        title = clean(entry.findtext(f"{NS}title", ""))
        link  = ""
        for lnk in entry.findall(f"{NS}link"):
            if lnk.get("rel", "alternate") == "alternate":
                link = lnk.get("href", "")
                break
        if not link:
            link = urljoin(BASE, f"/postings/{pid}")

        # published date lives in <updated> or <published>
        pub = (entry.findtext(f"{NS}published", "") or
               entry.findtext(f"{NS}updated",   ""))[:10]  # YYYY-MM-DD

        cards.append({
            "posting_id":     pid,
            "posting_url":    link,
            "working_title":  title,
            "posting_number": "",   # filled from detail page
            "department":     "",
            "position_type":  "",
            "close_date":     "",
            "atom_published": pub,  # seed date; overwritten by detail page
        })

    print(f"  Atom feed: {len(cards)} postings found")
    return cards


def _collect_via_html(session: requests.Session) -> list[dict]:
    """Fallback: paginate HTML search results to collect posting IDs."""
    cards = []
    seen  = set()
    page  = 1

    while True:
        url = SEARCH_URL if page == 1 else f"{SEARCH_URL}?page={page}"
        print(f"  Listing page {page}: {url}")
        r = get(session, url)
        if not r:
            break

        if DEBUG and page == 1:
            with open("debug_listing_p1.html", "w", encoding="utf-8") as f:
                f.write(r.text)

        s   = BeautifulSoup(r.text, "lxml")
        new = 0

        for row in s.find_all("div", class_="row"):
            a = row.find("a", href=re.compile(r"^/postings/\d+$"))
            if not a:
                continue
            pid = re.search(r"/postings/(\d+)", a["href"]).group(1)
            if pid in seen:
                continue
            seen.add(pid)
            new += 1

            cols = row.find_all("div", class_=re.compile(r"col-md-2"))
            def col(i): return clean(cols[i].get_text()) if len(cols) > i else ""

            cards.append({
                "posting_id":      pid,
                "posting_url":     urljoin(BASE, a["href"]),
                "working_title":   clean(a.get_text()),
                "posting_number":  col(1),
                "department":      col(2),
                "position_type":   col(3),
                "close_date":      col(4),
            })

        print(f"    +{new} cards  (total: {len(cards)})")
        next_link = s.find("a", string=re.compile(r"^Next$", re.I))
        if not next_link or not new:
            break
        page += 1
        time.sleep(PAGE_DELAY)

    return cards


# ── Phase 2: scrape each detail page ─────────────────────────────────────────

def scrape_detail(session: requests.Session, card: dict, idx: int) -> dict:
    url = card["posting_url"]

    # Seed with listing-level data (atom_published is a fallback posting date)
    job = {k: "" for k in CSV_COLUMNS}
    job.update({
        "posting_url":    url,
        "scraped_at":     datetime.now(UTC).isoformat(),
        "working_title":  card.get("working_title", ""),
        "posting_number": card.get("posting_number", ""),
        "department":     card.get("department", ""),
        "close_date":     card.get("close_date", ""),
        "posting_date":   card.get("atom_published", ""),  # overwritten if detail has it
    })

    r = get(session, url)
    if not r:
        return job

    if DEBUG and idx == 1:
        with open("debug_detail_p1.html", "w", encoding="utf-8") as f:
            f.write(r.text)

    s = BeautifulSoup(r.text, "lxml")

    # ── Extract every <th>/<td> pair ──────────────────────────────────────────
    # All structured data on PeopleAdmin detail pages lives in plain tables.
    # Duties repeat, so we collect them all and join.
    duties_list = []

    for tr in s.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue

        label = clean(th.get_text()).rstrip(":").lower()
        value = clean(td.get_text())

        if not value:
            continue

        # Special case: Duties/Responsibilities repeat; collect all
        if label == "duties/responsibilities":
            duties_list.append(value)
            continue

        # Skip "Percentage of time" rows (they're interleaved with duties)
        if label == "percentage of time":
            if duties_list:
                duties_list[-1] += f"  [{value}% of time]"
            continue

        # Map label → field (exact first, then regex fallback)
        field = resolve_label(label)
        if field and not job.get(field):
            job[field] = value

    # Join all duty blocks
    if duties_list:
        job["duties_responsibilities"] = " | ".join(duties_list)

    # ── Apply link ────────────────────────────────────────────────────────────
    apply_a = s.find("a", href=re.compile(r"/pre_apply|/apply", re.I))
    if apply_a:
        href = apply_a["href"]
        job["apply_link"] = href if href.startswith("http") else urljoin(BASE, href)
    else:
        job["apply_link"] = url  # fallback to posting URL

    return job


# ── Incremental helpers ───────────────────────────────────────────────────────

def _load_existing_jobs(csv_path: str) -> dict:
    """Load all existing rows from uga_jobs.csv keyed by posting_number AND posting_url.

    Two keys per row so we can dedup both HTML-fallback cards (which carry a
    posting_number from the listing page) AND Atom-feed cards (which have an
    empty posting_number but a valid posting_url).
    """
    if FULL_REFRESH:
        return {}
    p = Path(csv_path)
    if not p.exists():
        return {}
    existing = {}
    try:
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pid = row.get("posting_number", "").strip()
                url = row.get("posting_url",    "").strip()
                if pid:
                    existing[pid] = row
                if url:
                    existing[url] = row   # secondary key — used for Atom-feed dedup
    except Exception:
        pass
    return existing


# ── Main orchestrator ─────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    session = requests.Session()
    session.headers.update(HEADERS)

    print("=" * 60)
    print("Phase 1: Collecting job listings...")
    if FULL_REFRESH:
        print("  Mode: FULL REFRESH (--full) — re-fetching all jobs")
    else:
        print("  Mode: INCREMENTAL — keeping existing jobs, fetching new only")
    print("=" * 60)

    # Load existing jobs before hitting the network
    existing_jobs = _load_existing_jobs(CSV_OUTPUT)
    if existing_jobs:
        print(f"  Loaded {len(existing_jobs)} existing jobs from {CSV_OUTPUT}")

    cards = collect_listing_ids(session)
    if not cards:
        print("No listings found.")
        # Deduplicate existing_jobs.values(): dual-keyed dict means same row
        # appears under both posting_number key AND posting_url key.
        seen_keys: set = set()
        result = []
        for job in existing_jobs.values():
            k = job.get("posting_number", "").strip() or job.get("posting_url", "").strip()
            if k and k not in seen_keys:
                seen_keys.add(k)
                result.append(job)
        return result

    # Filter to only cards we haven't already scraped.
    # Check BOTH posting_number (HTML cards) AND posting_url (Atom feed cards,
    # which always have posting_number="" so the number check alone never filters them).
    new_cards = [
        c for c in cards
        if (c.get("posting_number", "").strip() not in existing_jobs
            and c.get("posting_url",    "").strip() not in existing_jobs)
    ]
    skipped   = len(cards) - len(new_cards)
    print(f"\nTotal listings: {len(cards)}  |  New: {len(new_cards)}  |  Skipped (already have): {skipped}")

    print("\n" + "=" * 60)
    print(f"Phase 2: Fetching details for {len(new_cards)} new jobs...")
    print("=" * 60 + "\n")

    new_jobs = []
    for i, card in enumerate(new_cards, 1):
        print(f"[{i}/{len(new_cards)}]  {card['working_title']}")
        print(f"            {card.get('department','')}  |  {card.get('position_type','')}  |  closes: {card.get('close_date','')}")

        job = scrape_detail(session, card, i)
        new_jobs.append(job)

        filled = sum(1 for k in CSV_COLUMNS if job.get(k))
        print(f"  ✓ {filled}/{len(CSV_COLUMNS)} fields"
              f"  |  summary: {'✓' if job.get('position_summary') else '✗'}"
              f"  |  min_qual: {'✓' if job.get('minimum_qualifications') else '✗'}"
              f"  |  apply: {'✓' if job.get('apply_link') else '✗'}")
        print()

        time.sleep(DETAIL_DELAY)

    # Merge: new jobs take precedence; existing fill in the rest.
    # NOTE: existing_jobs is dual-keyed (posting_number + posting_url → same row).
    # We cannot simply do {**existing_jobs} because that would include each row
    # under two keys, causing list(merged.values()) to yield duplicates.
    # Instead, build merged with one canonical key per job.
    merged: dict = {}

    for job in new_jobs:
        pid = job.get("posting_number", "").strip()
        url = job.get("posting_url",    "").strip()
        key = pid or url
        if key:
            merged[key] = job

    carried_over = 0
    for job in existing_jobs.values():
        pid = job.get("posting_number", "").strip()
        url = job.get("posting_url",    "").strip()
        key = pid or url
        if key and key not in merged:
            merged[key] = job
            carried_over += 1

    print(f"\nTotal after merge: {len(merged)} jobs  ({len(new_jobs)} new + {carried_over} existing)")
    return list(merged.values())


# ── Output ────────────────────────────────────────────────────────────────────

def save_json(jobs, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(jobs)} records → {path}")

def save_csv(jobs, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(jobs)
    print(f"Saved {len(jobs)} records → {path}")


# ── Verify against known posting (run: python3 scraper.py --test) ────────────

def test_parse():
    """Quick offline test using debug_detail_471776.html if present."""
    import os
    path = "debug_detail_471776.html"
    if not os.path.exists(path):
        # Try uploads folder
        path = "/sessions/ecstatic-hopeful-carson/mnt/uploads/debug_detail_471776.html"
    with open(path, encoding="utf-8") as f:
        html = f.read()
    s = BeautifulSoup(html, "lxml")
    job = {k: "" for k in CSV_COLUMNS}
    job["posting_url"] = "https://www.ugajobsearch.com/postings/471776"
    duties_list = []
    for tr in s.find_all("tr"):
        th, td = tr.find("th"), tr.find("td")
        if not th or not td:
            continue
        label = clean(th.get_text()).rstrip(":").lower()
        value = clean(td.get_text())
        if not value:
            continue
        if label == "duties/responsibilities":
            duties_list.append(value)
            continue
        if label == "percentage of time":
            if duties_list:
                duties_list[-1] += f"  [{value}% of time]"
            continue
        field = resolve_label(label)
        if field and not job.get(field):
            job[field] = value
    if duties_list:
        job["duties_responsibilities"] = " | ".join(duties_list)
    apply_a = s.find("a", href=re.compile(r"/pre_apply|/apply", re.I))
    if apply_a:
        href = apply_a["href"]
        job["apply_link"] = href if href.startswith("http") else urljoin(BASE, href)

    print("=== PARSE TEST: postings/471776 ===\n")
    for col in CSV_COLUMNS:
        val = job.get(col, "")
        status = "✓" if val else "✗"
        print(f"  {status} {col:40s} {val[:80]!r}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--test" in sys.argv:
        test_parse()
        sys.exit(0)

    print("=" * 60)
    print("UGA Jobs Scraper  —  ugajobsearch.com (PeopleAdmin)")
    if DEBUG: print("[DEBUG mode]")
    print("=" * 60 + "\n")

    jobs = scrape()
    if jobs:
        save_json(jobs, JSON_OUTPUT)
        save_csv(jobs, CSV_OUTPUT)
        print(f"\nDone. {len(jobs)} postings collected.")
    else:
        print("\nNo jobs collected. Run with --debug to inspect HTML.")
