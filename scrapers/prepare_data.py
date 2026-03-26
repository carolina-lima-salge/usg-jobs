"""
prepare_data.py — Convert usg_all_jobs.csv → jobs.json for the USG Jobs website.

Reads the master CSV and outputs a compact JSON optimised for the static site:
  - Only the fields the site needs (skips huge qualification blocks)
  - Normalises full/part-time and regular/temporary values
  - Adds metadata (last_updated, total count, institution list, locations list)
  - Cleans known data quality issues per source

Usage:
    python3 prepare_data.py                       # auto-finds usg_all_jobs.csv
    python3 prepare_data.py /path/to/jobs.csv     # explicit path
    python3 prepare_data.py --stats               # print stats only, no output

Output:
    jobs.json  (written next to this script)
"""

import csv, json, re, sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(10_000_000)

HERE = Path(__file__).parent

DATE_FORMATS = [
    "%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y",
    "%m-%d-%Y", "%Y/%m/%d",
    "%m/%d/%y",   # 2-digit year e.g. 03/06/26 (GA State)
]

def parse_date_str(s: str) -> str:
    """Normalise any date string to YYYY-MM-DD, or return '' if unparseable."""
    s = (s or "").strip()
    if not s:
        return ""
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else ""

def normalise_fpt(v: str) -> str:
    v = (v or "").strip().lower()
    if "full" in v: return "Full-Time"
    if "part" in v: return "Part-Time"
    return ""

def normalise_rt(v: str) -> str:
    v = (v or "").strip().lower()
    if "regular" in v or "perm" in v: return "Regular"
    if "temp" in v or "contract" in v: return "Temporary"
    return ""

# Strings that look like a salary type but are actually just noise
SALARY_JUNK_PATTERNS = [
    r"^beginning of",
    r"^return to",
    r"^printable",
    r"^- description",
    r"qualifications",
]

def is_garbled(s: str) -> bool:
    """Return True if the string looks like scraped navigation/HTML noise."""
    if not s:
        return False
    low = s.lower().strip()
    # GA State summary/dept starts with these navigation strings
    garbled_starts = [
        "beginning of the main content",
        "return to the home page",
        "- description:",
        "qualifications college/business unit",
    ]
    if any(low.startswith(g) for g in garbled_starts):
        return True
    # GA State new format: "Job Title - 123456 Description: ..." (number + "description")
    if re.search(r'\b\d{5,}\b.*\bdescription\b', low[:80], re.I):
        return True
    return False

def extract_salary_from_extra(extra_sections: str, existing_salary: str) -> str:
    """
    Pull salary out of extra_sections when the main salary field is blank.
    Looks for section labels: [Proposed Salary], [Salary Range], [Shift/Salary/Benefits].
    """
    if existing_salary:
        return existing_salary
    if not extra_sections:
        return ""
    # Each section in extra_sections looks like "[Label] content || [Label2] content2"
    salary_labels = [
        "proposed salary", "salary range", "salary", "pay range",
        "shift/salary/benefits", "compensation",
    ]
    parts = re.split(r'\s*\|\|\s*', extra_sections)
    for part in parts:
        m = re.match(r'\[([^\]]+)\]\s*(.*)', part.strip(), re.DOTALL)
        if not m:
            continue
        label   = m.group(1).strip().lower()
        content = clean(m.group(2))
        if not content:
            continue
        if any(label == sl or label.startswith(sl) for sl in salary_labels):
            return content[:200]
    return ""


# Salary patterns that appear embedded inside description text (not labeled sections)
_TEXT_SALARY_PAT = re.compile(
    r'(?:salary\s*(?:range|:)\s*\$?\s*[\d,]+(?:\.\d+)?'         # "Salary: $53,000"
    r'(?:\s*[-–]\s*\$?[\d,]+(?:\.\d+)?)?)'                       #  optional range
    r'|(?:minimum\s+(?:hourly\s+rate|salary|monthly\s+salary)'   # "Minimum hourly rate $15"
    r'\s+(?:is:?\s*)?\$?\s*[\d,]+(?:\.\d+)?(?:/\w+)?)'
    r'|(?:\$\s*[\d,]+(?:\.\d+)?\s*'                              # "$25.00 per hour"
    r'(?:per\s+(?:hour|month)|/hr|/hour))',
    re.I
)

def extract_salary_from_text(*text_fields: str) -> str:
    """
    Last-resort salary extraction: scan free-text fields for inline salary mentions.
    Only matches explicit patterns (e.g. 'Salary: $53,000', '$15.67 per hour') to
    avoid false positives from grant amounts or enrollment figures.
    """
    for text in text_fields:
        if not text:
            continue
        m = _TEXT_SALARY_PAT.search(text)
        if m:
            return m.group().strip()[:200]
    return ""


def extract_gastate_fields(dept_text: str) -> tuple[str, str]:
    """
    For GA State jobs the scraper embeds the full description in the department
    field. Extract salary and job type from the text before it gets discarded.
    Returns (salary_str, type_str) — either may be empty string.
    """
    salary = ""
    job_type = ""

    # Salary patterns: "Salary : $79,400 (minimum) - $103,200 (midpoint)"
    #                  "Anticipated Hiring Range: $79,400- $103,200"
    sal_pat = re.search(
        r'(?:Salary\s*:|Anticipated Hiring Range\s*:)\s*\$?([\d,]+(?:\.\d+)?)'
        r'(?:\s*\([^)]*\))?\s*[-–]\s*\$?([\d,]+(?:\.\d+)?)',
        dept_text, re.I
    )
    if sal_pat:
        lo, hi = sal_pat.group(1), sal_pat.group(2)
        salary = f"${lo}" + (f" - ${hi}" if hi else "")

    # Type pattern: "Job Type: Full Time (Benefits Eligible)"
    type_pat = re.search(r'Job Type\s*:\s*(Full[- ]?Time|Part[- ]?Time)', dept_text, re.I)
    if type_pat:
        job_type = re.sub(r'[- ]+', '-', type_pat.group(1).strip())  # "Full-Time"

    return salary, job_type


def detect_salary_type(salary: str) -> str:
    """Return 'Hourly', 'Annual', or '' based on salary string."""
    s = salary.lower()
    if not s:
        return ""
    hourly_patterns = ["/hr", "per hour", "/hour", " hourly", "an hour", "$/hr"]
    for pat in hourly_patterns:
        if pat in s:
            return "Hourly"
    # Has digits/dollar signs → likely annual (if no hourly indicator found)
    if re.search(r'[\$\d]', s):
        return "Annual"
    return ""

def clean(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()

def truncate(s: str, n=600) -> str:
    s = clean(s)
    return (s[:n].rsplit(" ", 1)[0] + "…") if len(s) > n else s

def fix_apply_link(apply_link: str, posting_url: str) -> str:
    """Fix broken relative apply links (e.g. GA Tech's '?apply=true')."""
    al = clean(apply_link)
    pu = clean(posting_url)
    # If apply_link is relative or clearly broken, fall back to posting_url
    if not al or al.startswith("?") or not al.startswith("http"):
        return pu or al
    return al


def convert(csv_path: Path) -> dict:
    jobs = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            # Resolve ID (different scrapers use different column names)
            job_id = clean(row.get("job_id") or row.get("posting_number") or "")
            if not job_id:
                continue

            source = clean(row.get("source", ""))

            # Resolve title
            title = clean(row.get("job_title") or row.get("working_title") or "")

            # Department — skip if garbled (GA State scraper artefact: "- Description: ...")
            dept_raw = clean(row.get("department", ""))
            # For GA State, extract salary + type from the blob before discarding it
            gastate_salary, gastate_type = ("", "")
            if source == "Georgia State University" and dept_raw:
                gastate_salary, gastate_type = extract_gastate_fields(dept_raw)
            department = "" if is_garbled(dept_raw) else dept_raw

            # Resolve summary — keep as-is even if imperfect (GA State has navigation
            # chrome but showing it is better than showing nothing)
            summary_raw = clean(
                row.get("job_summary") or
                row.get("position_summary") or ""
            )

            # Responsibilities appended if summary is short
            if len(summary_raw) < 100:
                resp = clean(row.get("responsibilities", ""))
                if resp:
                    summary_raw = (summary_raw + " " + resp).strip()

            # Resolve URLs
            apply_link  = clean(row.get("apply_link", ""))
            posting_url = clean(row.get("posting_url", ""))

            # Fix broken relative links (e.g. GA Tech '?apply=true')
            apply_url = fix_apply_link(apply_link, posting_url)
            # "view" = direct job description page (posting_url preferred, no login wall)
            view_url  = posting_url or apply_url

            # Salary — dedicated column → extra_sections → GA State dept blob → text scan
            salary_raw = (
                extract_salary_from_extra(
                    clean(row.get("extra_sections", "")),
                    clean(row.get("salary", ""))
                )
                or gastate_salary
                or extract_salary_from_text(
                    clean(row.get("job_summary", "")),
                    clean(row.get("other_information", "")),
                    clean(row.get("responsibilities", "")),
                )
            )
            salary_type = detect_salary_type(salary_raw)

            # Full/Part-Time — dedicated column, or GA State dept blob fallback
            # UGA's employment_type = 'Employee' is not a full/part indicator — ignore it
            fpt_raw = clean(row.get("full_part_time", ""))
            job_type = normalise_fpt(fpt_raw) or normalise_fpt(gastate_type)

            # Regular/Temporary
            rt_raw = clean(row.get("regular_temporary", ""))
            employment = normalise_rt(rt_raw)

            # Location + remote flag
            location = clean(row.get("location", ""))
            is_remote = "remote" in location.lower()

            jobs.append({
                "id":          job_id,
                "title":       title,
                "institution": clean(row.get("institution", "")),
                "department":  department,
                "location":    location,
                "remote":      is_remote,
                "posted":      parse_date_str(row.get("posted_date", "")),
                "closes":      parse_date_str(row.get("close_date", "")),
                "type":        job_type,
                "employment":  employment,
                "salary":      salary_raw,
                "salary_type": salary_type,
                "summary":     truncate(summary_raw, 700),
                "view":        view_url,
                "apply":       apply_url,
                "source":      source,
            })

    # Sort newest first (jobs with no date go to end)
    jobs.sort(key=lambda j: j["posted"] or "0000-00-00", reverse=True)

    # Build institution list for dropdown (sorted by job count desc)
    inst_counts = Counter(j["institution"] for j in jobs if j["institution"])
    institutions = [inst for inst, _ in inst_counts.most_common()]

    # Build location list for dropdown (top 40 by frequency, excluding blanks)
    loc_counts = Counter(j["location"] for j in jobs if j["location"])
    locations = [loc for loc, _ in loc_counts.most_common(40)]

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return {
        "meta": {
            "last_updated": today,
            "total":        len(jobs),
            "institutions": institutions,
            "locations":    locations,
        },
        "jobs": jobs,
    }


def main():
    stats_only = "--stats" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]

    if args:
        csv_path = Path(args[0])
    else:
        candidates = [
            HERE / "usg_all_jobs.csv",
            HERE.parent / "usg_scraper" / "usg_all_jobs.csv",
            Path("usg_all_jobs.csv"),
        ]
        csv_path = next((p for p in candidates if p.exists()), None)
        if csv_path is None:
            print("ERROR: Could not find usg_all_jobs.csv")
            print("  Run the scrapers first: bash run_usg_scraper.sh")
            sys.exit(1)

    print(f"Reading: {csv_path.resolve()}")
    data = convert(csv_path)

    if stats_only:
        print(f"\nTotal jobs: {data['meta']['total']:,}")
        print(f"Institutions ({len(data['meta']['institutions'])}):")
        counts = Counter(j["institution"] for j in data["jobs"])
        for inst, n in counts.most_common(10):
            print(f"  {n:4d}  {inst}")
        print(f"\nTop locations:")
        for loc in data['meta']['locations'][:10]:
            print(f"  {loc}")
        # Data quality summary
        print(f"\nData quality:")
        for src in set(j['source'] for j in data['jobs']):
            src_jobs = [j for j in data['jobs'] if j['source'] == src]
            with_sal = sum(1 for j in src_jobs if j['salary'])
            with_type = sum(1 for j in src_jobs if j['type'])
            with_emp = sum(1 for j in src_jobs if j['employment'])
            with_date = sum(1 for j in src_jobs if j['posted'])
            n = len(src_jobs)
            print(f"  {src} ({n}): salary={with_sal}, type={with_type}, employment={with_emp}, date={with_date}")
        return

    out = HERE / "jobs.json"
    out.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")),
                   encoding="utf-8")

    size_kb = out.stat().st_size / 1024
    print(f"Written: {out}  ({size_kb:.0f} KB, {data['meta']['total']:,} jobs)")
    print(f"Institutions: {len(data['meta']['institutions'])}")
    print(f"Locations in dropdown: {len(data['meta']['locations'])}")
    print(f"Last updated: {data['meta']['last_updated']}")


if __name__ == "__main__":
    main()
