"""
USG Jobs Master Merger
======================
Combines output from all four USG scrapers into one unified CSV and JSON:

  Source                  File                Scraper
  ──────────────────────  ──────────────────  ────────────────────
  UGA (PeopleAdmin)       uga_jobs.csv        scraper.py
  GA Tech (OneHCM)        gatech_jobs.csv     scraper_gatech.py
  23 USG (OneHCM)         onehcm_jobs.csv     scraper_onehcm.py
  Georgia State (Taleo)   gastate_jobs.csv    scraper_gastate.py

Output:
  usg_all_jobs.csv   — master CSV, one row per job
  usg_all_jobs.json  — same data as JSON

Master columns
──────────────
  job_id                   unique ID within the source system
  job_title                position title
  institution              university / institution name
  department               department, college, or unit
  location                 city or campus name
  posted_date              date the posting went live
  close_date               application deadline (when available)
  full_part_time           Full-Time / Part-Time (when available)
  regular_temporary        Regular / Temporary (when available)
  employment_type          employee vs student/graduate (UGA)
  salary                   pay / salary info (when available from any source)
  about_us                 about the institution or department
  job_summary              position overview / summary
  responsibilities         duties and responsibilities
  required_qualifications  minimum qualifications
  preferred_qualifications preferred qualifications
  knowledge_skills         knowledge, skills & abilities
  other_information        close dates, notes, etc.
  background_check         background check requirements
  extra_sections           catch-all for unparsed sections
  apply_link               direct application URL
  posting_url              canonical job posting URL
  source                   which scraper produced this row
  scraped_at               ISO timestamp when scraped

Usage:
  python3 merge_all.py            # looks for all four CSVs in same folder
  python3 merge_all.py --summary  # print summary table only
"""

import csv, json, sys, re
from pathlib import Path
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────

HERE = Path(__file__).parent

SOURCES = [
    ("uga",     HERE / "uga_jobs.csv",     "University of Georgia"),
    ("gatech",  HERE / "gatech_jobs.csv",  "Georgia Institute of Technology"),
    ("onehcm",  HERE / "onehcm_jobs.csv",  None),   # institution already in CSV
    ("gastate", HERE / "gastate_jobs.csv", "Georgia State University"),
]

OUTPUT_JSON = HERE / "usg_all_jobs.json"
OUTPUT_CSV  = HERE / "usg_all_jobs.csv"

MASTER_COLUMNS = [
    "job_id", "job_title", "institution", "department", "location",
    "posted_date", "close_date", "full_part_time", "regular_temporary",
    "employment_type", "salary",
    "about_us", "job_summary", "responsibilities",
    "required_qualifications", "preferred_qualifications",
    "knowledge_skills", "other_information", "background_check",
    "extra_sections", "apply_link", "posting_url",
    "source", "scraped_at",
]

# Increase CSV field limit for large description cells
csv.field_size_limit(10_000_000)

# ── Column mappers ─────────────────────────────────────────────────────────────

def _clean(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def _map_uga(row: dict) -> dict:
    """Map UGA (PeopleAdmin) columns → master schema."""
    return {
        "job_id":                   _clean(row.get("posting_number")),
        "job_title":                _clean(row.get("working_title")),
        "institution":              "University of Georgia",
        "department":               _clean(row.get("department")),
        "location":                 _clean(row.get("location")),
        "posted_date":              _clean(row.get("posting_date")),
        "close_date":               _clean(row.get("close_date")),
        "full_part_time":           _clean(row.get("full_part_time")),
        "regular_temporary":        "",          # UGA doesn't separate this way
        "employment_type":          _clean(row.get("employment_type")),
        "salary":                   _clean(row.get("salary")),
        "about_us":                 _clean(row.get("about_department")),
        "job_summary":              _clean(row.get("position_summary")),
        "responsibilities":         _clean(row.get("duties_responsibilities")),
        "required_qualifications":  _clean(row.get("minimum_qualifications")),
        "preferred_qualifications": _clean(row.get("preferred_qualifications")),
        "knowledge_skills":         _clean(row.get("knowledge_skills_abilities")),
        "other_information": " | ".join(filter(None, [
            _clean(row.get("special_instructions")),
            _clean(row.get("physical_demands")),
            (f"FTE: {row['fte']}" if row.get("fte") else ""),
            (f"FLSA: {row['flsa']}" if row.get("flsa") else ""),
            (f"Work schedule: {row['work_schedule']}" if row.get("work_schedule") else ""),
        ])),
        "background_check": " | ".join(filter(None, [
            ("Position of Trust" if row.get("position_of_trust","").lower() == "yes" else ""),
            ("P-Card Required"   if row.get("p_card_required","").lower() == "yes" else ""),
        ])),
        "extra_sections":           "",
        "apply_link":               _clean(row.get("apply_link")),
        "posting_url":              _clean(row.get("posting_url")),
        "source":                   "UGA (PeopleAdmin)",
        "scraped_at":               _clean(row.get("scraped_at")),
    }


def _map_gatech(row: dict) -> dict:
    """Map GA Tech (OneHCM SiteId=03000) columns → master schema."""
    return {
        "job_id":                   _clean(row.get("job_id")),
        "job_title":                _clean(row.get("job_title")),
        "institution":              _clean(row.get("institution")) or "Georgia Institute of Technology",
        "department":               _clean(row.get("department")),
        "location":                 _clean(row.get("location")),
        "posted_date":              _clean(row.get("posted_date")),
        "close_date":               "",
        "full_part_time":           _clean(row.get("full_part_time")),
        "regular_temporary":        _clean(row.get("regular_temporary")),
        "employment_type":          "",
        "salary":                   _clean(row.get("salary", "")),
        "about_us":                 _clean(row.get("about_us")),
        "job_summary":              _clean(row.get("job_summary")),
        "responsibilities":         _clean(row.get("responsibilities")),
        "required_qualifications":  _clean(row.get("required_qualifications")),
        "preferred_qualifications": _clean(row.get("preferred_qualifications")),
        "knowledge_skills":         _clean(row.get("knowledge_skills_abilities")),
        "other_information":        _clean(row.get("other_information")),
        "background_check":         _clean(row.get("background_check")),
        "extra_sections":           _clean(row.get("extra_sections")),
        "apply_link":               _clean(row.get("apply_link")),
        "posting_url":              _clean(row.get("posting_url")),
        "source":                   "Georgia Tech (OneHCM)",
        "scraped_at":               _clean(row.get("scraped_at")),
    }


def _map_onehcm(row: dict) -> dict:
    """Map OneHCM columns → master schema (already close to master)."""
    return {
        "job_id":                   _clean(row.get("job_id")),
        "job_title":                _clean(row.get("job_title")),
        "institution":              _clean(row.get("institution")),
        "department":               _clean(row.get("department")),
        "location":                 _clean(row.get("location")),
        "posted_date":              _clean(row.get("posted_date")),
        "close_date":               "",
        "full_part_time":           _clean(row.get("full_part_time")),
        "regular_temporary":        _clean(row.get("regular_temporary")),
        "employment_type":          "",
        "salary":                   _clean(row.get("salary", "")),
        "about_us":                 _clean(row.get("about_us")),
        "job_summary":              _clean(row.get("job_summary")),
        "responsibilities":         _clean(row.get("responsibilities")),
        "required_qualifications":  _clean(row.get("required_qualifications")),
        "preferred_qualifications": _clean(row.get("preferred_qualifications")),
        "knowledge_skills":         _clean(row.get("knowledge_skills_abilities")),
        "other_information":        _clean(row.get("other_information")),
        "background_check":         _clean(row.get("background_check")),
        "extra_sections":           _clean(row.get("extra_sections")),
        "apply_link":               _clean(row.get("apply_link")),
        "posting_url":              _clean(row.get("posting_url")),
        "source":                   "USG OneHCM",
        "scraped_at":               _clean(row.get("scraped_at")),
    }


def _map_gastate(row: dict) -> dict:
    """Map Georgia State (Taleo + Interfolio) columns → master schema."""
    return {
        "job_id":                   _clean(row.get("job_id")),
        "job_title":                _clean(row.get("job_title")),
        "institution":              "Georgia State University",
        "department":               _clean(row.get("department")),
        "location":                 _clean(row.get("location")),
        "posted_date":              _clean(row.get("posted_date")),
        "close_date":               "",
        "full_part_time":           _clean(row.get("full_part_time")),
        "regular_temporary":        _clean(row.get("regular_temporary")),
        "employment_type":          "",
        "salary":                   _clean(row.get("salary", "")),
        "about_us":                 _clean(row.get("about_us")),
        "job_summary":              _clean(row.get("job_summary")),
        "responsibilities":         _clean(row.get("responsibilities")),
        "required_qualifications":  _clean(row.get("required_qualifications")),
        "preferred_qualifications": _clean(row.get("preferred_qualifications")),
        "knowledge_skills":         _clean(row.get("knowledge_skills_abilities")),
        "other_information":        _clean(row.get("other_information")),
        "background_check":         _clean(row.get("background_check")),
        "extra_sections":           _clean(row.get("extra_sections")),
        "apply_link":               _clean(row.get("apply_link")),
        "posting_url":              _clean(row.get("posting_url")),
        "source":                   "Georgia State University",
        "scraped_at":               _clean(row.get("scraped_at")),
    }


MAPPERS = {
    "uga":     _map_uga,
    "gatech":  _map_gatech,
    "onehcm":  _map_onehcm,
    "gastate": _map_gastate,
}

# ── Loader ────────────────────────────────────────────────────────────────────

def load_csv(path: Path) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


# ── Merge ─────────────────────────────────────────────────────────────────────

def merge_all(summary_only: bool = False) -> list[dict]:
    all_jobs: list[dict] = []
    seen_urls: set[str] = set()          # deduplicate by posting_url
    counts: dict[str, int] = {}

    for key, path, _inst in SOURCES:
        if not path.exists():
            print(f"  [skip] {path.name} not found")
            continue

        raw_rows = load_csv(path)
        mapper   = MAPPERS[key]
        added    = 0

        for raw in raw_rows:
            mapped = mapper(raw)

            # Skip rows with no title (bad data)
            if not mapped.get("job_title"):
                continue

            # Deduplicate by posting URL (fall back to job_id)
            dedup_key = mapped.get("posting_url") or mapped.get("job_id") or ""
            if dedup_key and dedup_key in seen_urls:
                continue
            if dedup_key:
                seen_urls.add(dedup_key)

            all_jobs.append(mapped)
            added += 1

        counts[key] = added
        print(f"  {path.name}: {added} rows loaded")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 50)
    print(f"  Total jobs: {len(all_jobs)}")
    print()

    # Per-institution breakdown
    inst_counts: dict[str, int] = {}
    for j in all_jobs:
        inst = j.get("institution", "Unknown")
        inst_counts[inst] = inst_counts.get(inst, 0) + 1

    print(f"  {'Institution':<45}  {'Jobs':>5}")
    print(f"  {'-'*45}  {'-'*5}")
    for inst, count in sorted(inst_counts.items(), key=lambda x: -x[1]):
        print(f"  {inst:<45}  {count:>5}")

    print()
    source_counts: dict[str, int] = {}
    for j in all_jobs:
        s = j.get("source", "?")
        source_counts[s] = source_counts.get(s, 0) + 1
    print(f"  {'Source system':<35}  {'Jobs':>5}")
    print(f"  {'-'*35}  {'-'*5}")
    for src, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"  {src:<35}  {count:>5}")
    print("=" * 50)

    return all_jobs


# ── Save ──────────────────────────────────────────────────────────────────────

def save(jobs: list[dict]):
    # JSON
    OUTPUT_JSON.write_text(
        json.dumps(jobs, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MASTER_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(jobs)

    print(f"\n  Saved {len(jobs)} records →")
    print(f"    {OUTPUT_JSON}")
    print(f"    {OUTPUT_CSV}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    summary_only = "--summary" in sys.argv

    print("=" * 60)
    print("USG Jobs Master Merger")
    print(f"  Run at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    print()

    jobs = merge_all(summary_only)

    if not summary_only:
        save(jobs)
