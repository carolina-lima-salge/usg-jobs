"""
fetch_usg_news.py
─────────────────
Fetches the latest news from usg.edu/news and saves it as news_data.json
in the repo root. Run by the fetch_usg_news GitHub Action daily.
"""

import json
import re
import sys
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL   = "https://www.usg.edu"
NEWS_URL   = "https://www.usg.edu/news/"
OUT_FILE   = "../news_data.json"   # relative to scrapers/ directory
MAX_PAGES  = 5                     # fetch up to 5 pages of results
PER_PAGE   = 40                    # articles per page target

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; USGJobsBot/1.0; "
        "+https://usgjobs.org)"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Category detection ────────────────────────────────────────────────────────

def detect_category(title: str, desc: str) -> str:
    text = (title + " " + desc).lower()
    if re.search(r'hir|employ|staff|faculty|job|position|recruit|workforce|human resource', text):
        return "hiring"
    if re.search(r'research|grant|study|scientist|lab|clinical|discovery|innovation|stem', text):
        return "research"
    if re.search(r'regent|board|policy|chancellor|budget|legislat|approv|vote|tuition|fee', text):
        return "policy"
    if re.search(r'student|enrollment|graduat|degree|campus|academic|admission|scholarship', text):
        return "student"
    if re.search(r'\bfund\b|award|million|\bgrant\b|gift|donat|invest|endow', text):
        return "funding"
    return "general"

# ── Parse a single page of usg.edu/news ──────────────────────────────────────

def parse_articles(soup: BeautifulSoup) -> list[dict]:
    articles = []

    # Strategy 1: <article> tags (common CMS pattern)
    for el in soup.find_all("article"):
        item = extract_from_element(el)
        if item:
            articles.append(item)

    if articles:
        return articles

    # Strategy 2: Common news-listing class patterns
    selectors = [
        {"class": re.compile(r"news[-_]?item|news[-_]?entry|news[-_]?listing|listing[-_]?item|press[-_]?item", re.I)},
        {"class": re.compile(r"teaser|story|post[-_]?item|release", re.I)},
    ]
    for attrs in selectors:
        els = soup.find_all("div", attrs) + soup.find_all("li", attrs)
        if len(els) >= 3:
            for el in els:
                item = extract_from_element(el)
                if item:
                    articles.append(item)
            if len(articles) >= 3:
                return articles

    # Strategy 3: Find all <h2>/<h3> anchors inside main content
    main = soup.find("main") or soup.find(id=re.compile(r"main|content", re.I)) or soup.body
    if main:
        for heading in main.find_all(["h2", "h3"]):
            link = heading.find("a", href=True)
            if not link:
                link = heading.find_next("a", href=True)
            if not link:
                continue
            href = link.get("href", "")
            if not href or href == "#":
                continue
            full_url = urljoin(BASE_URL, href)
            # Skip nav links — only keep /news/ paths
            if "/news/" not in full_url and "usg.edu" not in full_url:
                continue
            title = heading.get_text(" ", strip=True)
            if len(title) < 10:
                continue
            # Look for a date/description nearby
            parent = heading.parent
            desc_el = parent.find(["p", "span"], class_=re.compile(r"desc|summary|excerpt|teaser", re.I))
            desc = desc_el.get_text(" ", strip=True)[:300] if desc_el else ""
            date_el = parent.find(["time", "span", "div"], class_=re.compile(r"date|time|posted", re.I))
            date_str = ""
            if date_el:
                date_str = date_el.get("datetime") or date_el.get_text(" ", strip=True)
            articles.append({
                "title":    title,
                "link":     full_url,
                "description": desc,
                "pubDate":  normalise_date(date_str),
                "category": detect_category(title, desc),
            })

    return articles


def extract_from_element(el) -> dict | None:
    """Try to extract a news article from a container element."""
    # Find the primary link + title
    link_el  = el.find("a", href=True)
    title_el = el.find(["h1", "h2", "h3", "h4"])
    if not title_el and link_el:
        title_el = link_el
    if not title_el:
        return None

    title = title_el.get_text(" ", strip=True)
    if len(title) < 8:
        return None

    href = ""
    if link_el:
        href = link_el.get("href", "")
    if not href and title_el:
        a = title_el.find("a", href=True)
        if a:
            href = a.get("href", "")
    if not href:
        return None

    full_url = urljoin(BASE_URL, href)

    # Description
    desc = ""
    for tag in el.find_all(["p", "div"], class_=re.compile(r"desc|summary|excerpt|teaser|body|intro", re.I)):
        t = tag.get_text(" ", strip=True)
        if len(t) > 20 and t != title:
            desc = t[:300]
            break
    if not desc:
        # Fallback: first <p> child
        p = el.find("p")
        if p:
            t = p.get_text(" ", strip=True)
            if len(t) > 20 and t != title:
                desc = t[:300]

    # Date
    date_str = ""
    time_el = el.find("time")
    if time_el:
        date_str = time_el.get("datetime") or time_el.get_text(" ", strip=True)
    else:
        date_el = el.find(["span", "div", "p"], class_=re.compile(r"date|time|publish|posted", re.I))
        if date_el:
            date_str = date_el.get("datetime") or date_el.get_text(" ", strip=True)

    return {
        "title":       title,
        "link":        full_url,
        "description": desc,
        "pubDate":     normalise_date(date_str),
        "category":    detect_category(title, desc),
    }


# ── Date normalisation ────────────────────────────────────────────────────────

_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

def normalise_date(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    # Already ISO-ish?
    if re.match(r"\d{4}-\d{2}-\d{2}", raw):
        return raw[:10]
    # "Month DD, YYYY"
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", raw)
    if m:
        mo = _MONTH_MAP.get(m.group(1)[:3].lower())
        if mo:
            return f"{m.group(3)}-{mo:02d}-{int(m.group(2)):02d}"
    # "DD Month YYYY"
    m = re.search(r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})", raw)
    if m:
        mo = _MONTH_MAP.get(m.group(2)[:3].lower())
        if mo:
            return f"{m.group(3)}-{mo:02d}-{int(m.group(1)):02d}"
    # MM/DD/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return raw


# ── Pagination helper ─────────────────────────────────────────────────────────

def find_next_page(soup: BeautifulSoup, current_url: str) -> str | None:
    """Return the URL of the next page, or None."""
    # Look for rel="next"
    link = soup.find("link", rel="next")
    if link and link.get("href"):
        return urljoin(current_url, link["href"])
    # Look for a "Next" button
    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True).lower()
        if text in ("next", "next »", "»", "next page", ">"):
            return urljoin(current_url, a["href"])
    # Look for a pager with a "next" class
    next_a = soup.find("a", class_=re.compile(r"next", re.I))
    if next_a and next_a.get("href"):
        return urljoin(current_url, next_a["href"])
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    all_articles = []
    seen_links   = set()
    url          = NEWS_URL

    print(f"Fetching USG news from {url} …")

    for page_num in range(1, MAX_PAGES + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"  Page {page_num}: fetch error — {e}", file=sys.stderr)
            break

        soup     = BeautifulSoup(resp.text, "lxml")
        articles = parse_articles(soup)
        print(f"  Page {page_num}: found {len(articles)} articles at {url}")

        new_count = 0
        for art in articles:
            key = art["link"].rstrip("/").lower()
            if key in seen_links:
                continue
            seen_links.add(key)
            all_articles.append(art)
            new_count += 1

        if new_count == 0:
            print("  No new articles — stopping pagination.")
            break

        next_url = find_next_page(soup, url)
        if not next_url or next_url == url:
            break
        url = next_url

    if not all_articles:
        print("ERROR: No articles scraped. Keeping existing news_data.json.", file=sys.stderr)
        sys.exit(1)

    # Sort newest first (articles without dates go to the end)
    all_articles.sort(key=lambda a: a.get("pubDate") or "0000", reverse=True)

    output = {
        "meta": {
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source":     NEWS_URL,
            "total":      len(all_articles),
        },
        "articles": all_articles,
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(all_articles)} articles → {OUT_FILE}")


if __name__ == "__main__":
    main()
