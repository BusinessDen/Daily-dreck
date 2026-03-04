#!/usr/bin/env python3
"""
fetch-news.py — RSS feed aggregator for The Daily Dreck news sidebar.
Fetches Denver business news from local and national sources,
deduplicates by title similarity, and outputs the 15 most recent + breaking.
"""

import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.request import urlopen, Request
from urllib.error import URLError

# RSS feeds to monitor
FEEDS = [
    # ── TIER 1: Core Colorado business (no keyword filter) ──
    {"name": "BusinessDen", "short": "BusinessDen", "url": "https://businessden.com/feed/", "filter_keywords": None},
    {"name": "Denver Post", "short": "Denver Post", "url": "https://www.denverpost.com/business/feed/", "filter_keywords": None},
    {"name": "BizWest", "short": "BizWest", "url": "https://bizwest.com/feed/", "filter_keywords": None},
    {"name": "CO Real Estate Journal", "short": "CREJ", "url": "https://crej.com/feed/", "filter_keywords": None},
    {"name": "Bisnow Denver", "short": "Bisnow", "url": "https://www.bisnow.com/rss/denver", "filter_keywords": None},
    {"name": "Mile High CRE", "short": "Mile High CRE", "url": "https://milehighcre.com/feed/", "filter_keywords": None},
    {"name": "Boulder Daily Camera", "short": "Daily Camera", "url": "https://www.dailycamera.com/business/feed/", "filter_keywords": None},

    # ── TIER 2: Colorado general news (business keyword filter) ──
    {"name": "Colorado Sun", "short": "Colorado Sun", "url": "https://coloradosun.com/feed/", "filter_keywords": ["business", "real estate", "restaurant", "retail", "development", "economy", "housing", "commercial", "office", "denver", "foreclosure", "construction", "startup", "investor", "cannabis"]},
    {"name": "Denverite", "short": "Denverite", "url": "https://denverite.com/feed/", "filter_keywords": ["business", "restaurant", "real estate", "development", "economy", "housing", "commercial", "retail", "construction", "office", "startup", "investor"]},
    {"name": "CPR News", "short": "CPR", "url": "https://www.cpr.org/feed/", "filter_keywords": ["business", "economy", "housing", "real estate", "denver", "restaurant", "development", "commercial", "retail"]},
    {"name": "9News", "short": "9News", "url": "https://www.9news.com/feeds/syndication/rss/news", "filter_keywords": ["business", "real estate", "restaurant", "development", "economy", "housing", "commercial", "retail", "denver"]},
    {"name": "Denver7", "short": "Denver7", "url": "https://www.denver7.com/money.rss", "filter_keywords": ["business", "colorado", "denver", "economy", "housing", "real estate", "restaurant", "retail", "development"]},
    {"name": "Fox31 Denver", "short": "Fox31", "url": "https://kdvr.com/feed/", "filter_keywords": ["business", "economy", "restaurant", "real estate", "housing", "development", "retail", "commercial", "denver", "construction"]},
    {"name": "Westword", "short": "Westword", "url": "https://www.westword.com/feed", "filter_keywords": ["business", "restaurant", "real estate", "development", "retail", "housing", "commercial", "economy", "foreclosure", "construction", "cannabis", "marijuana"]},
    {"name": "Colorado Politics", "short": "CO Politics", "url": "https://www.coloradopolitics.com/rss/", "filter_keywords": ["business", "economy", "housing", "real estate", "development", "tax", "budget", "cannabis", "marijuana", "restaurant", "retail", "commercial", "construction"]},
    {"name": "Sentinel Colorado", "short": "Sentinel", "url": "https://sentinelcolorado.com/feed/", "filter_keywords": ["business", "economy", "restaurant", "real estate", "housing", "development", "commercial", "retail", "construction", "aurora"]},

    # ── TIER 3: Niche/blogs (food, development, culture with business angle) ──
    {"name": "Eater Denver", "short": "Eater Denver", "url": "https://denver.eater.com/rss/index.xml", "filter_keywords": None},
    {"name": "Denver Infill", "short": "Denver Infill", "url": "https://denverinfill.com/feed/", "filter_keywords": None},

    # ── TIER 4: Cannabis industry (Colorado keyword filter) ──
    {"name": "MJBizDaily", "short": "MJBizDaily", "url": "https://mjbizdaily.com/feed/", "filter_keywords": ["denver", "colorado", "boulder", "front range", "colorado springs"]},
    {"name": "Marijuana Moment", "short": "MJ Moment", "url": "https://www.marijuanamoment.net/feed/", "filter_keywords": ["denver", "colorado", "boulder", "front range", "colorado springs"]},

    # ── TIER 5: National (Colorado keyword filter) ──
    {"name": "Washington Post", "short": "WaPo", "url": "https://feeds.washingtonpost.com/rss/business", "filter_keywords": ["denver", "colorado", "boulder", "aurora, co", "front range"]},
]

# How far back to look for articles
MAX_AGE_HOURS = 168  # 7 days — catches weekly publications
BREAKING_WINDOW_HOURS = 6
MAX_HEADLINES = 20


def fetch_feed(feed_config):
    """Fetch and parse a single RSS feed. Returns list of article dicts."""
    articles = []
    try:
        req = Request(feed_config["url"], headers={"User-Agent": "DailyDreck/1.0 (BusinessDen internal)"})
        with urlopen(req, timeout=15) as resp:
            raw = resp.read()
        
        root = ET.fromstring(raw)
        
        # Handle both RSS 2.0 and Atom formats
        items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
        
        for item in items:
            title = ""
            link = ""
            pub_date = None
            description = ""

            # RSS 2.0
            if item.find("title") is not None:
                title = item.find("title").text or ""
            if item.find("link") is not None:
                link = item.find("link").text or ""
            if item.find("pubDate") is not None:
                try:
                    pub_date = parsedate_to_datetime(item.find("pubDate").text)
                except (ValueError, TypeError):
                    pub_date = None
            if item.find("description") is not None:
                description = item.find("description").text or ""

            # Atom
            if not title:
                atom_title = item.find("{http://www.w3.org/2005/Atom}title")
                if atom_title is not None:
                    title = atom_title.text or ""
            if not link:
                atom_link = item.find("{http://www.w3.org/2005/Atom}link")
                if atom_link is not None:
                    link = atom_link.get("href", "")
            if pub_date is None:
                atom_date = item.find("{http://www.w3.org/2005/Atom}published")
                if atom_date is None:
                    atom_date = item.find("{http://www.w3.org/2005/Atom}updated")
                if atom_date is not None and atom_date.text:
                    try:
                        pub_date = datetime.fromisoformat(atom_date.text.replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        pub_date = None

            if not title or not pub_date:
                continue

            # Filter by age
            if pub_date.tzinfo is None:
                pub_date = pub_date.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - pub_date
            if age > timedelta(hours=MAX_AGE_HOURS):
                continue

            # Filter by keywords if required
            keywords = feed_config.get("filter_keywords")
            if keywords:
                text_to_check = (title + " " + description).lower()
                if not any(kw in text_to_check for kw in keywords):
                    continue

            articles.append({
                "title": title.strip(),
                "link": link.strip(),
                "source": feed_config["short"],
                "pub_date": pub_date.isoformat(),
                "age_hours": age.total_seconds() / 3600,
            })

    except (URLError, ET.ParseError, Exception) as e:
        print(f"Warning: Failed to fetch {feed_config['name']}: {e}")

    return articles


def normalize_title(title):
    """Normalize a title for deduplication comparison."""
    t = title.lower().strip()
    t = re.sub(r'[^\w\s]', '', t)
    t = re.sub(r'\s+', ' ', t)
    # Remove common prefixes like "BREAKING:" or "UPDATE:"
    t = re.sub(r'^(breaking|update|exclusive|opinion|analysis)\s*:?\s*', '', t)
    return t


def deduplicate(articles):
    """Remove duplicate articles (same story from multiple sources).
    Keeps the version from the most authoritative source."""
    # Priority: local sources first
    source_priority = {
        "BusinessDen": 1, "Denver Post": 2, "Colorado Sun": 3, "BizWest": 4,
        "CPR": 5, "9News": 6, "WaPo": 7,
    }

    seen = {}
    for article in articles:
        norm = normalize_title(article["title"])
        # Check for near-duplicates (titles sharing >60% of words)
        is_dupe = False
        for seen_norm in list(seen.keys()):
            words_a = set(norm.split())
            words_b = set(seen_norm.split())
            if len(words_a) == 0 or len(words_b) == 0:
                continue
            overlap = len(words_a & words_b) / max(len(words_a), len(words_b))
            if overlap > 0.6:
                # Keep the one with higher source priority (lower number)
                existing = seen[seen_norm]
                new_pri = source_priority.get(article["source"], 99)
                old_pri = source_priority.get(existing["source"], 99)
                if new_pri < old_pri:
                    del seen[seen_norm]
                    seen[norm] = article
                is_dupe = True
                break
        if not is_dupe:
            seen[norm] = article

    return list(seen.values())


def classify_breaking(articles):
    """Flag articles as breaking if published within the breaking window."""
    for article in articles:
        article["breaking"] = article["age_hours"] <= BREAKING_WINDOW_HOURS
    return articles


def format_time_ago(age_hours):
    """Convert hours to human-readable time-ago string."""
    if age_hours < 1:
        minutes = int(age_hours * 60)
        return f"{minutes} min ago"
    elif age_hours < 24:
        return f"{int(age_hours)} hrs ago"
    else:
        days = int(age_hours / 24)
        return f"{days} day{'s' if days > 1 else ''} ago"


def main():
    print("=== The Daily Dreck — Fetching news feeds ===")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    all_articles = []
    for feed in FEEDS:
        print(f"Fetching {feed['name']}...")
        articles = fetch_feed(feed)
        print(f"  Found {len(articles)} relevant articles")
        all_articles.extend(articles)

    print(f"\nTotal articles before dedup: {len(all_articles)}")

    # Deduplicate
    all_articles = deduplicate(all_articles)
    print(f"After dedup: {len(all_articles)}")

    # Sort by publication date (newest first)
    all_articles.sort(key=lambda a: a["pub_date"], reverse=True)

    # Classify breaking
    all_articles = classify_breaking(all_articles)

    # Select: all breaking + most recent 15
    breaking = [a for a in all_articles if a["breaking"]]
    non_breaking = [a for a in all_articles if not a["breaking"]]
    selected = breaking + non_breaking[:MAX_HEADLINES - len(breaking)]

    # Add formatted time
    for article in selected:
        article["time_ago"] = format_time_ago(article["age_hours"])

    # Write output
    output = {
        "fetched_date": datetime.now().strftime("%Y-%m-%d"),
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "article_count": len(selected),
        "breaking_count": len(breaking),
        "articles": selected,
    }

    with open("daily-dreck-news.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSelected {len(selected)} headlines ({len(breaking)} breaking)")
    for a in selected[:5]:
        prefix = "[BREAKING] " if a["breaking"] else ""
        print(f"  {prefix}{a['source']}: {a['title'][:70]}...")

    print("\nWritten to daily-dreck-news.json")


if __name__ == "__main__":
    main()
