#!/usr/bin/env python3
"""
fetch-news.py — RSS feed aggregator for The Daily Dreck news sidebar.
Fetches Denver business news from local and national sources,
filters out sports game coverage, deduplicates by title similarity,
and outputs the 20 most recent headlines.
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

# ── SPORTS CONTENT FILTER ──
# Patterns that indicate sports game coverage (not sports business)
SPORTS_GAME_PATTERNS = [
    # Game results and scores
    r'\b\d+[-–]\d+\b',  # Score patterns like "3-2", "24–17"
    r'\bfinal\s*:', r'\bscore\b', r'\bbox\s*score\b',
    r'\bgame\s*recap\b', r'\bpostgame\b', r'\bpregame\b',
    # Win/loss language
    r'\b(beat|beats|defeated|defeats|routs|rout|swept|sweeps)\s',
    r'\b(wins|loses|lost|won)\s+(over|to|against)\b',
    r'\b(shut\s*out|walk[- ]?off|overtime|shootout|extra\s*innings?)\b',
    # Player/game performance
    r'\b(touchdown|home\s*run|three[- ]?pointer|goal|assist|interception|sack|strikeout)s?\b',
    r'\b(rushing|passing|batting|pitching)\s+(yards?|average|stats?)\b',
    r'\b(mvp|all[- ]?star|pro\s*bowl|all[- ]?pro)\b',
    r'\b(roster|starting\s*lineup|injury\s*report|game[- ]?day|matchup)\b',
    r'\b(first\s*quarter|second\s*half|fourth\s*quarter|first\s*period|third\s*period)\b',
    # Playoff/season language
    r'\b(playoff|postseason|preseason|regular\s*season|wild\s*card|divisional)\b',
    r'\b(nfl\s*draft|trade\s*deadline|free\s*agent|waiver)\b',
    # Specific game contexts
    r'\b(kicks?\s*off|tips?\s*off|first\s*pitch)\b',
]

# Terms that indicate sports BUSINESS (should NOT be filtered)
SPORTS_BUSINESS_TERMS = [
    "stadium", "arena", "naming rights", "franchise", "relocation",
    "development", "construction", "billion", "million", "investment",
    "taxpayer", "bond", "financing", "lease", "sale", "sold",
    "owner", "ownership", "valuation", "revenue", "sponsor",
    "headquarters", "training facility", "real estate",
]

# RSS categories that are clearly non-business
NON_BUSINESS_CATEGORIES = {
    "sports", "sport", "football", "basketball", "baseball", "hockey",
    "soccer", "nfl", "nba", "mlb", "nhl", "mls", "ncaa",
    "broncos", "nuggets", "avalanche", "rockies", "rapids", "mammoth",
    "weather", "forecast", "crime", "police", "fire",
    "entertainment", "celebrity", "movies", "tv", "television",
    "obituaries", "obituary", "comics", "puzzles", "horoscope",
    "opinion", "letters to the editor", "editorials",
}


def is_sports_game_content(title, description="", categories=None):
    """Check if an article is about a sporting event (not sports business)."""
    text = (title + " " + description).lower()

    # First check: if it has sports business terms, allow it through
    if any(term in text for term in SPORTS_BUSINESS_TERMS):
        return False

    # Second check: if RSS categories indicate sports/non-business
    if categories:
        lower_cats = {c.lower().strip() for c in categories}
        if lower_cats & NON_BUSINESS_CATEGORIES:
            # Category is non-business, but double-check for business angle
            if not any(term in text for term in SPORTS_BUSINESS_TERMS):
                return True

    # Third check: match against sports game patterns
    for pattern in SPORTS_GAME_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return True

    return False


def extract_categories(item):
    """Extract category tags from an RSS item."""
    categories = []

    # RSS 2.0 <category> tags
    for cat in item.findall("category"):
        if cat.text:
            categories.append(cat.text.strip())

    # Dublin Core subject
    dc_ns = "{http://purl.org/dc/elements/1.1/}"
    for subj in item.findall(f"{dc_ns}subject"):
        if subj.text:
            categories.append(subj.text.strip())

    # Atom categories
    atom_ns = "{http://www.w3.org/2005/Atom}"
    for cat in item.findall(f"{atom_ns}category"):
        term = cat.get("term") or cat.get("label") or cat.text
        if term:
            categories.append(term.strip())

    return categories


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

            # Extract RSS categories
            categories = extract_categories(item)

            # Filter by keywords if required
            keywords = feed_config.get("filter_keywords")
            if keywords:
                text_to_check = (title + " " + description + " " + " ".join(categories)).lower()
                if not any(kw in text_to_check for kw in keywords):
                    continue

            # Filter out sports game content
            if is_sports_game_content(title, description, categories):
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
    # Priority: local business-first sources ranked highest
    source_priority = {
        "Denver Post": 1, "BizWest": 2, "Colorado Sun": 3,
        "CREJ": 4, "Bisnow": 5, "Mile High CRE": 6, "Daily Camera": 7,
        "Denverite": 8, "CPR": 9, "9News": 10, "Denver7": 11, "Fox31": 12,
        "Westword": 13, "CO Politics": 14, "Sentinel": 15,
        "Eater Denver": 16, "Denver Infill": 17,
        "MJBizDaily": 18, "MJ Moment": 19, "WaPo": 20,
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

    # Select top headlines — strictly newest first, breaking is just a badge
    selected = all_articles[:MAX_HEADLINES]

    # Add formatted time
    for article in selected:
        article["time_ago"] = format_time_ago(article["age_hours"])

    breaking_count = sum(1 for a in selected if a["breaking"])

    # Write output
    output = {
        "fetched_date": datetime.now().strftime("%Y-%m-%d"),
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "article_count": len(selected),
        "breaking_count": breaking_count,
        "articles": selected,
    }

    with open("daily-dreck-news.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSelected {len(selected)} headlines ({breaking_count} breaking)")
    for a in selected[:5]:
        prefix = "[BREAKING] " if a["breaking"] else ""
        print(f"  {prefix}{a['source']}: {a['title'][:70]}...")

    print("\nWritten to daily-dreck-news.json")


if __name__ == "__main__":
    main()
