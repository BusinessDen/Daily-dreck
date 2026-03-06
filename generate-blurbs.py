#!/usr/bin/env python3
"""
generate-blurbs.py — Daily Dreck Blurb Generator

Called by GitHub Actions each morning. Reads data from each live tool,
sends it to the Claude API for editorial analysis, and writes
daily-dreck-blurbs.json for the landing page to consume.

Requires: ANTHROPIC_API_KEY in environment
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request
import urllib.error

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-5-20250929"
MAX_TOKENS = 2500

# ============================================
# DATA LOADERS
# Each function loads the relevant data file and returns
# a summary dict the prompt can use
# ============================================

def fetch_json_url(url):
    """Fetch JSON from a URL. Returns parsed data or None on failure."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "DailyDreck/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, json.JSONDecodeError, Exception) as e:
        print(f"Warning: Could not fetch {url}: {e}")
        return None


def load_restaurant_data():
    """Fetch restaurant-data.json from GitHub Pages and compute key metrics.
    Uses the 'changes' array as the source of truth for openings/closures."""
    data = fetch_json_url("https://businessden.github.io/Restaurant-tracker/restaurant-data.json")
    if not data:
        return None

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    fourteen_days_ago = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    restaurants = data.get("restaurants", [])
    changes = data.get("changes", [])

    # Count from changes array
    # "opened"/"closed"/"reopened" = verified; "_unclear" suffix = unverified leads
    def count_changes(start, end):
        opens_verified = 0
        closes_verified = 0
        opens_unverified = 0
        closes_unverified = 0
        temp_closed = 0
        reopened = 0
        verified_events = []
        all_events = []
        for c in changes:
            d = c.get("date", "")
            ctype = c.get("type", "")
            if d < start or d > end:
                continue
            if ctype == "opened":
                opens_verified += 1
                verified_events.append(c)
            elif ctype == "opened_unclear":
                opens_unverified += 1
            elif ctype == "closed":
                closes_verified += 1
                verified_events.append(c)
            elif ctype == "closed_unclear":
                closes_unverified += 1
            elif ctype == "temporarily_closed":
                temp_closed += 1
            elif ctype == "reopened":
                reopened += 1
                verified_events.append(c)
            all_events.append(c)
        return {
            "opens_verified": opens_verified,
            "closes_verified": closes_verified,
            "opens_unverified": opens_unverified,
            "closes_unverified": closes_unverified,
            "temp_closed": temp_closed,
            "reopened": reopened,
            "verified_events": verified_events,
            "all_events": all_events,
        }

    today_data = count_changes(today, today)
    # If today has no data yet (scraper hasn't run), use yesterday
    if today_data["opens_verified"] == 0 and today_data["closes_verified"] == 0 and today_data["opens_unverified"] == 0 and today_data["closes_unverified"] == 0:
        today_data = count_changes(yesterday, yesterday)

    week_data = count_changes(seven_days_ago, today)
    prev_week_data = count_changes(fourteen_days_ago, seven_days_ago)
    month_data = count_changes(thirty_days_ago, today)

    # Extract recent verified events
    latest_events = sorted(week_data["verified_events"], key=lambda x: x.get("date", ""), reverse=True)[:10]
    formatted_events = []
    for e in latest_events:
        formatted_events.append({
            "name": e.get("name", "Unknown"),
            "type": e.get("type", ""),
            "date": e.get("date", ""),
            "neighborhood": e.get("neighborhood", ""),
            "cuisine": e.get("cuisine", ""),
            "address": e.get("address", ""),
        })

    # Fetch Claude-reviewed leads from claude-reviews.json
    reviews_data = fetch_json_url("https://businessden.github.io/Restaurant-tracker/claude-reviews.json")
    leads = []
    leads_today = 0
    leads_today_openings = 0
    leads_today_closures = 0
    leads_openings = 0
    leads_closures = 0
    if reviews_data:
        all_reviews = reviews_data.get("reviews", [])
        leads = [r for r in all_reviews if r.get("verdict") == "valid_lead"]
        for lead in leads:
            ct = lead.get("change_type", "")
            is_opening = ct in ("opened", "opened_unclear")
            is_closure = ct in ("closed", "closed_unclear", "temporarily_closed")
            if is_opening:
                leads_openings += 1
            elif is_closure:
                leads_closures += 1
            if lead.get("change_date", "") == today or lead.get("change_date", "") == yesterday:
                leads_today += 1
                if is_opening:
                    leads_today_openings += 1
                elif is_closure:
                    leads_today_closures += 1

    formatted_leads = []
    for r in leads:
        formatted_leads.append({
            "name": r.get("name", "Unknown"),
            "change_type": r.get("change_type", ""),
            "change_date": r.get("change_date", ""),
            "neighborhood": r.get("neighborhood", ""),
            "cuisine": r.get("cuisine", ""),
            "confidence": r.get("confidence", 0),
            "summary": r.get("summary", "")[:200],
        })

    last_scrape = data.get("metadata", {}).get("last_scrape", "unknown")

    return {
        "openings_verified_7d": week_data["opens_verified"],
        "closures_verified_7d": week_data["closes_verified"],
        "total_tracked": len(restaurants),
        "latest_verified": formatted_events,
        "leads_total": len(leads),
        "leads_today": leads_today,
        "leads_today_openings": leads_today_openings,
        "leads_today_closures": leads_today_closures,
        "leads_openings": leads_openings,
        "leads_closures": leads_closures,
        "leads_detail": formatted_leads,
        "last_scrape": last_scrape,
    }


def load_foreclosure_data():
    """Fetch foreclosure-data.json from GitHub Pages and compute key metrics.
    Data is a flat array of records. Key date fields:
    - first_publication_date: when the NED notice was first published (best proxy for "filing date")
    - ned_recorded_date: when NED was recorded at county
    - scheduled_sale_date: upcoming auction date
    Financial fields: original_loan_amount, total_due, winning_bid
    Status: "sold", "continued"
    """
    data = fetch_json_url("https://businessden.github.io/Colorado-foreclosure/foreclosure-data.json")
    if not data:
        return None

    records = data if isinstance(data, list) else data.get("records", data.get("foreclosures", []))

    today = datetime.now().strftime("%Y-%m-%d")
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    fourteen_days_ago = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    seven_days_ahead = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")

    def get_amount(rec):
        """Get the best available dollar amount."""
        for field in ["total_due", "winning_bid", "original_loan_amount"]:
            val = rec.get(field)
            if val and isinstance(val, (int, float)) and val > 0:
                return val
        return 0

    def get_filing_date(rec):
        """Best available date representing when this case entered the system."""
        return rec.get("first_publication_date") or rec.get("ned_recorded_date") or ""

    # Count filings by first_publication_date
    def filed_in_window(rec, start, end):
        d = get_filing_date(rec)
        return d and start <= d <= end

    filings_7d = sum(1 for r in records if filed_in_window(r, seven_days_ago, today))
    filings_prev_7d = sum(1 for r in records if filed_in_window(r, fourteen_days_ago, seven_days_ago))
    filings_30d = sum(1 for r in records if filed_in_window(r, thirty_days_ago, today))

    # Upcoming sales (auctions in next 7 days)
    upcoming_sales = sum(
        1 for r in records
        if r.get("scheduled_sale_date") and today <= r["scheduled_sale_date"] <= seven_days_ahead
    )

    # High-value properties (all time, recent filings)
    high_value = []
    total_value_7d = 0
    for r in records:
        if filed_in_window(r, seven_days_ago, today):
            amt = get_amount(r)
            total_value_7d += amt
            if amt >= 2_000_000:
                high_value.append({
                    "address": r.get("property_address", "Unknown"),
                    "amount": amt,
                    "county": r.get("county", ""),
                })

    # Status breakdown
    sold = sum(1 for r in records if r.get("status") == "sold")
    continued = sum(1 for r in records if r.get("status") == "continued")

    # County breakdown (all records)
    county_counts = {}
    for r in records:
        c = r.get("county", "unknown")
        county_counts[c] = county_counts.get(c, 0) + 1

    # Recent notable filings for editorial color (highest value this week)
    recent = sorted(
        [r for r in records if filed_in_window(r, seven_days_ago, today)],
        key=lambda x: get_amount(x),
        reverse=True
    )[:5]
    recent_filings = [{
        "address": r.get("property_address", "Unknown"),
        "amount": get_amount(r),
        "county": r.get("county", ""),
        "status": r.get("status", ""),
        "date": get_filing_date(r),
    } for r in recent]

    return {
        "filings_7d": filings_7d,
        "filings_prev_7d": filings_prev_7d,
        "filings_30d": filings_30d,
        "high_value_count": len(high_value),
        "high_value_examples": high_value[:3],
        "total_value_7d": total_value_7d,
        "total_tracked": len(records),
        "upcoming_sales_7d": upcoming_sales,
        "sold_count": sold,
        "continued_count": continued,
        "county_counts": county_counts,
        "recent_filings": recent_filings,
    }


def load_reputation_data():
    """Fetch mentions-data.json from the Reputation dashboard and compute key metrics."""
    data = fetch_json_url("https://businessden.github.io/reputation/mentions-data.json")
    if not data:
        return None

    mentions = data.get("mentions", [])
    if not mentions:
        return None

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    fourteen_days_ago = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    def mention_date(m):
        d = m.get("published") or m.get("first_seen") or ""
        return d[:10]

    today_mentions = [m for m in mentions if mention_date(m) == today]
    yesterday_mentions = [m for m in mentions if mention_date(m) == yesterday]
    week_mentions = [m for m in mentions if seven_days_ago <= mention_date(m) <= today]
    prev_week_mentions = [m for m in mentions if fourteen_days_ago <= mention_date(m) < seven_days_ago]
    month_mentions = [m for m in mentions if thirty_days_ago <= mention_date(m) <= today]

    daily_count = len(today_mentions) if today_mentions else len(yesterday_mentions)
    daily_label = "today" if today_mentions else "yesterday"

    week_sources = set(m.get("source_domain", m.get("source", "")) for m in week_mentions)
    all_sources = set(m.get("source_domain", m.get("source", "")) for m in mentions)

    source_counts = {}
    for m in week_mentions:
        s = m.get("source", "Unknown")
        source_counts[s] = source_counts.get(s, 0) + 1
    top_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    via_counts = {}
    for m in week_mentions:
        v = m.get("found_via", "unknown")
        via_counts[v] = via_counts.get(v, 0) + 1

    recent = sorted(week_mentions, key=lambda m: (m.get("published") or m.get("first_seen") or ""), reverse=True)[:8]
    formatted_recent = [{"title": m.get("title", "")[:80], "source": m.get("source", ""), "published": mention_date(m)} for m in recent]

    return {
        "mentions_today": daily_count,
        "mentions_today_label": daily_label,
        "mentions_7d": len(week_mentions),
        "mentions_prev_7d": len(prev_week_mentions),
        "mentions_30d": len(month_mentions),
        "total_mentions": len(mentions),
        "unique_sources_7d": len(week_sources),
        "unique_sources_total": len(all_sources),
        "top_sources_7d": top_sources,
        "via_breakdown_7d": via_counts,
        "recent_mentions": formatted_recent,
    }


def load_subscriber_data():
    """Fetch subscribers.json and snapshots.json from the subscriber dashboard and compute key metrics."""
    subs_data = fetch_json_url("https://businessden.github.io/subscriber/data/subscribers.json")
    snap_data = fetch_json_url("https://businessden.github.io/subscriber/data/snapshots.json")
    if not subs_data or not snap_data:
        return None

    all_subs = subs_data.get("subscribers", [])
    snapshots = snap_data.get("snapshots", [])
    if not all_subs or len(snapshots) < 2:
        return None

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    fourteen_days_ago = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    def ts_to_date(ts):
        if not ts:
            return ""
        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")

    # Build daily new/canceled counts
    daily_new = {}
    daily_canceled = {}
    for s in all_subs:
        if s.get("started_at"):
            d = ts_to_date(s["started_at"])
            daily_new[d] = daily_new.get(d, 0) + 1
        if s.get("status") == "canceled" and s.get("canceled_at"):
            d = ts_to_date(s["canceled_at"])
            daily_canceled[d] = daily_canceled.get(d, 0) + 1

    # Compute net for specific days
    def net_for_day(d):
        return daily_new.get(d, 0) - daily_canceled.get(d, 0)

    def net_for_range(start, end):
        total_new = 0
        total_canceled = 0
        for d, count in daily_new.items():
            if start <= d <= end:
                total_new += count
        for d, count in daily_canceled.items():
            if start <= d <= end:
                total_canceled += count
        return total_new, total_canceled, total_new - total_canceled

    # Today's net (fall back to yesterday)
    today_new = daily_new.get(today, 0)
    today_canceled = daily_canceled.get(today, 0)
    today_net = today_new - today_canceled
    net_day_label = "today"
    if today_new == 0 and today_canceled == 0:
        today_new = daily_new.get(yesterday, 0)
        today_canceled = daily_canceled.get(yesterday, 0)
        today_net = today_new - today_canceled
        net_day_label = "yesterday"

    # Period stats
    new_7d, canceled_7d, net_7d = net_for_range(seven_days_ago, today)
    new_prev_7d, canceled_prev_7d, net_prev_7d = net_for_range(fourteen_days_ago, seven_days_ago)

    # MTD
    month_start = today[:8] + "01"
    new_mtd, canceled_mtd, net_mtd = net_for_range(month_start, today)
    days_mtd = max(1, (datetime.now() - datetime.strptime(month_start, "%Y-%m-%d")).days)
    net_mtd_per_day = round(net_mtd / days_mtd, 1)

    # 90-day net from snapshots
    snaps_90 = snapshots[-90:] if len(snapshots) >= 90 else snapshots
    cum_net_90 = 0
    for sn in snaps_90:
        d = sn.get("date", "")
        cum_net_90 += daily_new.get(d, 0) - daily_canceled.get(d, 0)

    # YTD net
    year_start = today[:4] + "-01-01"
    new_ytd, canceled_ytd, net_ytd = net_for_range(year_start, today)
    days_ytd = max(1, (datetime.now() - datetime.strptime(year_start, "%Y-%m-%d")).days)
    net_ytd_per_day = round(net_ytd / days_ytd, 1)

    # Current active count from latest snapshot
    latest_snap = snapshots[-1] if snapshots else {}
    active_total = latest_snap.get("active", latest_snap.get("total", 0))

    # Recent daily trend (last 14 days)
    recent_trend = []
    for i in range(14, 0, -1):
        d = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        n_new = daily_new.get(d, 0)
        n_can = daily_canceled.get(d, 0)
        recent_trend.append({"date": d, "new": n_new, "canceled": n_can, "net": n_new - n_can})

    return {
        "today_new": today_new,
        "today_canceled": today_canceled,
        "today_net": today_net,
        "net_day_label": net_day_label,
        "new_7d": new_7d,
        "canceled_7d": canceled_7d,
        "net_7d": net_7d,
        "new_prev_7d": new_prev_7d,
        "canceled_prev_7d": canceled_prev_7d,
        "net_prev_7d": net_prev_7d,
        "net_mtd": net_mtd,
        "net_mtd_per_day": net_mtd_per_day,
        "new_mtd": new_mtd,
        "canceled_mtd": canceled_mtd,
        "net_90d": cum_net_90,
        "net_ytd": net_ytd,
        "net_ytd_per_day": net_ytd_per_day,
        "active_total": active_total,
        "recent_trend": recent_trend,
    }


# ============================================
# LOAD PREVIOUS BLURBS (to avoid repetition)
# ============================================

def load_previous_blurbs():
    """Load yesterday's blurbs so Claude can avoid repeating itself."""
    path = Path("daily-dreck-blurbs.json")
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


# ============================================
# CLAUDE API CALL
# ============================================

def call_claude(prompt, system_prompt):
    """Call the Anthropic Messages API and return the text response."""
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }

    payload = {
        "model": MODEL,
        "max_tokens": MAX_TOKENS,
        "system": system_prompt,
        "messages": [
            {"role": "user", "content": prompt}
        ],
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=data,
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            # Extract text from content blocks
            text_parts = [b["text"] for b in body.get("content", []) if b.get("type") == "text"]
            return "\n".join(text_parts)
    except urllib.error.HTTPError as e:
        print(f"API error {e.code}: {e.read().decode()}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"Request failed: {e}", file=sys.stderr)
        raise


# ============================================
# MAIN
# ============================================

def main():
    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    today_str = datetime.now().strftime("%A, %B %d, %Y")
    today_iso = datetime.now().strftime("%Y-%m-%d")

    # Load data
    restaurant_data = load_restaurant_data()
    foreclosure_data = load_foreclosure_data()
    reputation_data = load_reputation_data()
    subscriber_data = load_subscriber_data()
    previous_blurbs = load_previous_blurbs()

    # Build the system prompt
    system_prompt = """You are the editorial voice of The Daily Dreck, an internal data intelligence page
for BusinessDen, a Denver business journalism outlet. Your job is to write brief, sharp,
editorial blurbs about the data — like a newsroom editor summarizing the morning's numbers
for the team.

VOICE: Authoritative, concise, wry when appropriate. Think Bloomberg terminal meets
newsroom whiteboard. Never promotional — just the numbers and what they mean.

OUTPUT FORMAT: Respond with ONLY valid JSON, no markdown backticks, no preamble. The JSON must match this structure exactly:

{
  "generated_date": "YYYY-MM-DD",
  "lead_headline": "A single punchy sentence summarizing the day's most interesting data point across all tools",
  "tools": {
    "restaurant": {
      "blurb": "~50 word editorial blurb focused on Claude-verified leads. Name the restaurants and neighborhoods. Mention openings and closures separately.",
      "meta": "Updated today · 5:15 AM",
      "ticker_daily": {
        "label1": "opened", "value1": "N (opening leads new today)",
        "label2": "closed", "value2": "N (closure leads new today)",
        "period": "today"
      },
      "ticker_weekly": {
        "label1": "opened", "value1": "N (total active opening leads)",
        "label2": "closed", "value2": "N (total active closure leads)",
        "period": "all leads"
      }
    },
    "foreclosure": {
      "blurb": "~50 word editorial blurb about foreclosure data.",
      "meta": "Updated today · 5:15 AM",
      "ticker_daily": {
        "label1": "new filings", "value1": "N", "class1": "",
        "label2": "upcoming auctions", "value2": "N", "class2": "",
        "period": "today"
      },
      "ticker_weekly": {
        "label1": "filings", "value1": "N", "class1": "",
        "label2": "sold", "value2": "N", "class2": "",
        "period": "7-day"
      }
    },
    "retail": {
      "meta": "Expected Q2 2026"
    },
    "reputation": {
      "blurb": "~50 word editorial blurb about BusinessDen mentions/citations in other media this week.",
      "meta": "Updated today · 5:15 AM",
      "ticker_daily": {
        "label1": "mentions", "value1": "N", "class1": "",
        "period": "today"
      },
      "ticker_weekly": {
        "label1": "mentions", "value1": "N", "class1": "",
        "label2": "sources", "value2": "N", "class2": "",
        "period": "7-day"
      }
    },
    "subscriptions": {
      "blurb": "~50 word editorial blurb about subscriber net growth. Start with today's net change, then MTD, then 90-day net. Describe the recent 1-2 week trend vs the longer-term pattern.",
      "meta": "Updated today · 5:15 AM",
      "net_mtd": "N (integer, month-to-date net new subscribers)",
      "net_ytd": "N (integer, year-to-date net new subscribers)",
      "net_mtd_per_day": "N.N (float, MTD net per day = net_mtd / days elapsed this month)",
      "net_ytd_per_day": "N.N (float, YTD net per day = net_ytd / days elapsed this year)"
    }
  }
}

For ticker values: use "+N" for openings/positive numbers, "−N" (with minus sign −) for closures/negative,
plain "N" for neutral counts. For class1/class2: use "up" for green, "down" for red, or "" for neutral cream.

IMPORTANT:
- Start each live tool blurb with the headline numbers in bold (wrap key figures in <strong> tags)
- Then contextualize: compare to last week, note trends, flag neighborhoods or patterns
- Never repeat yesterday's phrasing — find a fresh angle
- Keep retail entries as-is — do NOT generate blurbs for them, they are static in the HTML
- For reputation: note top citing sources, week-over-week trend, and any notable pickups
- For subscriptions: START with today's net change (e.g. "Net +3 yesterday"), then state impact on MTD number, then 90-day net. Then describe the recent 1-2 week trend vs the longer-term pattern. Keep it factual and concise."""

    # Build the user prompt with data
    prompt_parts = [f"Today is {today_str}.\n"]

    if restaurant_data:
        prompt_parts.append(f"""RESTAURANT DATA:

CLAUDE-VERIFIED LEADS (the PRIMARY focus of the blurb — these are from the Leads tab):
- Total active leads: {restaurant_data['leads_total']} ({restaurant_data['leads_openings']} openings, {restaurant_data['leads_closures']} closures/temp closures)
- New leads today: {restaurant_data['leads_today']}
- Lead details: {json.dumps(restaurant_data['leads_detail'], indent=2)}

CONFIRMED CHANGES (secondary context):
- Past 7 days (confirmed): {restaurant_data['openings_verified_7d']} openings, {restaurant_data['closures_verified_7d']} closures
- Total tracked: {restaurant_data['total_tracked']}
- Last scrape: {restaurant_data['last_scrape']}

BLURB RULES:
- The blurb MUST focus on the Claude-verified leads — name the restaurants, neighborhoods, and what was detected
- Mention both openings and closures separately if both exist
- Use the lead summaries to add color (e.g. "former location of X" or "chain expanding from Boulder")
- Only fall back to confirmed stats if there are ZERO leads
- For the ticker: value1 in ticker_daily = leads_today; value1 in ticker_weekly = leads_total
""")
    else:
        prompt_parts.append("RESTAURANT DATA: Not available today. Write a generic blurb noting data is being refreshed.\n")

    if foreclosure_data:
        prompt_parts.append(f"""FORECLOSURE DATA:
- New filings this week (by publication date): {foreclosure_data['filings_7d']}
- Previous week (for comparison): {foreclosure_data['filings_prev_7d']}
- Past 30 days: {foreclosure_data['filings_30d']}
- High-value filings (>$2M) this week: {foreclosure_data['high_value_count']}
- Total value of this week's filings: ${foreclosure_data['total_value_7d']:,.0f}
- Upcoming auctions next 7 days: {foreclosure_data['upcoming_sales_7d']}
- Total active cases: {foreclosure_data['total_tracked']} ({foreclosure_data['sold_count']} sold, {foreclosure_data['continued_count']} continued)
- Counties: {json.dumps(foreclosure_data['county_counts'])}
- Highest-value recent filings: {json.dumps(foreclosure_data['recent_filings'][:5], indent=2)}
""")
    else:
        prompt_parts.append("FORECLOSURE DATA: Not available today. Write a generic blurb noting data is being refreshed.\n")

    if reputation_data:
        prompt_parts.append(f"""REPUTATION DATA (BusinessDen mentions in other media):
- Mentions {reputation_data['mentions_today_label']}: {reputation_data['mentions_today']}
- Past 7 days: {reputation_data['mentions_7d']} mentions across {reputation_data['unique_sources_7d']} unique sources
- Previous 7 days (for comparison): {reputation_data['mentions_prev_7d']} mentions
- Past 30 days: {reputation_data['mentions_30d']} mentions
- All-time total: {reputation_data['total_mentions']} mentions from {reputation_data['unique_sources_total']} sources
- Top citing sources this week: {json.dumps(reputation_data['top_sources_7d'])}
- Discovery channels: {json.dumps(reputation_data['via_breakdown_7d'])}
- Recent notable mentions: {json.dumps(reputation_data['recent_mentions'][:6], indent=2)}
""")
    else:
        prompt_parts.append("REPUTATION DATA: Not available today. Write a generic blurb noting data is being refreshed.\n")

    if subscriber_data:
        prompt_parts.append(f"""SUBSCRIBER DATA (BusinessDen paying subscribers):
- Net change {subscriber_data['net_day_label']}: {subscriber_data['today_net']:+d} ({subscriber_data['today_new']} new, {subscriber_data['today_canceled']} canceled)
- Net change past 7 days: {subscriber_data['net_7d']:+d} ({subscriber_data['new_7d']} new, {subscriber_data['canceled_7d']} canceled)
- Previous 7 days (for comparison): {subscriber_data['net_prev_7d']:+d} ({subscriber_data['new_prev_7d']} new, {subscriber_data['canceled_prev_7d']} canceled)
- Month-to-date net: {subscriber_data['net_mtd']:+d} ({subscriber_data['net_mtd_per_day']:+.1f}/day)
- 90-day cumulative net: {subscriber_data['net_90d']:+d}
- Year-to-date net: {subscriber_data['net_ytd']:+d} ({subscriber_data['net_ytd_per_day']:+.1f}/day)
- Current active subscribers: {subscriber_data['active_total']}
- Daily trend (last 14 days): {json.dumps(subscriber_data['recent_trend'])}

IMPORTANT for subscriptions blurb: Start with {subscriber_data['net_day_label']}'s net ({subscriber_data['today_net']:+d}), then state MTD net ({subscriber_data['net_mtd']:+d}), then 90-day net ({subscriber_data['net_90d']:+d}). Then describe how the last 1-2 weeks compare with the longer-term trend.
""")
    else:
        prompt_parts.append("SUBSCRIBER DATA: Not available today. Write a generic blurb noting data is being refreshed.\n")

    if previous_blurbs and previous_blurbs.get("generated_date") != today_iso:
        prompt_parts.append(f"""YESTERDAY'S BLURBS (do NOT repeat these — find fresh phrasing):
- Restaurant: {previous_blurbs.get('tools', {}).get('restaurant', {}).get('blurb', 'N/A')}
- Foreclosure: {previous_blurbs.get('tools', {}).get('foreclosure', {}).get('blurb', 'N/A')}
- Reputation: {previous_blurbs.get('tools', {}).get('reputation', {}).get('blurb', 'N/A')}
- Subscriptions: {previous_blurbs.get('tools', {}).get('subscriptions', {}).get('blurb', 'N/A')}
- Lead headline: {previous_blurbs.get('lead_headline', 'N/A')}
""")

    prompt_parts.append("Generate today's Daily Dreck blurbs as JSON.")

    prompt = "\n".join(prompt_parts)

    print(f"Generating blurbs for {today_str}...")
    print(f"Restaurant data: {'available' if restaurant_data else 'unavailable'}")
    print(f"Foreclosure data: {'available' if foreclosure_data else 'unavailable'}")
    print(f"Reputation data: {'available' if reputation_data else 'unavailable'}")
    print(f"Subscriber data: {'available' if subscriber_data else 'unavailable'}")

    response_text = call_claude(prompt, system_prompt)

    # Parse JSON response
    # Strip any accidental markdown fencing
    cleaned = response_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    try:
        blurbs = json.loads(cleaned)
    except json.JSONDecodeError as e:
        print(f"Failed to parse Claude response as JSON: {e}", file=sys.stderr)
        print(f"Raw response:\n{response_text}", file=sys.stderr)
        sys.exit(1)

    # Ensure generated_date is set
    blurbs["generated_date"] = today_iso

    # Write output
    output_path = Path("daily-dreck-blurbs.json")
    with open(output_path, "w") as f:
        json.dump(blurbs, f, indent=2)

    print(f"Blurbs written to {output_path}")
    print(f"Lead headline: {blurbs.get('lead_headline', 'N/A')}")


if __name__ == "__main__":
    main()
