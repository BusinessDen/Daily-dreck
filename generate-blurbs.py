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
MAX_TOKENS = 1500

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
    """Fetch restaurant-data.json from GitHub Pages and compute key metrics."""
    data = fetch_json_url("https://businessden.github.io/Restaurant-tracker/restaurant-data.json")
    if not data:
        return None

    today = datetime.now().strftime("%Y-%m-%d")
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    restaurants = data if isinstance(data, list) else data.get("restaurants", [])

    # Count openings and closures in time windows
    openings_7d = 0
    closures_7d = 0
    openings_30d = 0
    closures_30d = 0
    latest_events = []

    for r in restaurants:
        detected = r.get("first_seen") or r.get("detected_date") or ""
        status = r.get("status", "").lower()

        if detected >= seven_days_ago:
            if "open" in status or "new" in status:
                openings_7d += 1
            elif "close" in status or "permanently" in status:
                closures_7d += 1

        if detected >= thirty_days_ago:
            if "open" in status or "new" in status:
                openings_30d += 1
            elif "close" in status or "permanently" in status:
                closures_30d += 1

        # Collect most recent events for editorial color
        if detected >= seven_days_ago:
            latest_events.append({
                "name": r.get("name", "Unknown"),
                "status": status,
                "date": detected,
                "address": r.get("address", ""),
                "neighborhood": r.get("neighborhood", ""),
            })

    # Sort latest events by date descending
    latest_events.sort(key=lambda x: x["date"], reverse=True)

    return {
        "openings_7d": openings_7d,
        "closures_7d": closures_7d,
        "openings_30d": openings_30d,
        "closures_30d": closures_30d,
        "total_tracked": len(restaurants),
        "latest_events": latest_events[:10],  # Top 10 most recent
    }


def load_foreclosure_data():
    """Fetch foreclosure-data.json from GitHub Pages and compute key metrics."""
    data = fetch_json_url("https://businessden.github.io/Colorado-foreclosure/foreclosure-data.json")
    if not data:
        return None

    today = datetime.now().strftime("%Y-%m-%d")
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    filings = data if isinstance(data, list) else data.get("filings", data.get("foreclosures", []))

    new_this_week = 0
    new_this_month = 0
    high_value_count = 0
    total_value = 0
    recent_filings = []

    for f_item in filings:
        filed = f_item.get("filed_date") or f_item.get("sale_date") or f_item.get("first_seen") or ""
        amount = f_item.get("amount") or f_item.get("original_amount") or 0
        if isinstance(amount, str):
            amount = float(amount.replace(",", "").replace("$", "")) if amount.strip() else 0

        if filed >= seven_days_ago:
            new_this_week += 1
            if amount >= 2000000:
                high_value_count += 1
            total_value += amount
            recent_filings.append({
                "address": f_item.get("address", "Unknown"),
                "amount": amount,
                "date": filed,
            })

        if filed >= thirty_days_ago:
            new_this_month += 1

    recent_filings.sort(key=lambda x: x["date"], reverse=True)

    return {
        "filings_7d": new_this_week,
        "filings_30d": new_this_month,
        "high_value_count": high_value_count,
        "total_value_7d": total_value,
        "total_tracked": len(filings),
        "recent_filings": recent_filings[:8],
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
      "blurb": "~50 word editorial blurb. Start with the key numbers, then provide context — week-over-week trend, notable neighborhoods, what it means.",
      "meta": "Updated today · 5:00 AM",
      "ticker": {
        "label1": "opened", "value1": "+N", "class1": "up",
        "label2": "closed", "value2": "−N", "class2": "down",
        "period": "7-day"
      }
    },
    "foreclosure": {
      "blurb": "~50 word editorial blurb about foreclosure data.",
      "meta": "Updated today · 5:00 AM",
      "ticker": {
        "label1": "new filings", "value1": "N", "class1": "",
        "label2": "", "value2": "", "class2": "",
        "period": "this week"
      }
    },
    "retail": {
      "blurb": "Brief status note (~20 words). This tool is in development.",
      "meta": "Expected Q2 2026",
      "ticker": {"label1": "", "value1": "", "class1": "", "label2": "", "value2": "", "class2": "", "period": ""}
    },
    "revenue": {
      "blurb": "Brief status note (~20 words). This tool is planned.",
      "meta": "Expected Q3 2026",
      "ticker": {"label1": "", "value1": "", "class1": "", "label2": "", "value2": "", "class2": "", "period": ""}
    },
    "reputation": {
      "blurb": "Brief status note (~20 words). This tool is planned.",
      "meta": "Expected Q4 2026",
      "ticker": {"label1": "", "value1": "", "class1": "", "label2": "", "value2": "", "class2": "", "period": ""}
    }
  }
}

For ticker values: use "+N" for openings/positive numbers, "−N" (with minus sign −) for closures/negative,
plain "N" for neutral counts. For class1/class2: use "up" for green, "down" for red, or "" for neutral cream.

IMPORTANT:
- Start each live tool blurb with the headline numbers in bold (wrap key figures in <strong> tags)
- Then contextualize: compare to last week, note trends, flag neighborhoods or patterns
- Never repeat yesterday's phrasing — find a fresh angle
- Keep retail/revenue/reputation blurbs very short since they have no data yet"""

    # Build the user prompt with data
    prompt_parts = [f"Today is {today_str}.\n"]

    if restaurant_data:
        prompt_parts.append(f"""RESTAURANT DATA:
- 7-day openings: {restaurant_data['openings_7d']}
- 7-day closures: {restaurant_data['closures_7d']}
- 30-day openings: {restaurant_data['openings_30d']}
- 30-day closures: {restaurant_data['closures_30d']}
- Total tracked: {restaurant_data['total_tracked']}
- Recent events: {json.dumps(restaurant_data['latest_events'][:6], indent=2)}
""")
    else:
        prompt_parts.append("RESTAURANT DATA: Not available today. Write a generic blurb noting data is being refreshed.\n")

    if foreclosure_data:
        prompt_parts.append(f"""FORECLOSURE DATA:
- Filings this week: {foreclosure_data['filings_7d']}
- Filings this month: {foreclosure_data['filings_30d']}
- High-value filings (>$2M) this week: {foreclosure_data['high_value_count']}
- Total value of this week's filings: ${foreclosure_data['total_value_7d']:,.0f}
- Total tracked: {foreclosure_data['total_tracked']}
- Recent filings: {json.dumps(foreclosure_data['recent_filings'][:5], indent=2)}
""")
    else:
        prompt_parts.append("FORECLOSURE DATA: Not available today. Write a generic blurb noting data is being refreshed.\n")

    if previous_blurbs and previous_blurbs.get("generated_date") != today_iso:
        prompt_parts.append(f"""YESTERDAY'S BLURBS (do NOT repeat these — find fresh phrasing):
- Restaurant: {previous_blurbs.get('tools', {}).get('restaurant', {}).get('blurb', 'N/A')}
- Foreclosure: {previous_blurbs.get('tools', {}).get('foreclosure', {}).get('blurb', 'N/A')}
- Lead headline: {previous_blurbs.get('lead_headline', 'N/A')}
""")

    prompt_parts.append("Generate today's Daily Dreck blurbs as JSON.")

    prompt = "\n".join(prompt_parts)

    print(f"Generating blurbs for {today_str}...")
    print(f"Restaurant data: {'available' if restaurant_data else 'unavailable'}")
    print(f"Foreclosure data: {'available' if foreclosure_data else 'unavailable'}")

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
