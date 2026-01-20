# Southend United FC Match History

Historical match data scraper and database for Southend United Football Club, covering matches from 1909 to present.

## Overview

This project scrapes match data from multiple sources and consolidates them into a SQLite database. The data includes:

- Match date, opposition, venue (H/A/N)
- Score and result
- Competition
- Season
- Optional: attendance, referee, scorers, lineup

**Current database:** 4,933 matches

## Project Structure

```
sufc_history/
├── main.py                 # CLI entry point
├── data/
│   ├── matches.db          # SQLite database
│   ├── all_matches.csv     # Exported CSV
│   └── all_matches.json    # Exported JSON
├── models/
│   └── match.py            # Match dataclass
├── scrapers/
│   ├── base.py             # Abstract base scraper
│   ├── statto.py           # statto.com (1909-2017)
│   ├── transfermarkt.py    # transfermarkt.co.uk (1910-present)
│   ├── football_data.py    # football-data.co.uk (1993-present, league only)
│   └── eleven_v_eleven.py  # 11v11.com (1906-present, requires Playwright)
├── storage/
│   └── database.py         # SQLite storage layer
└── utils/
    ├── http_client.py      # Rate-limited HTTP client
    └── season_utils.py     # Season formatting utilities
```

## Installation

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -e .

# For 11v11 scraping (optional, requires browser automation)
pip install playwright
playwright install chromium
```

## Usage

### Scraping Data

```bash
# Scrape a single season
python main.py scrape --season 2023

# Scrape a range of seasons
python main.py scrape --start 2020 --end 2025

# Scrape using a specific source
python main.py scrape --season 2023 --source transfermarkt

# Scrape all available data
python main.py scrape --all
```

**Available sources:** `statto`, `transfermarkt`, `football-data`

### Exporting Data

```bash
# Export to CSV
python main.py export --format csv -o data/all_matches.csv

# Export to JSON
python main.py export --format json -o data/all_matches.json
```

### Viewing Statistics

```bash
python main.py stats
```

Output:
```
Total matches: 4933

Season Summary:
----------------------------------------------------------------------
Season        Matches    W    D    L    GF    GA
----------------------------------------------------------------------
2015-2016          51   18   11   22    63    69
2016-2017          52   22   13   17    75    62
...
```

## Data Sources

| Source | Coverage | Notes |
|--------|----------|-------|
| [statto.com](https://statto.com) | 1909-2017 | Primary historical source |
| [transfermarkt.co.uk](https://transfermarkt.co.uk) | 1910-present | Modern era, includes attendance/referee |
| [football-data.co.uk](https://football-data.co.uk) | 1993-present | League matches only |
| [11v11.com](https://11v11.com) | 1906-present | Requires Playwright for Cloudflare bypass |

## Using 11v11 Scraper

The 11v11.com website uses Cloudflare protection, which blocks standard HTTP requests. This requires browser automation to bypass.

### Option 1: Using the Python Scraper

```python
from scrapers.eleven_v_eleven import ElevenVElevenScraper, fetch_with_playwright
from storage.database import Database

# Fetch HTML using Playwright
html = fetch_with_playwright(2023)  # Start year of season

# Parse and store
scraper = ElevenVElevenScraper()
scraper.set_html_for_season(2023, html)
matches = scraper.scrape_season(2023)

db = Database("data/matches.db")
for match in matches:
    db.upsert_match(match)
```

### Option 2: Using Claude Code with Playwright MCP

When you have Claude Code with the Playwright MCP server configured, you can fetch data from Cloudflare-protected sites that would otherwise return 403 errors.

**URL format:** `https://www.11v11.com/teams/southend-united/tab/matches/season/YEAR/`

Note: The URL uses the **end year** of the season (e.g., `/season/2016/` for the 2015-16 season).

**Steps:**
1. Navigate to the season page using `browser_navigate`
2. Extract table data using `browser_evaluate` with JavaScript:

```javascript
() => {
  const table = document.querySelector('table');
  const rows = Array.from(table.querySelectorAll('tbody tr'));
  return rows.map(row => {
    const cells = row.querySelectorAll('td');
    return {
      date: cells[0]?.textContent?.trim(),
      match: cells[1]?.textContent?.trim(),
      result: cells[2]?.textContent?.trim(),
      score: cells[3]?.textContent?.trim(),
      competition: cells[4]?.textContent?.trim()
    };
  });
}
```

3. Parse the extracted data and insert into the database using the Match model

This approach is useful for filling gaps in historical data that other sources don't cover.

## Data Model

Each match contains:

| Field | Type | Description |
|-------|------|-------------|
| date | date | Match date |
| opposition | str | Opponent team name |
| venue | H/A/N | Home, Away, or Neutral |
| goals_for | int | Southend goals scored |
| goals_against | int | Opposition goals scored |
| competition | str | Competition name |
| season | str | Season (e.g., "2023-2024") |
| attendance | int? | Match attendance |
| referee | str? | Referee name |
| scorers | list? | Goal scorers |
| lineup | list? | Starting lineup |
| source | str | Data source name |

## Database Schema

The SQLite database uses an upsert strategy with a unique constraint on `(date, opposition, competition)` to handle data from multiple sources without duplicates.

## Rate Limiting

All scrapers include rate limiting to be respectful to source websites:
- Default: 2.0 seconds between requests
- Transfermarkt: 3.0 seconds (stricter)
- Random user agent rotation
- Exponential backoff on failures

## License

MIT
