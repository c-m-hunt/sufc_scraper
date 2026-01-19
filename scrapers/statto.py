"""Scraper for statto.com historical match data."""

import logging
import re
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup

from models.match import Match
from scrapers.base import BaseScraper
from utils.http_client import HttpClient
from utils.season_utils import format_season

logger = logging.getLogger(__name__)


class StattoScraper(BaseScraper):
    """Scraper for statto.com historical match data (1909-2017)."""

    SOURCE_NAME = "statto"
    MIN_SEASON = 1909
    MAX_SEASON = 2017  # Site appears to have stopped updating

    BASE_URL = "https://www.statto.com"
    TEAM_SLUG = "southend-united"

    # Competition name mappings
    COMPETITION_MAP = {
        "english division one": "First Division",
        "english division two": "Second Division",
        "english division three": "Third Division",
        "english division three (south)": "Third Division South",
        "english division three south": "Third Division South",
        "english division three (north)": "Third Division North",
        "english division three north": "Third Division North",
        "english division four": "Fourth Division",
        "english premier league": "Premier League",
        "english championship": "Championship",
        "english league one": "League One",
        "english league two": "League Two",
        "english conference": "National League",
        "conference national": "National League",
        "fa cup": "FA Cup",
        "league cup": "League Cup",
        "efl cup": "League Cup",
        "football league cup": "League Cup",
        "football league trophy": "EFL Trophy",
        "associate members cup": "EFL Trophy",
        "auto windscreens shield": "EFL Trophy",
        "johnstone's paint trophy": "EFL Trophy",
    }

    def __init__(self, http_client: HttpClient | None = None):
        super().__init__(http_client)

    def _get_season_url(self, start_year: int) -> str:
        """Get the URL for a season's results."""
        season = f"{start_year}-{start_year + 1}"
        return f"{self.BASE_URL}/football/teams/{self.TEAM_SLUG}/{season}/results"

    def _parse_date(self, date_str: str, year_hint: int) -> Optional[datetime]:
        """Parse date from statto.com format (DD.MM.YYYY)."""
        date_str = date_str.strip()

        formats = [
            "%d.%m.%Y",
            "%d/%m/%Y",
            "%d.%m.%y",
            "%d/%m/%y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        logger.warning(f"Cannot parse date: {date_str}")
        return None

    def _parse_result(self, result_str: str) -> tuple[int, int]:
        """Parse result string like 'W2-0' or 'L0-1' or 'D1-1'."""
        result_str = result_str.strip().upper()

        # Match pattern like W2-0, L0-1, D1-1
        match = re.match(r"[WDL]?(\d+)-(\d+)", result_str)
        if match:
            return int(match.group(1)), int(match.group(2))

        raise ValueError(f"Cannot parse result: {result_str}")

    def _normalize_competition(self, comp: str) -> str:
        """Normalize competition name."""
        comp_lower = comp.strip().lower()
        for key, value in self.COMPETITION_MAP.items():
            if key in comp_lower:
                return value
        return comp.strip()

    def scrape_season(self, start_year: int) -> list[Match]:
        """Scrape all matches for a season from statto.com."""
        season = format_season(start_year)
        url = self._get_season_url(start_year)

        logger.info(f"Fetching {season} from statto.com")

        try:
            html = self.http_client.get_text(url)
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return []

        soup = BeautifulSoup(html, "lxml")
        matches = []

        # Find all tables on the page
        tables = soup.find_all("table")

        for table in tables:
            table_matches = self._parse_results_table(table, season, start_year)
            matches.extend(table_matches)

        logger.info(f"Found {len(matches)} matches for {season}")
        return matches

    def _parse_results_table(
        self, table, season: str, year_hint: int
    ) -> list[Match]:
        """Parse a results table."""
        matches = []
        current_competition = "Unknown"

        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            # Check if this is a competition header row (single cell spanning columns)
            if len(cells) == 1:
                comp_text = cells[0].get_text(strip=True)
                if comp_text and not comp_text.isdigit():
                    current_competition = self._normalize_competition(comp_text)
                continue

            # Check if this is a table header row
            cell_texts = [c.get_text(strip=True).lower() for c in cells]
            if "date" in cell_texts or "opponent" in cell_texts:
                continue

            # Try to parse as match row
            match = self._parse_match_row(cells, current_competition, season, year_hint)
            if match:
                matches.append(match)

        return matches

    def _parse_match_row(
        self, cells: list, competition: str, season: str, year_hint: int
    ) -> Optional[Match]:
        """
        Parse a table row into a Match object.

        Expected columns: No, Date, Opponent, Venue, Result, Pos, Pt
        """
        if len(cells) < 5:
            return None

        cell_texts = [cell.get_text(strip=True) for cell in cells]

        # Find indices by content pattern
        date_idx = None
        opponent_idx = None
        venue_idx = None
        result_idx = None

        for i, text in enumerate(cell_texts):
            # Date: DD.MM.YYYY pattern
            if re.match(r"\d{2}\.\d{2}\.\d{4}", text):
                date_idx = i
            # Venue: home/away
            elif text.lower() in ("home", "away", "h", "a"):
                venue_idx = i
            # Result: W2-0, L0-1, D1-1 pattern
            elif re.match(r"[WDL]\d+-\d+", text, re.IGNORECASE):
                result_idx = i
            # Opponent: has a team link
            elif cells[i].find("a", href=re.compile(r"/teams/")):
                opponent_idx = i

        # Validate we found all required fields
        if None in (date_idx, opponent_idx, venue_idx, result_idx):
            return None

        # Extract values
        date_str = cell_texts[date_idx]
        venue_str = cell_texts[venue_idx].lower()
        result_str = cell_texts[result_idx]

        # Get opponent name from link
        opponent_link = cells[opponent_idx].find("a")
        opponent = opponent_link.get_text(strip=True) if opponent_link else cell_texts[opponent_idx]

        # Parse date
        match_date = self._parse_date(date_str, year_hint)
        if not match_date:
            return None

        # Parse venue
        venue = "H" if venue_str in ("home", "h") else "A"

        # Parse result
        try:
            goals_for, goals_against = self._parse_result(result_str)
        except ValueError:
            return None

        return Match(
            date=match_date,
            opposition=opponent,
            venue=venue,
            goals_for=goals_for,
            goals_against=goals_against,
            competition=competition,
            season=season,
            source=self.SOURCE_NAME,
            source_match_id=f"{match_date.isoformat()}_{opponent}",
        )
