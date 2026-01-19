"""Scraper for Transfermarkt match data."""

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


class TransfermarktScraper(BaseScraper):
    """Scraper for Transfermarkt historical match data."""

    SOURCE_NAME = "transfermarkt"
    MIN_SEASON = 1910
    MAX_SEASON = 2026

    BASE_URL = "https://www.transfermarkt.co.uk"
    TEAM_ID = 2793  # Southend United
    TEAM_SLUG = "southend-united"

    # Competition name mappings
    COMPETITION_MAP = {
        "premier league": "Premier League",
        "championship": "Championship",
        "league one": "League One",
        "league two": "League Two",
        "third division": "Third Division",
        "third division south": "Third Division South",
        "fourth division": "Fourth Division",
        "second division": "Second Division",
        "first division": "First Division",
        "national league": "National League",
        "conference": "National League",
        "fa cup": "FA Cup",
        "efl cup": "League Cup",
        "league cup": "League Cup",
        "efl trophy": "EFL Trophy",
        "football league trophy": "EFL Trophy",
    }

    def __init__(self, http_client: HttpClient | None = None):
        # Use longer rate limit for Transfermarkt (they're stricter)
        if http_client is None:
            http_client = HttpClient(rate_limit=3.0)
        super().__init__(http_client)

    def _get_season_url(self, start_year: int) -> str:
        """Get the URL for a season's fixtures."""
        return f"{self.BASE_URL}/{self.TEAM_SLUG}/spielplan/verein/{self.TEAM_ID}/saison_id/{start_year}"

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date from Transfermarkt format (e.g., 'Sat 05/08/2017')."""
        date_str = date_str.strip()

        # Remove day name prefix
        date_str = re.sub(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+", "", date_str)

        formats = [
            "%d/%m/%Y",
            "%d/%m/%y",
            "%d.%m.%Y",
            "%d.%m.%y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        logger.warning(f"Cannot parse date: {date_str}")
        return None

    def _parse_score(self, score_str: str) -> tuple[int, int]:
        """Parse score from format like '2:1' or '2-1'."""
        score_str = score_str.strip()
        match = re.search(r"(\d+)\s*[:|-]\s*(\d+)", score_str)
        if match:
            return int(match.group(1)), int(match.group(2))
        raise ValueError(f"Cannot parse score: {score_str}")

    def _parse_attendance(self, att_str: str) -> Optional[int]:
        """Parse attendance figure."""
        if not att_str:
            return None
        # Remove thousands separators (comma or dot)
        att_str = att_str.replace(",", "").replace(".", "").strip()
        try:
            return int(att_str)
        except ValueError:
            return None

    def _normalize_competition(self, comp: str) -> Optional[str]:
        """Normalize competition name. Returns None for non-competition text."""
        comp_lower = comp.strip().lower()

        # Skip summary/navigation text that isn't a competition
        skip_terms = [
            "overall balance", "home record", "away record",
            "table section", "matches", "ranking", "club",
            "filter by"
        ]
        if any(term in comp_lower for term in skip_terms):
            return None

        for key, value in self.COMPETITION_MAP.items():
            if key in comp_lower:
                return value

        # Return original if it looks like a competition name
        if comp.strip():
            return comp.strip()
        return None

    def _clean_opponent_name(self, name: str) -> str:
        """Remove ranking info from opponent name (e.g., 'Fleetwood(19.)' -> 'Fleetwood')."""
        return re.sub(r"\s*\(\d+\.\)\s*$", "", name).strip()

    def _is_fixtures_table(self, table) -> bool:
        """Check if a table contains fixture data."""
        rows = table.find_all("tr")
        if len(rows) < 2:
            return False

        header_row = rows[0]
        header_cells = header_row.find_all(["td", "th"])
        header_texts = [c.get_text(strip=True).lower() for c in header_cells]

        # A fixtures table has Date and Venue columns
        return "date" in header_texts or "venue" in header_texts

    def _extract_competitions_from_summary(self, soup) -> list[str]:
        """Extract competition names from the summary table."""
        competitions = []

        # Find the summary table (usually first table with competition stats)
        for table in soup.find_all("table"):
            rows = table.find_all("tr")

            # Look for single-cell rows with competition names
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) == 1:
                    text = cells[0].get_text(strip=True)
                    normalized = self._normalize_competition(text)
                    if normalized:
                        competitions.append(normalized)

        return competitions

    def scrape_season(self, start_year: int) -> list[Match]:
        """Scrape all matches for a season from Transfermarkt."""
        season = format_season(start_year)
        url = self._get_season_url(start_year)

        logger.info(f"Fetching {season} from Transfermarkt")

        try:
            html = self.http_client.get_text(url)
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return []

        soup = BeautifulSoup(html, "lxml")
        matches = []

        # First, extract the list of competitions from the summary table
        competitions = self._extract_competitions_from_summary(soup)
        logger.debug(f"Found competitions: {competitions}")

        # Find all fixtures tables
        fixtures_tables = []
        for table in soup.find_all("table"):
            if self._is_fixtures_table(table):
                fixtures_tables.append(table)

        # Match each fixtures table to a competition
        # The order typically matches: first fixtures table = first competition, etc.
        for i, table in enumerate(fixtures_tables):
            # Determine competition for this table
            if i < len(competitions):
                competition = competitions[i]
            else:
                competition = "Unknown"

            # Parse this table's matches
            table_matches = self._parse_fixtures_table(table, competition, season)
            matches.extend(table_matches)

        logger.info(f"Found {len(matches)} matches for {season}")
        return matches

    def _parse_fixtures_table(self, table, competition: str, season: str) -> list[Match]:
        """Parse a fixtures table into Match objects."""
        matches = []
        rows = table.find_all("tr")

        if len(rows) < 2:
            return matches

        # Find column indices from header
        header_row = rows[0]
        header_cells = header_row.find_all(["td", "th"])
        header_texts = [c.get_text(strip=True).lower() for c in header_cells]

        col_indices = {}
        for i, text in enumerate(header_texts):
            if "date" in text:
                col_indices["date"] = i
            elif "venue" in text:
                col_indices["venue"] = i
            elif "opponent" in text:
                col_indices["opponent"] = i
            elif "result" in text:
                col_indices["result"] = i
            elif "attendance" in text:
                col_indices["attendance"] = i

        # Parse each row
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            match = self._parse_match_row(cells, col_indices, competition, season)
            if match:
                matches.append(match)

        return matches

    def _parse_match_row(
        self, cells: list, col_indices: dict, competition: str, season: str
    ) -> Optional[Match]:
        """Parse a table row into a Match object."""
        cell_texts = [c.get_text(strip=True) for c in cells]

        # Get date
        date_idx = col_indices.get("date", 1)
        if date_idx >= len(cell_texts):
            return None
        date_str = cell_texts[date_idx]
        match_date = self._parse_date(date_str)
        if not match_date:
            return None

        # Get venue (H/A)
        venue_idx = col_indices.get("venue", 3)
        if venue_idx >= len(cell_texts):
            return None
        venue_str = cell_texts[venue_idx].strip().upper()
        if venue_str not in ("H", "A"):
            return None
        venue = venue_str

        # Get opponent - look for cell with team link
        opponent = None
        for cell in cells:
            link = cell.find("a", href=re.compile(r"/verein/|/teams/"))
            if link:
                opponent = link.get_text(strip=True)
                break

        if not opponent:
            # Fall back to looking for text after ranking pattern
            for i, text in enumerate(cell_texts):
                if re.match(r"\(\d+\.\)", text) or text == "":
                    for j in range(i + 1, len(cell_texts)):
                        candidate = cell_texts[j]
                        if candidate and not re.match(r"^\d+[:-]\d+$", candidate):
                            opponent = candidate
                            break
                    break

        if not opponent:
            return None

        opponent = self._clean_opponent_name(opponent)

        # Get result - search for score pattern
        result_str = None
        result_idx = col_indices.get("result", -1)
        if result_idx >= 0 and result_idx < len(cell_texts):
            result_str = cell_texts[result_idx]

        if not result_str or not re.search(r"\d+[:-]\d+", result_str):
            for text in cell_texts:
                if re.match(r"^\d+[:-]\d+$", text):
                    result_str = text
                    break

        if not result_str:
            return None

        try:
            score1, score2 = self._parse_score(result_str)
        except ValueError:
            return None

        # Determine goals from Southend's perspective
        if venue == "H":
            goals_for, goals_against = score1, score2
        else:
            goals_for, goals_against = score2, score1

        # Get attendance
        attendance = None
        att_idx = col_indices.get("attendance")
        if att_idx is not None and att_idx < len(cell_texts):
            attendance = self._parse_attendance(cell_texts[att_idx])

        return Match(
            date=match_date,
            opposition=opponent,
            venue=venue,
            goals_for=goals_for,
            goals_against=goals_against,
            competition=competition,
            season=season,
            attendance=attendance,
            source=self.SOURCE_NAME,
            source_match_id=f"{match_date.isoformat()}_{opponent}",
        )
