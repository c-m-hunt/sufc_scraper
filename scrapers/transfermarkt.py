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
        """Parse date from Transfermarkt format (e.g., 'Sat 08/08/2015')."""
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

    def _normalize_competition(self, comp: str) -> str:
        """Normalize competition name."""
        comp_lower = comp.strip().lower()
        for key, value in self.COMPETITION_MAP.items():
            if key in comp_lower:
                return value
        return comp.strip()

    def _clean_opponent_name(self, name: str) -> str:
        """Remove ranking info from opponent name (e.g., 'Fleetwood(19.)' -> 'Fleetwood')."""
        return re.sub(r"\s*\(\d+\.\)\s*$", "", name).strip()

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

        # Find all tables on the page
        tables = soup.find_all("table")

        current_competition = "Unknown"

        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Check if this is a fixtures table (has Date, Venue, Result columns)
            header_row = rows[0]
            header_cells = header_row.find_all(["td", "th"])
            header_texts = [c.get_text(strip=True).lower() for c in header_cells]

            # Check for fixture table headers
            if "date" not in header_texts and "venue" not in header_texts:
                # This might be a summary table - check for competition names
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) == 1:
                        comp_text = cells[0].get_text(strip=True)
                        if comp_text and not comp_text.isdigit():
                            current_competition = self._normalize_competition(comp_text)
                continue

            # This is a fixtures table - find column indices
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
                elif "matchday" in text.lower() or "round" in text.lower():
                    col_indices["round"] = i

            # Parse match rows
            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue

                match = self._parse_match_row(cells, col_indices, current_competition, season)
                if match:
                    matches.append(match)

        logger.info(f"Found {len(matches)} matches for {season}")
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

        # Get opponent - look for cell with team link or text after venue
        opponent = None
        opponent_idx = col_indices.get("opponent")

        if opponent_idx is not None and opponent_idx < len(cells):
            # Use opponent column
            opponent_cell = cells[opponent_idx]
            opponent_link = opponent_cell.find("a")
            if opponent_link:
                opponent = opponent_link.get_text(strip=True)
            else:
                opponent = cell_texts[opponent_idx]
        else:
            # Find opponent by looking for team link in cells
            for cell in cells:
                link = cell.find("a", href=re.compile(r"/verein/|/teams/"))
                if link:
                    opponent = link.get_text(strip=True)
                    break

        if not opponent:
            # Fall back to cell after ranking column
            for i, text in enumerate(cell_texts):
                if re.match(r"\(\d+\.\)", text) or text == "":
                    # Next non-empty cell might be opponent
                    for j in range(i + 1, len(cell_texts)):
                        if cell_texts[j] and not re.match(r"^\d+[:-]\d+$", cell_texts[j]):
                            opponent = cell_texts[j]
                            break
                    break

        if not opponent:
            return None

        opponent = self._clean_opponent_name(opponent)

        # Get result
        result_idx = col_indices.get("result", -1)
        result_str = cell_texts[result_idx] if result_idx < len(cell_texts) else None

        # If no result column, search for score pattern
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
