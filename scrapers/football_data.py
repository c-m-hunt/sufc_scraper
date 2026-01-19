"""Scraper for football-data.co.uk CSV files."""

import csv
import io
import logging
from datetime import datetime
from typing import Optional

from models.match import Match
from scrapers.base import BaseScraper
from utils.http_client import HttpClient
from utils.season_utils import format_season

logger = logging.getLogger(__name__)


# Southend United's division history (approximate)
# Format: {start_year: division_code}
# E0=Premier, E1=Championship, E2=League One, E3=League Two, EC=Conference
SOUTHEND_DIVISIONS = {
    1993: "E2",  # Division 2 (now League One)
    1994: "E2",
    1995: "E2",
    1996: "E2",
    1997: "E3",  # Relegated to Division 3
    1998: "E3",
    1999: "E3",
    2000: "E3",
    2001: "E3",
    2002: "E3",
    2003: "E3",
    2004: "E3",
    2005: "E2",  # Promoted to League One
    2006: "E1",  # Promoted to Championship
    2007: "E2",  # Relegated
    2008: "E2",
    2009: "E2",
    2010: "E3",  # Relegated to League Two
    2011: "E3",
    2012: "E3",
    2013: "E3",
    2014: "E3",
    2015: "E2",  # Promoted to League One
    2016: "E2",
    2017: "E2",
    2018: "E2",
    2019: "E2",
    2020: "E3",  # Relegated to League Two
    2021: "EC",  # Relegated to National League
    2022: "EC",
    2023: "EC",
    2024: "EC",
    2025: "EC",
}


class FootballDataScraper(BaseScraper):
    """Scraper for football-data.co.uk CSV data."""

    SOURCE_NAME = "football-data"
    MIN_SEASON = 1993
    MAX_SEASON = 2026

    BASE_URL = "https://www.football-data.co.uk/mmz4281"
    TEAM_NAME = "Southend"  # How Southend appears in the data

    def __init__(self, http_client: HttpClient | None = None):
        super().__init__(http_client)

    def _get_csv_url(self, start_year: int, division: str) -> str:
        """Get the URL for a season's CSV file."""
        # Format: mmz4281/9394/E2.csv for 1993-1994 League One
        season_code = f"{start_year % 100:02d}{(start_year + 1) % 100:02d}"
        return f"{self.BASE_URL}/{season_code}/{division}.csv"

    def _get_division_for_season(self, start_year: int) -> Optional[str]:
        """Get the division code for a season."""
        return SOUTHEND_DIVISIONS.get(start_year)

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date from CSV (various formats)."""
        # Try different date formats
        formats = [
            "%d/%m/%Y",  # 28/08/1993
            "%d/%m/%y",  # 28/08/93
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Cannot parse date: {date_str}")

    def _parse_match_row(
        self, row: dict, season: str
    ) -> Optional[Match]:
        """Parse a CSV row into a Match object if it involves Southend."""
        home_team = row.get("HomeTeam", "")
        away_team = row.get("AwayTeam", "")

        # Check if Southend is involved
        is_home = self.TEAM_NAME in home_team
        is_away = self.TEAM_NAME in away_team

        if not is_home and not is_away:
            return None

        # Parse basic data
        try:
            match_date = self._parse_date(row.get("Date", ""))
            fthg = int(row.get("FTHG", row.get("HG", 0)))
            ftag = int(row.get("FTAG", row.get("AG", 0)))
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing row: {e}")
            return None

        # Determine venue and opponent from Southend's perspective
        if is_home:
            venue = "H"
            opposition = away_team
            goals_for = fthg
            goals_against = ftag
        else:
            venue = "A"
            opposition = home_team
            goals_for = ftag
            goals_against = fthg

        # Get competition from division
        div = row.get("Div", "")
        competition_map = {
            "E0": "Premier League",
            "E1": "Championship",
            "E2": "League One",
            "E3": "League Two",
            "EC": "National League",
        }
        competition = competition_map.get(div, div)

        # Optional fields
        attendance = None
        if "Attendance" in row and row["Attendance"]:
            try:
                attendance = int(row["Attendance"])
            except (ValueError, TypeError):
                pass

        referee = row.get("Referee") or None

        return Match(
            date=match_date,
            opposition=opposition,
            venue=venue,
            goals_for=goals_for,
            goals_against=goals_against,
            competition=competition,
            season=season,
            attendance=attendance,
            referee=referee,
            source=self.SOURCE_NAME,
            source_match_id=f"{match_date.isoformat()}_{opposition}",
        )

    def scrape_season(self, start_year: int) -> list[Match]:
        """Scrape all Southend matches for a season."""
        division = self._get_division_for_season(start_year)
        if not division:
            logger.warning(f"No division mapping for {start_year}")
            return []

        season = format_season(start_year)
        url = self._get_csv_url(start_year, division)

        logger.info(f"Fetching {season} from {url}")

        try:
            response_text = self.http_client.get_text(url)
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return []

        matches = []
        reader = csv.DictReader(io.StringIO(response_text))

        for row in reader:
            match = self._parse_match_row(row, season)
            if match:
                matches.append(match)

        logger.info(f"Found {len(matches)} Southend matches in {season}")
        return matches

    def scrape_all_divisions_for_season(self, start_year: int) -> list[Match]:
        """
        Scrape all divisions for a season (for cup games that might be in other divisions).

        Note: This is slower as it checks all divisions. Use scrape_season() for
        just league matches.
        """
        all_matches = []
        season = format_season(start_year)

        for division in ["E0", "E1", "E2", "E3", "EC"]:
            url = self._get_csv_url(start_year, division)

            try:
                response_text = self.http_client.get_text(url)
            except Exception:
                continue

            reader = csv.DictReader(io.StringIO(response_text))
            for row in reader:
                match = self._parse_match_row(row, season)
                if match:
                    all_matches.append(match)

        return all_matches
