"""Scraper for 11v11.com match data.

This scraper requires browser-based fetching due to Cloudflare protection.
It can work with pre-fetched HTML content or integrate with Playwright.
"""

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


class ElevenVElevenScraper(BaseScraper):
    """Scraper for 11v11.com historical match data."""

    SOURCE_NAME = "11v11"
    MIN_SEASON = 1906
    MAX_SEASON = 2026

    BASE_URL = "https://www.11v11.com/teams/southend-united/tab/matches/season"
    TEAM_NAME = "Southend United"

    # Competition name mapping
    COMPETITION_MAP = {
        "League Division Three": "Third Division",
        "League Division Three South": "Third Division",
        "League Division Four": "Fourth Division",
        "League Division Two": "Second Division",
        "League Division One": "First Division",
        "Football League Third Division": "Third Division",
        "Football League Second Division": "Second Division",
        "Football League First Division": "First Division",
        "Premier League": "Premier League",
        "Championship": "Championship",
        "League One": "League One",
        "League Two": "League Two",
        "National League": "National League",
        "Conference National": "National League",
        "FA Cup": "FA Cup",
        "League Cup": "League Cup",
        "EFL Cup": "League Cup",
        "Football League Cup": "League Cup",
    }

    def __init__(self, http_client: HttpClient | None = None):
        super().__init__(http_client)
        self._html_cache: dict[int, str] = {}

    def set_html_for_season(self, start_year: int, html: str) -> None:
        """
        Set pre-fetched HTML content for a season.
        Use this when fetching via Playwright or other browser automation.
        """
        self._html_cache[start_year] = html

    def _get_season_url(self, start_year: int) -> str:
        """Get the URL for a season page (uses end year in URL)."""
        end_year = start_year + 1
        return f"{self.BASE_URL}/{end_year}/"

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date from 11v11 format (e.g., '16 Aug 1975')."""
        return datetime.strptime(date_str.strip(), "%d %b %Y").date()

    def _parse_score(self, score_str: str) -> tuple[int, int]:
        """Parse score string (e.g., '2-1') into home and away goals."""
        # Handle aggregate scores like "3-0 Agg: 3-2"
        score_str = score_str.split("Agg:")[0].strip()
        match = re.match(r"(\d+)-(\d+)", score_str)
        if match:
            return int(match.group(1)), int(match.group(2))
        raise ValueError(f"Cannot parse score: {score_str}")

    def _normalize_competition(self, comp: str) -> str:
        """Normalize competition name to match existing data."""
        comp = comp.strip()
        return self.COMPETITION_MAP.get(comp, comp)

    def _parse_match_row(self, row, season: str) -> Optional[Match]:
        """Parse a table row into a Match object."""
        cells = row.find_all("td")
        if len(cells) < 5:
            return None

        try:
            # Extract data from cells
            date_str = cells[0].get_text(strip=True)
            match_str = cells[1].get_text(strip=True)
            result = cells[2].get_text(strip=True)  # W/D/L
            score_str = cells[3].get_text(strip=True)
            competition = cells[4].get_text(strip=True)

            # Parse date
            match_date = self._parse_date(date_str)

            # Parse match string to get opponent and venue
            # Format: "Southend United v Sheffield Wednesday" (home)
            # Format: "Sheffield Wednesday v Southend United" (away)
            if " v " in match_str:
                parts = match_str.split(" v ")
                home_team = parts[0].strip()
                away_team = parts[1].strip()

                if self.TEAM_NAME in home_team:
                    venue = "H"
                    opposition = away_team
                else:
                    venue = "A"
                    opposition = home_team
            else:
                logger.warning(f"Cannot parse match: {match_str}")
                return None

            # Parse score
            home_goals, away_goals = self._parse_score(score_str)

            # Convert to Southend's perspective
            if venue == "H":
                goals_for = home_goals
                goals_against = away_goals
            else:
                goals_for = away_goals
                goals_against = home_goals

            # Normalize competition name
            competition = self._normalize_competition(competition)

            # Get match URL for source tracking
            link = cells[1].find("a")
            source_match_id = link.get("href") if link else None

            return Match(
                date=match_date,
                opposition=opposition,
                venue=venue,
                goals_for=goals_for,
                goals_against=goals_against,
                competition=competition,
                season=season,
                source=self.SOURCE_NAME,
                source_match_id=source_match_id,
            )

        except Exception as e:
            logger.warning(f"Error parsing row: {e}")
            return None

    def scrape_season(self, start_year: int) -> list[Match]:
        """
        Scrape all matches for a season.

        Note: Due to Cloudflare protection, HTML must be pre-fetched
        using browser automation and set via set_html_for_season().
        """
        season = format_season(start_year)

        # Check for cached HTML
        if start_year not in self._html_cache:
            logger.warning(
                f"No HTML cached for {season}. "
                f"Use set_html_for_season() with browser-fetched content."
            )
            return []

        html = self._html_cache[start_year]
        return self.parse_html(html, season)

    def parse_html(self, html: str, season: str) -> list[Match]:
        """Parse HTML content and extract matches."""
        soup = BeautifulSoup(html, "lxml")
        matches = []

        # Find the matches table
        table = soup.find("table")
        if not table:
            logger.warning(f"No table found for {season}")
            return []

        # Find all data rows (skip header)
        tbody = table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")
        else:
            rows = table.find_all("tr")[1:]  # Skip header row

        for row in rows:
            match = self._parse_match_row(row, season)
            if match:
                matches.append(match)

        logger.info(f"Parsed {len(matches)} matches for {season}")
        return matches


def fetch_with_playwright(start_year: int) -> str:
    """
    Fetch season page using Playwright (requires playwright to be installed).

    This is a helper function for manual/script usage.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise ImportError("playwright is required. Install with: pip install playwright")

    url = f"https://www.11v11.com/teams/southend-united/tab/matches/season/{start_year + 1}/"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        html = page.content()
        browser.close()

    return html
