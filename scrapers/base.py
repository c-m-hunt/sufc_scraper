"""Base scraper class defining the interface for all scrapers."""

from abc import ABC, abstractmethod
from typing import Iterator

from models.match import Match
from utils.http_client import HttpClient


class BaseScraper(ABC):
    """Abstract base class for match data scrapers."""

    # Override in subclasses
    SOURCE_NAME: str = "unknown"
    MIN_SEASON: int = 1900
    MAX_SEASON: int = 2100

    def __init__(self, http_client: HttpClient | None = None):
        """
        Initialize the scraper.

        Args:
            http_client: HTTP client to use for requests. If None, creates a new one.
        """
        self.http_client = http_client or HttpClient()

    def can_scrape_season(self, start_year: int) -> bool:
        """Check if this scraper can handle the given season."""
        return self.MIN_SEASON <= start_year < self.MAX_SEASON

    @abstractmethod
    def scrape_season(self, start_year: int) -> list[Match]:
        """
        Scrape all matches for a given season.

        Args:
            start_year: The starting year of the season (e.g., 1920 for 1920-1921)

        Returns:
            List of Match objects for that season
        """
        pass

    def scrape_seasons(self, start_year: int, end_year: int) -> Iterator[Match]:
        """
        Scrape matches for a range of seasons.

        Args:
            start_year: First season start year (inclusive)
            end_year: Last season start year (inclusive)

        Yields:
            Match objects
        """
        for year in range(start_year, end_year + 1):
            if self.can_scrape_season(year):
                for match in self.scrape_season(year):
                    yield match
