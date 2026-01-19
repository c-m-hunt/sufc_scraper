from dataclasses import dataclass, field
from datetime import date
from typing import Literal, Optional


@dataclass
class Match:
    """Represents a single Southend United match."""

    # Compulsory fields
    date: date
    opposition: str
    venue: Literal["H", "A", "N"]  # Home, Away, Neutral
    goals_for: int
    goals_against: int

    # Competition info
    competition: str
    season: str  # e.g., "1920-1921"

    # Optional fields (populated in enrichment pass)
    attendance: Optional[int] = None
    referee: Optional[str] = None
    scorers: Optional[list[str]] = field(default=None)
    lineup: Optional[list[str]] = field(default=None)

    # Source tracking (for later enrichment)
    source: str = ""  # e.g., "transfermarkt", "football-data"
    source_match_id: Optional[str] = None  # Match ID/URL for re-fetching detail
    detail_fetched: bool = False  # Whether scorers/lineup have been fetched

    @property
    def result(self) -> Literal["W", "D", "L"]:
        """Return the result from Southend's perspective."""
        if self.goals_for > self.goals_against:
            return "W"
        elif self.goals_for < self.goals_against:
            return "L"
        return "D"

    @property
    def score(self) -> str:
        """Return the score as a string."""
        return f"{self.goals_for}-{self.goals_against}"

    def __str__(self) -> str:
        venue_str = {"H": "vs", "A": "@", "N": "v"}[self.venue]
        return f"{self.date}: {venue_str} {self.opposition} {self.score} ({self.competition})"
