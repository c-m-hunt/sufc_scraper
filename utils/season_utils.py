"""Utilities for working with football seasons."""

from datetime import date
from typing import Iterator


def parse_season(season: str) -> tuple[int, int]:
    """
    Parse a season string into start and end years.

    Examples:
        "1920-1921" -> (1920, 1921)
        "2023-24" -> (2023, 2024)
        "1920-21" -> (1920, 1921)
    """
    parts = season.split("-")
    start_year = int(parts[0])
    end_year_str = parts[1]

    if len(end_year_str) == 2:
        # Handle short format like "20-21"
        century = start_year // 100 * 100
        end_year = century + int(end_year_str)
        # Handle century boundary (e.g., 1999-00)
        if end_year < start_year:
            end_year += 100
    else:
        end_year = int(end_year_str)

    return start_year, end_year


def format_season(start_year: int, end_year: int | None = None) -> str:
    """
    Format start/end years into a season string.

    Examples:
        (1920, 1921) -> "1920-1921"
        (1920, None) -> "1920-1921"
        (1920,) -> "1920-1921"
    """
    if end_year is None:
        end_year = start_year + 1
    return f"{start_year}-{end_year}"


def get_season_from_date(match_date: date) -> str:
    """
    Get the season string for a given date.

    Football seasons typically run from August to May.
    """
    if match_date.month >= 7:  # July onwards is start of new season
        return format_season(match_date.year)
    else:
        return format_season(match_date.year - 1)


def iter_seasons(start_year: int, end_year: int) -> Iterator[str]:
    """
    Iterate through seasons from start_year to end_year (inclusive).

    Examples:
        iter_seasons(1920, 1922) yields "1920-1921", "1921-1922"
    """
    for year in range(start_year, end_year):
        yield format_season(year)
