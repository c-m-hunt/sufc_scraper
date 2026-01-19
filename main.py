#!/usr/bin/env python3
"""Master controller for scraping Southend United match history."""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

from models.match import Match
from scrapers.football_data import FootballDataScraper
from scrapers.statto import StattoScraper
from scrapers.transfermarkt import TransfermarktScraper
from storage.database import Database
from utils.http_client import HttpClient
from utils.season_utils import format_season

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ScrapeController:
    """Orchestrates scraping from multiple sources."""

    def __init__(self, db: Database):
        self.db = db
        self.http_client = HttpClient(rate_limit=2.0)

        # Initialize scrapers with source priority
        # Lower years use earlier sources in the list
        self.scrapers = [
            StattoScraper(HttpClient(rate_limit=2.0)),
            TransfermarktScraper(HttpClient(rate_limit=3.0)),
            FootballDataScraper(HttpClient(rate_limit=1.0)),
        ]

    def get_scraper_for_season(self, start_year: int):
        """Get the best scraper for a given season."""
        for scraper in self.scrapers:
            if scraper.can_scrape_season(start_year):
                return scraper
        return None

    def scrape_season(self, start_year: int, source: str | None = None) -> int:
        """
        Scrape a single season.

        Args:
            start_year: Start year of the season
            source: Optional specific source to use

        Returns:
            Number of matches scraped
        """
        season = format_season(start_year)
        logger.info(f"Scraping season {season}")

        if source:
            # Use specific source
            scraper_map = {
                "football-data": FootballDataScraper,
                "transfermarkt": TransfermarktScraper,
                "statto": StattoScraper,
            }
            scraper_class = scraper_map.get(source)
            if not scraper_class:
                logger.error(f"Unknown source: {source}")
                return 0
            scrapers_to_try = [scraper_class(self.http_client)]
        else:
            # Get all scrapers that can handle this season, in priority order
            scrapers_to_try = [s for s in self.scrapers if s.can_scrape_season(start_year)]

        if not scrapers_to_try:
            logger.warning(f"No scraper available for {season}")
            return 0

        # Try each scraper until one returns matches
        for scraper in scrapers_to_try:
            logger.info(f"Trying {scraper.SOURCE_NAME} for {season}")
            matches = scraper.scrape_season(start_year)

            if matches:
                count = 0
                for match in matches:
                    self.db.upsert_match(match)
                    count += 1
                logger.info(f"Stored {count} matches for {season} from {scraper.SOURCE_NAME}")
                return count
            else:
                logger.info(f"{scraper.SOURCE_NAME} returned no matches, trying next source...")

        logger.warning(f"No matches found for {season} from any source")
        return 0

    def scrape_range(
        self, start_year: int, end_year: int, source: str | None = None
    ) -> int:
        """
        Scrape a range of seasons.

        Args:
            start_year: First season start year
            end_year: Last season start year (inclusive)
            source: Optional specific source to use

        Returns:
            Total matches scraped
        """
        total = 0
        for year in range(start_year, end_year + 1):
            count = self.scrape_season(year, source)
            total += count

        logger.info(f"Total matches scraped: {total}")
        return total

    def scrape_all(self) -> int:
        """Scrape all available seasons."""
        # Southend United founded 1906, but data starts 1909
        return self.scrape_range(1909, 2025)


def export_csv(db: Database, output_path: str) -> None:
    """Export all matches to CSV."""
    matches = db.get_all_matches()

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date", "opposition", "venue", "goals_for", "goals_against",
            "result", "competition", "season", "attendance", "referee",
            "scorers", "lineup", "source"
        ])

        for match in matches:
            writer.writerow([
                match.date.isoformat(),
                match.opposition,
                match.venue,
                match.goals_for,
                match.goals_against,
                match.result,
                match.competition,
                match.season,
                match.attendance or "",
                match.referee or "",
                ";".join(match.scorers) if match.scorers else "",
                ";".join(match.lineup) if match.lineup else "",
                match.source,
            ])

    logger.info(f"Exported {len(matches)} matches to {output_path}")


def export_json(db: Database, output_path: str) -> None:
    """Export all matches to JSON."""
    matches = db.get_all_matches()

    data = []
    for match in matches:
        data.append({
            "date": match.date.isoformat(),
            "opposition": match.opposition,
            "venue": match.venue,
            "goals_for": match.goals_for,
            "goals_against": match.goals_against,
            "result": match.result,
            "competition": match.competition,
            "season": match.season,
            "attendance": match.attendance,
            "referee": match.referee,
            "scorers": match.scorers,
            "lineup": match.lineup,
            "source": match.source,
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Exported {len(matches)} matches to {output_path}")


def print_stats(db: Database) -> None:
    """Print database statistics."""
    total = db.get_match_count()
    print(f"\nTotal matches: {total}")

    print("\nSeason Summary:")
    print("-" * 70)
    print(f"{'Season':<12} {'Matches':>8} {'W':>4} {'D':>4} {'L':>4} {'GF':>5} {'GA':>5}")
    print("-" * 70)

    for row in db.get_season_summary():
        print(
            f"{row['season']:<12} {row['matches']:>8} "
            f"{row['wins']:>4} {row['draws']:>4} {row['losses']:>4} "
            f"{row['scored']:>5} {row['conceded']:>5}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Southend United match history"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Scrape command
    scrape_parser = subparsers.add_parser("scrape", help="Scrape match data")
    scrape_parser.add_argument(
        "--season", type=int, help="Single season to scrape (start year)"
    )
    scrape_parser.add_argument(
        "--start", type=int, help="Start year for range"
    )
    scrape_parser.add_argument(
        "--end", type=int, help="End year for range"
    )
    scrape_parser.add_argument(
        "--source",
        choices=["football-data", "transfermarkt", "statto"],
        help="Specific source to use",
    )
    scrape_parser.add_argument(
        "--all", action="store_true", help="Scrape all available data"
    )
    scrape_parser.add_argument(
        "--db", default="data/matches.db", help="Database path"
    )

    # Export command
    export_parser = subparsers.add_parser("export", help="Export match data")
    export_parser.add_argument(
        "--format", choices=["csv", "json"], default="csv", help="Export format"
    )
    export_parser.add_argument(
        "--output", "-o", required=True, help="Output file path"
    )
    export_parser.add_argument(
        "--db", default="data/matches.db", help="Database path"
    )

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show statistics")
    stats_parser.add_argument(
        "--db", default="data/matches.db", help="Database path"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Change to project directory
    project_dir = Path(__file__).parent
    db_path = project_dir / args.db

    db = Database(db_path)

    if args.command == "scrape":
        controller = ScrapeController(db)

        if args.all:
            controller.scrape_all()
        elif args.season:
            controller.scrape_season(args.season, args.source)
        elif args.start and args.end:
            controller.scrape_range(args.start, args.end, args.source)
        else:
            print("Please specify --season, --start/--end, or --all")
            return 1

        print_stats(db)

    elif args.command == "export":
        if args.format == "csv":
            export_csv(db, args.output)
        else:
            export_json(db, args.output)

    elif args.command == "stats":
        print_stats(db)

    return 0


if __name__ == "__main__":
    sys.exit(main())
