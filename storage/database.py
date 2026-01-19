import json
import sqlite3
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Iterator, Optional

from models.match import Match


class Database:
    """SQLite storage layer for match data."""

    def __init__(self, db_path: str | Path = "data/matches.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        """Initialize the database schema."""
        with self._connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    opposition TEXT NOT NULL,
                    venue TEXT NOT NULL CHECK (venue IN ('H', 'A', 'N')),
                    goals_for INTEGER NOT NULL,
                    goals_against INTEGER NOT NULL,
                    competition TEXT NOT NULL,
                    season TEXT NOT NULL,
                    attendance INTEGER,
                    referee TEXT,
                    scorers TEXT,  -- JSON array
                    lineup TEXT,   -- JSON array
                    source TEXT,
                    source_match_id TEXT,
                    detail_fetched INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, opposition, competition)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_matches_season ON matches(season)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_matches_opposition ON matches(opposition)
            """)

    def insert_match(self, match: Match) -> int:
        """Insert a match into the database. Returns the row ID."""
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO matches (
                    date, opposition, venue, goals_for, goals_against,
                    competition, season, attendance, referee, scorers, lineup,
                    source, source_match_id, detail_fetched
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    match.date.isoformat(),
                    match.opposition,
                    match.venue,
                    match.goals_for,
                    match.goals_against,
                    match.competition,
                    match.season,
                    match.attendance,
                    match.referee,
                    json.dumps(match.scorers) if match.scorers else None,
                    json.dumps(match.lineup) if match.lineup else None,
                    match.source,
                    match.source_match_id,
                    1 if match.detail_fetched else 0,
                ),
            )
            return cursor.lastrowid

    def upsert_match(self, match: Match) -> int:
        """Insert or update a match. Returns the row ID."""
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO matches (
                    date, opposition, venue, goals_for, goals_against,
                    competition, season, attendance, referee, scorers, lineup,
                    source, source_match_id, detail_fetched
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, opposition, competition) DO UPDATE SET
                    venue = excluded.venue,
                    goals_for = excluded.goals_for,
                    goals_against = excluded.goals_against,
                    season = excluded.season,
                    attendance = COALESCE(excluded.attendance, attendance),
                    referee = COALESCE(excluded.referee, referee),
                    scorers = COALESCE(excluded.scorers, scorers),
                    lineup = COALESCE(excluded.lineup, lineup),
                    source = CASE WHEN excluded.source != '' THEN excluded.source ELSE source END,
                    source_match_id = COALESCE(excluded.source_match_id, source_match_id),
                    detail_fetched = MAX(detail_fetched, excluded.detail_fetched),
                    updated_at = CURRENT_TIMESTAMP
            """,
                (
                    match.date.isoformat(),
                    match.opposition,
                    match.venue,
                    match.goals_for,
                    match.goals_against,
                    match.competition,
                    match.season,
                    match.attendance,
                    match.referee,
                    json.dumps(match.scorers) if match.scorers else None,
                    json.dumps(match.lineup) if match.lineup else None,
                    match.source,
                    match.source_match_id,
                    1 if match.detail_fetched else 0,
                ),
            )
            return cursor.lastrowid

    def get_match(self, match_id: int) -> Optional[Match]:
        """Get a match by ID."""
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM matches WHERE id = ?", (match_id,)
            ).fetchone()
            return self._row_to_match(row) if row else None

    def get_matches_by_season(self, season: str) -> list[Match]:
        """Get all matches for a season."""
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM matches WHERE season = ? ORDER BY date",
                (season,),
            ).fetchall()
            return [self._row_to_match(row) for row in rows]

    def get_all_matches(self) -> list[Match]:
        """Get all matches ordered by date."""
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM matches ORDER BY date"
            ).fetchall()
            return [self._row_to_match(row) for row in rows]

    def get_matches_needing_enrichment(self, source: Optional[str] = None) -> list[Match]:
        """Get matches that haven't had detail fetched."""
        with self._connection() as conn:
            if source:
                rows = conn.execute(
                    """SELECT * FROM matches
                       WHERE detail_fetched = 0 AND source = ? AND source_match_id IS NOT NULL
                       ORDER BY date""",
                    (source,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM matches
                       WHERE detail_fetched = 0 AND source_match_id IS NOT NULL
                       ORDER BY date"""
                ).fetchall()
            return [self._row_to_match(row) for row in rows]

    def get_match_count(self) -> int:
        """Get total number of matches."""
        with self._connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]

    def get_season_summary(self) -> list[dict]:
        """Get a summary of matches per season."""
        with self._connection() as conn:
            rows = conn.execute("""
                SELECT season, COUNT(*) as matches,
                       SUM(CASE WHEN goals_for > goals_against THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN goals_for = goals_against THEN 1 ELSE 0 END) as draws,
                       SUM(CASE WHEN goals_for < goals_against THEN 1 ELSE 0 END) as losses,
                       SUM(goals_for) as scored, SUM(goals_against) as conceded
                FROM matches GROUP BY season ORDER BY season
            """).fetchall()
            return [dict(row) for row in rows]

    def _row_to_match(self, row: sqlite3.Row) -> Match:
        """Convert a database row to a Match object."""
        return Match(
            date=date.fromisoformat(row["date"]),
            opposition=row["opposition"],
            venue=row["venue"],
            goals_for=row["goals_for"],
            goals_against=row["goals_against"],
            competition=row["competition"],
            season=row["season"],
            attendance=row["attendance"],
            referee=row["referee"],
            scorers=json.loads(row["scorers"]) if row["scorers"] else None,
            lineup=json.loads(row["lineup"]) if row["lineup"] else None,
            source=row["source"] or "",
            source_match_id=row["source_match_id"],
            detail_fetched=bool(row["detail_fetched"]),
        )
