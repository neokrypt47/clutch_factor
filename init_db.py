"""Create an SQLite database from the available CSV files.

This script reads `events_raw.csv` and `clutch_summary.csv` and
normalizes the data into separate tables for leagues, teams, players, and
events.  The resulting database is saved as `clutch.db` in the project
root.
"""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Dict

DB_PATH = Path("clutch.db")
EVENTS_CSV = Path("events_raw.csv")
SUMMARY_CSV = Path("clutch_summary.csv")

# Known league identifiers from the API in case the CSV lacks them
LEAGUE_IDS = {
    "Premier League": 39,
    "La Liga": 140,
    "Ligue 1": 61,
    "Bundesliga": 78,
    "Serie A": 135,
}


def create_tables(conn: sqlite3.Connection) -> None:
    """Create empty tables in the SQLite database."""
    cur = conn.cursor()

    # Core lookup tables
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS leagues (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY,
            name TEXT,
            logo TEXT,
            league_id INTEGER,
            FOREIGN KEY (league_id) REFERENCES leagues(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY,
            name TEXT,
            image TEXT
        )
        """
    )

    # Events table linking to leagues, teams and players
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT,
            detail TEXT,
            comments TEXT,
            fixture_id INTEGER,
            league_id INTEGER,
            season INTEGER,
            time_elapsed INTEGER,
            time_extra INTEGER,
            team_id INTEGER,
            player_id INTEGER,
            assist_id INTEGER,
            FOREIGN KEY (league_id) REFERENCES leagues(id),
            FOREIGN KEY (team_id) REFERENCES teams(id),
            FOREIGN KEY (player_id) REFERENCES players(id),
            FOREIGN KEY (assist_id) REFERENCES players(id)
        )
        """
    )

    # Summary table derived from clutch_summary.csv
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clutch_summary (
            player_id INTEGER,
            player_name TEXT,
            team_id INTEGER,
            team_name TEXT,
            league_id INTEGER,
            league TEXT,
            year INTEGER,
            clutch_matches INTEGER,
            clutch_goal INTEGER,
            clutch_assist INTEGER,
            clutch_score INTEGER,
            score_per_match REAL,
            FOREIGN KEY (player_id) REFERENCES players(id),
            FOREIGN KEY (team_id) REFERENCES teams(id),
            FOREIGN KEY (league_id) REFERENCES leagues(id)
        )
        """
    )

    conn.commit()


def load_events(conn: sqlite3.Connection) -> None:
    """Populate the database with event data from EVENTS_CSV."""
    cur = conn.cursor()
    league_cache: Dict[int, int] = {}

    with EVENTS_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            league_name = row["league"]
            # Determine API league ID, falling back to known constants
            league_id = row.get("league_id") or LEAGUE_IDS.get(league_name)
            if league_id is None:
                raise ValueError(f"Unknown league id for {league_name}")
            league_id = int(league_id)

            # Insert league if unseen
            if league_id not in league_cache:
                cur.execute(
                    "INSERT OR IGNORE INTO leagues(id, name) VALUES (?, ?)",
                    (league_id, league_name),
                )
                league_cache[league_id] = league_id

            # Insert team
            team_id = int(row.get("team.id") or row.get("team_id"))
            cur.execute(
                "INSERT OR IGNORE INTO teams(id, name, logo, league_id) VALUES (?, ?, ?, ?)",
                (team_id, row.get("team.name") or row.get("team_name"), row.get("team.logo") or row.get("team_logo"), league_id),
            )

            # Insert players (main player and assister, if any)
            player_id = row.get("player.id") or row.get("player_id")
            player_name = row.get("player.name") or row.get("player_name")
            player_img = row.get("player.photo") or row.get("player_photo")
            if player_id and player_name:
                cur.execute(
                    "INSERT OR IGNORE INTO players(id, name, image) VALUES (?, ?, ?)",
                    (int(player_id), player_name, player_img),
                )
            assist_id = row.get("assist.id") or row.get("assist_id")
            assist_name = row.get("assist.name") or row.get("assist_name")
            assist_img = row.get("assist.photo") or row.get("assist_photo")
            if assist_id and assist_name:
                # assist IDs are sometimes stored as floats in the CSV
                cur.execute(
                    "INSERT OR IGNORE INTO players(id, name, image) VALUES (?, ?, ?)",
                    (int(float(assist_id)), assist_name, assist_img),
                )

            # Insert event row
            cur.execute(
                """
                INSERT INTO events(
                    type, detail, comments, fixture_id, league_id, season,
                    time_elapsed, time_extra, team_id, player_id, assist_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["type"],
                    row["detail"],
                    row.get("comments"),
                    int(row["fixture_id"]),
                    league_id,
                    int(row["season"]),
                    int(row["time.elapsed"]) if row["time.elapsed"] else None,
                    int(float(row["time.extra"])) if row["time.extra"] else None,
                    team_id,
                    int(player_id) if player_id else None,
                    int(float(assist_id)) if assist_id else None,
                ),
            )

    conn.commit()


def load_summary(conn: sqlite3.Connection) -> None:
    """Populate the clutch_summary table from SUMMARY_CSV."""
    cur = conn.cursor()

    with SUMMARY_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                int(float(r.get("player_id"))) if r.get("player_id") else None,
                r.get("player_name"),
                int(float(r.get("team_id"))) if r.get("team_id") else None,
                r.get("team_name"),
                int(float(r.get("league_id"))) if r.get("league_id") else None,
                r.get("league"),
                int(r["year"]),
                int(r["Clutch_Matches"]),
                int(r["Clutch_Goal"]),
                int(r["Clutch_Assist"]),
                int(r["Clutch_Score"]),
                float(r["Score_per_Match"]),
            )
            for r in reader
        ]
    cur.executemany(
        """
        INSERT INTO clutch_summary(
            player_id, player_name, team_id, team_name, league_id, league,
            year, clutch_matches, clutch_goal, clutch_assist, clutch_score,
            score_per_match
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def main() -> None:
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)
    load_events(conn)
    load_summary(conn)
    conn.close()
    print(f"✅ Created database at {DB_PATH}")


if __name__ == "__main__":
    main()
