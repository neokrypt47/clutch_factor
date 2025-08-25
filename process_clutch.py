"""Generate clutch statistics from the SQLite events database.

This script reads goal events from ``clutch.db`` and computes late-game
goals and assists (minute >= 76).  Results are written to both a CSV file
and the ``clutch_summary`` table inside the same database.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = Path("clutch.db")


def load_clutch_events(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return goal events with joined metadata needed for clutch stats."""
    query = """
        SELECT e.fixture_id,
               e.league_id,
               l.name   AS league,
               e.season,
               t.id     AS team_id,
               t.name   AS team_name,
               p.id     AS player_id,
               p.name   AS player_name,
               e.time_elapsed,
               a.id     AS assist_id,
               a.name   AS assist_name
        FROM events e
        JOIN leagues l ON e.league_id = l.id
        JOIN teams   t ON e.team_id = t.id
        JOIN players p ON e.player_id = p.id
        LEFT JOIN players a ON e.assist_id = a.id
        WHERE e.type = 'Goal'
          AND e.detail IN ('Normal Goal', 'Penalty', 'Own Goal')
          AND e.time_elapsed >= 76
    """
    return pd.read_sql_query(query, conn)


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    df = load_clutch_events(conn)
    print("Clutch goal events:", df.shape[0])

    # Scoring players
    goals = df[
        [
            "player_id",
            "player_name",
            "team_id",
            "team_name",
            "league_id",
            "league",
            "season",
            "fixture_id",
        ]
    ].copy()
    goals["Clutch_Goal"] = 1
    goals["Clutch_Assist"] = 0

    # Assisting players (if any)
    assists = df[df["assist_name"].notnull()][
        [
            "assist_id",
            "assist_name",
            "team_id",
            "team_name",
            "league_id",
            "league",
            "season",
            "fixture_id",
        ]
    ].copy()
    assists.rename(
        columns={"assist_id": "player_id", "assist_name": "player_name"},
        inplace=True,
    )
    assists["Clutch_Goal"] = 0
    assists["Clutch_Assist"] = 1

    clutch_df = pd.concat([goals, assists], ignore_index=True)

    # Clean player names that may include leading squad numbers or extra whitespace
    clutch_df["player_name"] = (
        clutch_df["player_name"]
        .str.replace(r"^\d+[\s.-]*", "", regex=True)
        .str.strip()
    )

    summary = (
        clutch_df.groupby(
            [
                "player_id",
                "player_name",
                "team_id",
                "team_name",
                "league_id",
                "league",
                "season",
            ]
        )
        .agg(
            {
                "fixture_id": "nunique",
                "Clutch_Goal": "sum",
                "Clutch_Assist": "sum",
            }
        )
        .rename(columns={"fixture_id": "Clutch_Matches"})
        .reset_index()
    )

    summary.rename(columns={"season": "year"}, inplace=True)
    summary["Clutch_Score"] = summary["Clutch_Goal"] * 3 + summary["Clutch_Assist"] * 2
    summary["Score_per_Match"] = summary["Clutch_Score"] / summary["Clutch_Matches"]
    summary = summary.sort_values("Clutch_Score", ascending=False)

    summary.to_csv("clutch_summary.csv", index=False)

    cur = conn.cursor()
    cur.execute("DELETE FROM clutch_summary")
    cur.executemany(
        """
        INSERT INTO clutch_summary(
            player_id, player_name, team_id, team_name, league_id, league,
            year, clutch_matches, clutch_goal, clutch_assist,
            clutch_score, score_per_match
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                int(row.player_id),
                row.player_name,
                int(row.team_id),
                row.team_name,
                int(row.league_id),
                row.league,
                int(row.year),
                int(row.Clutch_Matches),
                int(row.Clutch_Goal),
                int(row.Clutch_Assist),
                int(row.Clutch_Score),
                float(row.Score_per_Match),
            )
            for row in summary.itertuples(index=False)
        ],
    )
    conn.commit()
    conn.close()

    print("✅ clutch_summary table updated with", summary.shape[0], "rows")


if __name__ == "__main__":
    main()

