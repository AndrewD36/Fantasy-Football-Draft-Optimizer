import json
import os
import sqlite3
import time
from pathlib import Path
import httpx

SLEEPER_PLAYERS_URL = "https://api.sleeper.app/v1/players/nfl"
SCRIPT_DIR = Path(__file__).resolve().parent      # .../db/scripts
DB_DIR = SCRIPT_DIR.parent                        # .../db
DB_PATH = DB_DIR / "sleeper.sqlite"               # .../db/sleeper.sqlite

def ensure_db(conn: sqlite3.Connection) -> None:
    # Better concurrency / fewer "database is locked" issues
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS players (
        player_id TEXT PRIMARY KEY,

        first_name TEXT,
        last_name TEXT,
        full_name TEXT,
        search_full_name TEXT,
        search_first_name TEXT,
        search_last_name TEXT,

        team TEXT,
        position TEXT,
        status TEXT,
        sport TEXT,

        number INTEGER,
        age INTEGER,

        depth_chart_position INTEGER,
        depth_chart_order INTEGER,
        years_exp INTEGER,

        fantasy_positions_json TEXT,

        data_json TEXT NOT NULL,
        updated_at INTEGER NOT NULL
    );
    """)

    # Helpful indexes for common queries
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_team ON players(team);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_position ON players(position);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_status ON players(status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_search_full_name ON players(search_full_name);")

def is_cached(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM meta WHERE key='players_cached'").fetchone()
    return bool(row and row[0] == "1")


def mark_cached(conn: sqlite3.Connection, count: int) -> None:
    formatted_time = str(time.ctime(time.time()))
    conn.execute(
        "INSERT INTO meta(key,value) VALUES('players_cached','1') "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
    )
    conn.execute(
        "INSERT INTO meta(key,value) VALUES('players_cached_at', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (formatted_time,)
    )
    conn.execute(
        "INSERT INTO meta(key,value) VALUES('players_count', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(count),)
    )

def fetch_players() -> dict:
    with httpx.Client(timeout=90) as client:
        r = client.get(SLEEPER_PLAYERS_URL)
        r.raise_for_status()
        return r.json()  # dict keyed by player_id

# def main(force: bool = False) -> None:
def main():
    conn = sqlite3.connect(DB_PATH)

    try:
        ensure_db(conn)

        # if not force and is_cached(conn):
        #     print(f"Already cached in {DB_PATH}. Set force=True to refetch.")
        #     return

        print("Fetching players from Sleeper...")
        players_obj = fetch_players()
        now = int(time.time())

        # Prepare rows (safe .get for missing keys)
        rows = []
        for player_id, p in players_obj.items():
            first = p.get("first_name")
            last = p.get("last_name")
            full_name = " ".join([x for x in [first, last] if x]) or p.get("full_name")

            fantasy_positions = p.get("fantasy_positions")
            fantasy_positions_json = json.dumps(fantasy_positions) if fantasy_positions is not None else None

            rows.append((
                str(p.get("player_id") or player_id),

                first,
                last,
                full_name,
                p.get("search_full_name"),
                p.get("search_first_name"),
                p.get("search_last_name"),

                p.get("team"),
                p.get("position"),
                p.get("status"),
                p.get("sport"),

                p.get("number"),
                p.get("age"),

                p.get("depth_chart_position"),
                p.get("depth_chart_order"),
                p.get("years_exp"),

                fantasy_positions_json,

                json.dumps(p, separators=(",", ":")),
                now
            ))

        conn.execute("BEGIN;")
        conn.executemany(
            """
            INSERT INTO players (
                player_id,
                first_name, last_name, full_name,
                search_full_name, search_first_name, search_last_name,
                team, position, status, sport,
                number, age,
                depth_chart_position, depth_chart_order, years_exp,
                fantasy_positions_json,
                data_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                full_name=excluded.full_name,
                search_full_name=excluded.search_full_name,
                search_first_name=excluded.search_first_name,
                search_last_name=excluded.search_last_name,
                team=excluded.team,
                position=excluded.position,
                status=excluded.status,
                sport=excluded.sport,
                number=excluded.number,
                age=excluded.age,
                depth_chart_position=excluded.depth_chart_position,
                depth_chart_order=excluded.depth_chart_order,
                years_exp=excluded.years_exp,
                fantasy_positions_json=excluded.fantasy_positions_json,
                data_json=excluded.data_json,
                updated_at=excluded.updated_at
            """,
            rows
        )

        mark_cached(conn, len(rows))
        conn.commit()

        print(f"Cached {len(rows)} players into {DB_PATH}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
    # import os
    # force = os.getenv("FORCE", "0") == "1"
    # main(force=force)