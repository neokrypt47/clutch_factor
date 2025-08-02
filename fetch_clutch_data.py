import os
import time
import asyncio
from datetime import date
from typing import List, Dict

import pandas as pd
import requests
import httpx
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()
API_KEY = os.getenv("RAPIDAPI_KEY")
HOST = "api-football-v1.p.rapidapi.com"

HEADERS = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": HOST
}

# Top leagues and recent seasons
LEAGUES: Dict[int, str] = {
    39: "Premier League",
    140: "La Liga",
    61: "Ligue 1",
    78: "Bundesliga",
    135: "Serie A",
}

CURRENT_YEAR = date.today().year
SEASONS = list(range(CURRENT_YEAR, CURRENT_YEAR - 5, -1))

# Configure a requests session with retry and exponential backoff
RETRY_STRATEGY = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
SESSION = requests.Session()
ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)
SESSION.mount("https://", ADAPTER)
SESSION.mount("http://", ADAPTER)

# Pause between requests (seconds) to respect API rate limits
THROTTLE = 1

def get_fixtures(league_id: int, season: int) -> List[int]:
    url = f"https://{HOST}/v3/fixtures"
    params = {"league": league_id, "season": season}
    res = SESSION.get(url, headers=HEADERS, params=params)
    res.raise_for_status()
    fixtures = res.json()['response']
    return [f['fixture']['id'] for f in fixtures]

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def get_events(fixture_id: int) -> List[dict]:
    url = f"https://{HOST}/v3/fixtures/events"
    params = {"fixture": fixture_id}
    res = SESSION.get(url, headers=HEADERS, params=params)
    res.raise_for_status()
    return res.json()['response']

async def fetch_events_async(
    fixture_ids: List[int], league: str, season: int
) -> List[dict]:
    """Fetch events concurrently using httpx."""
    events: List[dict] = []
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient() as client:

        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            reraise=True,
        )
        async def _get(fixture_id: int):
            async with sem:
                url = f"https://{HOST}/v3/fixtures/events"
                params = {"fixture": fixture_id}
                res = await client.get(url, headers=HEADERS, params=params)
                res.raise_for_status()
                await asyncio.sleep(THROTTLE)
                data = res.json()["response"]
            for e in data:
                e["fixture_id"] = fixture_id
                e["league"] = league
                e["season"] = season
                events.append(e)

        tasks = [asyncio.create_task(_get(fid)) for fid in fixture_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for fid, result in zip(fixture_ids, results):
            if isinstance(result, Exception):
                print(f"Failed for fixture {fid}: {result}")
    return events


def main(use_async: bool = False) -> None:
    all_events: List[dict] = []

    for season in SEASONS:
        for league_id, league_name in LEAGUES.items():
            fixture_ids = get_fixtures(league_id, season)
            print(f"Found {len(fixture_ids)} fixtures for {league_name} {season}.")

            if use_async:
                events = asyncio.run(
                    fetch_events_async(fixture_ids, league_name, season)
                )
                all_events.extend(events)
            else:
                for i, fixture_id in enumerate(fixture_ids):
                    print(
                        f"[{i+1}/{len(fixture_ids)}] {league_name} {season} match {fixture_id}"
                    )
                    try:
                        events = get_events(fixture_id)
                        for e in events:
                            e["fixture_id"] = fixture_id
                            e["league"] = league_name
                            e["season"] = season
                            all_events.append(e)
                        time.sleep(THROTTLE)
                    except Exception as ex:
                        print(f"Failed for fixture {fixture_id}: {ex}")

    # Convert to DataFrame and save
    df = pd.json_normalize(all_events)
    df.to_csv("events_raw.csv", index=False)
    print("✅ Saved events_raw.csv")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch match events")
    parser.add_argument(
        "--async",
        dest="use_async",
        action="store_true",
        help="Fetch events concurrently using httpx",
    )

    args = parser.parse_args()
    main(use_async=args.use_async)
