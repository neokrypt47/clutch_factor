import os
import requests
import time
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("RAPIDAPI_KEY")
HOST = "api-football-v1.p.rapidapi.com"

HEADERS = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": HOST
}

# League & season
LEAGUE_ID = 39  # English Premier League
SEASON = 2023

def get_fixtures(league_id, season):
    url = f"https://{HOST}/v3/fixtures"
    params = {"league": league_id, "season": season}
    res = requests.get(url, headers=HEADERS, params=params)
    res.raise_for_status()
    fixtures = res.json()['response']
    return [f['fixture']['id'] for f in fixtures]

def get_events(fixture_id):
    url = f"https://{HOST}/v3/fixtures/events"
    params = {"fixture": fixture_id}
    res = requests.get(url, headers=HEADERS, params=params)
    res.raise_for_status()
    return res.json()['response']

def main():
    fixture_ids = get_fixtures(LEAGUE_ID, SEASON)
    print(f"Found {len(fixture_ids)} fixtures.")

    all_events = []
    for i, fixture_id in enumerate(fixture_ids):
        print(f"[{i+1}/{len(fixture_ids)}] Fetching events for match {fixture_id}")
        try:
            events = get_events(fixture_id)
            for e in events:
                e['fixture_id'] = fixture_id
                all_events.append(e)
            time.sleep(1)  # Respect rate limit
        except Exception as ex:
            print(f"Failed for fixture {fixture_id}: {ex}")

    # Convert to DataFrame and save
    df = pd.json_normalize(all_events)
    df.to_csv("events_raw.csv", index=False)
    print("✅ Saved events_raw.csv")

if __name__ == "__main__":
    main()
