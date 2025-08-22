import pandas as pd

df = pd.read_csv("events_raw.csv")
print("Total events:", df.shape[0])

# Step 1: Normalize fields
required_cols = { 'player.name', 'league', 'season', 'team.name'}
missing = [c for c in required_cols if c not in df.columns]
if 'team.name' not in df.columns and 'team.name' not in df.columns:
    missing.append('team_name')
if missing:
    raise ValueError(f"Missing columns in raw data: {missing}")

df['minute'] = df['time.elapsed']
df['player_name'] = df['player.name']
df['team_name'] = df['team_name'] if 'team_name' in df.columns else df['team.name']
df['team_id'] = df['team_id'] if 'team_id' in df.columns else df['team.id']

# Step 2: Filter for clutch-relevant events
goals = df[(df['type'] == 'Goal') & (df['detail'].isin(['Normal Goal', 'Penalty', 'Own Goal']))]
assists = df[df['detail'] == 'Assist']

print("Goals found:", goals.shape[0])
print("Assists found:", assists.shape[0])

# Step 3: Filter to clutch time (minute >= 76)
goals = goals[goals['minute'] >= 76]
assists = assists[assists['minute'] >= 76]

print("Clutch goals:", goals.shape[0])
print("Clutch assists:", assists.shape[0])

# Step 4: Tag & merge
goals['Clutch_Goal'] = 1
goals['Clutch_Assist'] = 0

assists['Clutch_Goal'] = 0
assists['Clutch_Assist'] = 1

clutch_df = pd.concat([goals, assists])
# Preserve both league name and ID so downstream consumers can show readable
# options yet still filter by the underlying identifier if needed.
clutch_df = clutch_df[
    [
        'player_name',
        'team_name',
        'fixture_id',
        'league',
        'season',
        'Clutch_Goal',
        'Clutch_Assist',
    ]
]

# Step 5: Group and summarize
summary = (
    clutch_df.groupby(['player_name', 'team_name', 'league', 'season'])
    .agg(
        {
            'fixture_id': 'nunique',
            'Clutch_Goal': 'sum',
            'Clutch_Assist': 'sum',
        }
    )
    .rename(columns={'fixture_id': 'Clutch_Matches'})
    .reset_index()
)

summary.rename(columns={'season': 'year'}, inplace=True)
summary['Clutch_Score'] = summary['Clutch_Goal'] * 3 + summary['Clutch_Assist'] * 2
summary['Score_per_Match'] = summary['Clutch_Score'] / summary['Clutch_Matches']
summary = summary.sort_values('Clutch_Score', ascending=False)

summary.to_csv("clutch_summary.csv", index=False)
print("✅ clutch_summary.csv generated with", summary.shape[0], "rows")
