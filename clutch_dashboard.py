import sqlite3
import streamlit as st
import pandas as pd
import plotly.express as px

DB_PATH = "clutch.db"


@st.cache_data
def load_data() -> pd.DataFrame:
    """Return the clutch summary table from the SQLite database.

    The CSV files that seed the database use capitalized column names such as
    ``Clutch_Score`` while the SQLite schema stores them in snake_case
    (``clutch_score``).  When pandas reads from the database we therefore
    normalize the column names back to the capitalized style expected by the
    dashboard.
    """

    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM clutch_summary", conn)

    # Map the database column names to the ones referenced elsewhere in the
    # dashboard.  Only rename columns that are present to keep the function
    # resilient to future schema changes.
    rename_map = {
        "clutch_matches": "Clutch_Matches",
        "clutch_goal": "Clutch_Goal",
        "clutch_assist": "Clutch_Assist",
        "clutch_score": "Clutch_Score",
        "score_per_match": "Score_per_Match",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    return df


df = load_data()


# Determine which column describes the league. Prefer the readable name
# if present, otherwise fall back to the numeric identifier.
league_col = "league" if "league" in df.columns else "league_id" if "league_id" in df.columns else None

df.set_index("player_name", inplace=True)


st.set_page_config(page_title="Clutch Player Dashboard", layout="wide")
st.title("🧠 Clutch Score Dashboard")
st.markdown("Visualizing late-game goals and assists under pressure (Minute ≥ 76).")

# Dropdown filters for league and year

league_options = sorted(df[league_col].dropna().unique()) if league_col else []
league = st.selectbox("League", ["All"] + list(league_options))
if league != "All" and league_col:
    df = df[df[league_col] == league]

year_options = sorted(df["year"].dropna().unique()) if "year" in df.columns else []
year = st.selectbox("Year", ["All"] + list(year_options))
if year != "All" and "year" in df.columns:

    df = df[df["year"] == year]

# Build a unique list of player names after filtering
player_names = sorted(df.index.str.strip().drop_duplicates())

# Show top N players
top_n = st.slider("Show Top N Players", min_value=5, max_value=50, value=10)
top_df = df.sort_values("Clutch_Score", ascending=False).head(top_n).reset_index()

# 📊 Interactive bar chart with Plotly
fig = px.bar(
    top_df,
    x="player_name",
    y="Clutch_Score",
    color="Clutch_Score",
    hover_data=["Clutch_Goal", "Clutch_Assist", "Score_per_Match", "Clutch_Matches"],
    labels={"player_name": "Player", "Clutch_Score": "Clutch Score"},
    title=f"Top {top_n} Clutch Players"
)

fig.update_layout(
    xaxis_tickangle=-45,
    hovermode="x",
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)

st.plotly_chart(fig, use_container_width=True)

# Optional: Player selection
player = st.selectbox("Select a Player to View Stats", player_names)
if player:
    st.subheader(f"📊 Stats for {player}")
    st.dataframe(df.loc[[player]])  # note the double brackets
