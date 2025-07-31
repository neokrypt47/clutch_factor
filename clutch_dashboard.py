import streamlit as st
import pandas as pd
import plotly.express as px

# Load the clutch data
df = pd.read_csv("clutch_summary.csv", index_col='player_name')

st.set_page_config(page_title="Clutch Player Dashboard", layout="wide")
st.title("🧠 Clutch Score Dashboard")
st.markdown("Visualizing late-game goals and assists under pressure (Minute ≥ 76).")

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
player = st.selectbox("Select a Player to View Stats", df.index.sort_values())
if player:
    st.subheader(f"📊 Stats for {player}")
    st.write(df.loc[player])
