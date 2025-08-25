import pandas as pd

def test_player_names_do_not_start_with_digit():
    df = pd.read_csv("clutch_summary.csv")
    assert not any(name[0].isdigit() for name in df["player_name"])
