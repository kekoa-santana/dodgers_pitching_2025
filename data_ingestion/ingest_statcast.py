from pybaseball import statcast_pitcher
from sqlalchemy import create_engine, text
import pandas as pd

from utils.utils import build_db_url

engine = create_engine(build_db_url(), pool_pre_ping=False)

def get_pitcher_ids() -> []:
    query = text("SELECT DISTINCT pitcher_id FROM raw.pitching_boxscores")

    with engine.begin() as conn:
        result = conn.execute(query)
        pitcher_ids = [row[0] for row in result]

    return pitcher_ids

def extract_statcast(pitcher_ids):
    frames = []

    for pitcher_id in pitcher_ids:
        df = statcast_pitcher('2025-03-18', '2025-11-01', pitcher_id)
        if df is not None:
            frames.append(df)

    all_stats = pd.concat(frames, ignore_index=True)
    all_stats.to_csv("statcast_pitching_lad_2025.csv", index = False)

def main():
    pitcher_ids = get_pitcher_ids()

    extract_statcast(pitcher_ids)

if __name__ == "__main__":
    main()