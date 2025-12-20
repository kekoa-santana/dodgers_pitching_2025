import pandas as pd
import os
from data_staging.statcast_utils import (
    map_pitch_result, is_bip, is_whiff, is_called_strike, 
    is_ball, clean_plate_location
)

# -----------------------------
#    LOAD PARQUET FILE
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PARQUET_PATH = os.path.join(BASE_DIR, 'data', 'statcast_pitching_lad_2025-03-18_2025-11-01_f008ac3a-0f27-4843-b345-95059ed956bf.parquet')

df = pd.read_parquet(PARQUET_PATH)

# ----------------------------
#   SELECT PROPER COLUMNS
#   CAST TO TYPES
# ----------------------------
def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    parquet_cols = [
        'game_pk', 'pitcher', 'batter', 'pitch_number', 'pitch_type', 'at_bat_number',
        'events', 'description', 'release_speed', 'release_pos_x', 'release_pos_y', 
        'release_pos_z', 'release_spin_rate', 'release_extension', 'spin_axis', 'effective_speed',
        'pfx_x', 'pfx_z', 'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az', 'zone', 'plate_x', 'plate_z',
        'sz_top', 'sz_bot', 'p_throws', 'stand', 'balls', 'strikes', 'inning', 'on_3b', 'on_2b', 'on_1b',
        'outs_when_up', 'home_score', 'away_score', 'bat_score', 'fld_score', 'home_score_diff', 'bat_score_diff',
        'if_fielding_alignments', 'of_fielding_alignments', 'api_break_z_with_gravity', 'api_break_x_arm', 
        'api_break_x_batter_in', 'arm_angle', 'attack_angle', 'atack_direction', 'swing_path_tilt'
    ]

    df = df[parquet_cols].copy()
    print(df.head())

# -----------------------------
#    CREATE DERIVED FIELDS
# -----------------------------
def create_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    # Use helpers from statcast_utils.py
    df['pitch_result_type'] = df['description'].apply(map_pitch_result)
    df['is_bip'] = df['description'].apply(is_bip)
    df['is_whiff'] = df['description'].apply(is_whiff)
    df['is_called_strike'] = df['description'].apply(is_called_strike)
    df['is_ball'] = df['description'].apply(is_ball)

# -----------------------------
#    CLEAN FIELDS
# -----------------------------
# TODO: clean all other columns, not just plate_x and plate_z
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.apply(clean_plate_location)
    return df

# REMOVE IMPOSSIBLE PITCH ROWS (NOT COMMON)

# WRITE TO POSTGRES

def main():
    create_derived_fields(df)
    print(df[['description', 'pitch_result_type']].head())

if __name__ == "__main__":
    main()