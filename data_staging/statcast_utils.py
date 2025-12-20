import pandas as pd
import numpy as np

# ----------------------------
#    MAP PITCH RESULT
# ----------------------------

def map_pitch_result(description: str) -> str:
    if pd.isna(description):
        return None

    desc = description.lower()

    if desc in ('swinging_strike', 'swinging_strike_blocked', 'foul_tip'):
        return "whiff"
    if desc == 'called_strike':
        return "called_strike"
    if desc == 'automatic_strike':
        return "automatic_strike"
    if desc in ('ball', 'blocked_ball', 'automatic_ball'):
        return "ball"
    if desc == "hit_into_play":
        return "in_play"
    if desc == 'foul':
        return 'foul'
    if desc == 'hit_by_pitch':
        return 'hit_by_pitch'
    if desc in ('bunt_foul_tip', 'foul_bunt', 'missed_bunt'):
        return 'bunt_strike'
    return 'others'

# ---------------------
#   FLAG HELPERS
# ---------------------

# BIP detection
def is_bip(description: str) -> bool:
    return isinstance(description, str) and description.lower() == 'hit_into_play'

# whiff detection
def is_whiff(description:str) -> bool:
    return isinstance(description, str) and description.lower() in ('swinging_strike', 'swinging_strike_blocked', 'foul_tip')

# called strike detection
def is_called_strike(description: str) -> bool:
    return isinstance(description, str) and description.lower() == 'called_strike'

# ball detection
def is_ball(description: str) -> bool:
    return isinstance(description, str) and description.lower() in ('ball', 'blocked_ball', 'automatic_ball')

# ---------------------
#   CLEANING HELPERS
# ---------------------

# plate_x/z cleaning
def clean_plate_location(df: pd.DataFrame) -> pd.DataFrame:
    if "plate_x" in df.columns:
        df.loc[(df["plate_x"] < -3) | (df["plate_x"] > 3), "plate_x"] = np.nan
    
    if "plate_z" in df.columns:
        df.loc[(df["plate_z"] < 0) | (df["plate_z"] > 7), "plate_z"] = np.nan

    return df

# EV/LA cleaning
def clean_ev_la(df: pd.DataFrame) -> pd.DataFrame:
    if "launch_speed" in df.columns:
        df.loc[(df["launch_speed"] < 0) | (df["launch_speed"] > 130), "launch_speed"] = np.nan
    if "launch_angle" in df.columns:
        df.loc[(df["launch_angle"] < -90) | (df["launch_angle"] > 90), "launch_angle"] = np.nan
    return df

# Remove NAN Pitch Type rows
def remove_nan_pitch(df: pd.DataFrame) -> pd.DataFrame:
    nan_mask = ~df['pitch_type'].isna()
    df = df[nan_mask].copy()
    return df

# Clean Spin Axis & Spin Rate



# ---------------------
#    PK ASSURANCE
# ---------------------
def assert_pk_unique(
    df: pd.DataFrame,
    pk_cols: list[str]
    ) -> pd.DataFrame:
    # Find PK duplicates and drop them

    missing = [c for c in pk_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing PK columns: {missing}")

    dup_mask = df.duplicated(subset=pk_cols, keep=False)
    if dup_mask.any():
        n = int(dup_mask.sum())
        sample = df.loc[dup_mask, pk_cols].head(25)

        print(f"[DQ] Dropping {n} duplicate rows based on PK {pk_cols}. Sample keys:\n{sample}")
        sort_cols = [col for col in ['game_pk', 'at_bat_number', 'pitch_number', 'game_date'] if col in df.columns]
        df = df.sort_values(by = sort_cols)
        
        df = df.drop_duplicates(subset=pk_cols, keep="last").copy()

    return df
# ---------------------
#   DATETIME PARSING
# ---------------------
def parse_game_date(df: pd.DataFrame, col: str = "game_date") -> pd.DataFrame:
    if col not in df.columns:
        return df
    
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

# ---------------------
#   TYPE NORMALIZATION
# ---------------------
# ONLY ACCOUNTS FOR COLUMNS THAT ARE IN THE INITIAL 3 TABLES
def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    # Nullable ints (IDs)
    int_like_cols = [
        'game_pk', 'pitcher_id', 'batter_id', 'pitcher', 'batter', 'on_3b', 'on_2b', 'on_1b'
        ]
    for col in int_like_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Small ints (counts/context)
    small_int_cols = [
        'pitch_number', 'at_bat_number', 'zone', 'balls', 'strikes', 'inning',
        'outs_when_up', 'home_score', 'away_score', 'bat_score', 'fld_score', 
        'home_score_diff', 'bat_score_diff', 'hit_location'
    ]
    for col in small_int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors= "coerce").astype("Int64")

    # Nullable booleans
    bool_cols = [
        'is_whiff', 'is_bip', 'is_called_strike', 'is_ball', 'is_strikeout', 'is_walk'
    ]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    # String cols
    string_cols = [
        'pitch_type', 'pitch_name', 'events', 'description', 'pitch_result_type',
        'p_throws', 'stand', 'if_fielding_alignment', 'of_fielding_alignment',
        'bb_type'
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype('string')

    # float cols
    float_cols = [
        'release_speed', 'release_pos_x', 'release_pos_y', 'release_pos_z', 'release_spin_rate', 'release_extension',
        'spin_axis', 'effective_speed', 'pfx_x', 'pfx_y', 'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az', 'plate_x', 'plate_z', 
        'sz_top', 'sz_bot', 'api_break_z_with_gravity', 'api_break_x_arm', 'api_break_x_batter_in', 'arm_angle',
        'attack_angle', 'attack_direction', 'swing_path_tilt', 'launch_angle'
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    return df