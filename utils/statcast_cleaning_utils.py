import pandas as pd

# ---------------------
#   CLEANING HELPERS
# ---------------------

# Remove NAN Pitch Type rows
def remove_nan_pitch(df: pd.DataFrame) -> pd.DataFrame:
    nan_mask = ~df['pitch_type'].isna()
    df = df[nan_mask].copy()
    return df

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

# Clean release physics
def clean_release_physics(df: pd.DataFrame) -> pd.DataFrame:
    if "release_speed" in df.columns:
        df.loc[(df["release_speed"] < 0) | (df["release_speed"] > 108), "release_speed"] = np.nan
    if "release_spin_rate" in df.columns:
        df.loc[(df["release_spin_rate"] < 500) | (df["release_spin_rate"] > 3500), "release_spin_rate"] = np.nan
    if "release_extension" in df.columns:
        df.loc[(df["release_extension"] < 3.5) | (df["release_extension"] > 9), "release_extension"] = np.nan

    return df

# Clean release location
def clean_release_location(df: pd.DataFrame) -> pd.DataFrame:
    if ("release_pos_x" in df.columns and "release_pos_y" in df.columns and "release_pos_z" in df.columns):
        df['release_location_outlier'] = (
            df['release_pos_x'].abs() > 6.5 |
            df['release_pos_y'] < 46 |
            df['release_pos_y'] > 60 |
            df['release_pos_z'] < 2.5 |
            df['release_pos_z'] > 9
        )
        
        for col in ['release_pos_x', 'release_pos_y', 'release_pos_z']:
            df.loc[df['release_location_outlier'], col] = np.nan

    return df

# Clean Spin Axis
def clean_spin_axis(df: pd.DataFrame) -> pd.DataFrame:
    if "spin_axis" in df.columns:
        df.loc[(df["spin_axis"] < 0) | (df["spin_axis"] > 360), "spin_axis"] = np.nan
    return df

# Clean effective speed
def clean_effective_speed(df: pd.DataFrame) -> pd.DataFrame:
    if "effective_speed" in df.columns and "release_speed" in df.columns:
        speed_delta = df["effective_speed"] - df["release_speed"]

        df.loc[(df["effective_speed"] < 60) | (df["effective_speed"] > 111), "effective_speed"] = np.nan
        df.loc[speed_delta.abs() > 6, "effective_speed"] = np.nan

    return df