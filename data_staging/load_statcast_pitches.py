import os
import pandas as pd
from sqlalchemy import create_engine

from utils.utils import build_db_url
from data_quality.statcast_specs import COMMON_PITCH_COLUMNS, STATCAST_PITCHES_SPEC, STATCAST_BATTED_BALLS_SPEC
from data_staging.transform_load_table import transform_and_load

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PARQUET_PATH = os.path.join(BASE_DIR, 'data', 'statcast_pitching_lad_2025-03-18_2025-11-01_f008ac3a-0f27-4843-b345-95059ed956bf.parquet')

def main(table_spec, table_name: str, constraint: str):
    engine = create_engine(build_db_url())
    df_raw = pd.read_parquet(PARQUET_PATH)

    n, report = transform_and_load(
        engine,
        df_raw,
        spec=table_spec,
        schema='staging',
        table=table_name,
        constraint=constraint
    )

    print(report)

if __name__ == "__main__":
    main(table_spec=STATCAST_BATTED_BALLS_SPEC, table_name='statcast_batted_balls', constraint = 'statcast_batted_balls_pkey')