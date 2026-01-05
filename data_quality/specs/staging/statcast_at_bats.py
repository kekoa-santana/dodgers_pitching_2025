from data_quality.specs.spec_engine import ColumnSpec, TableSpec

STATCAST_AT_BATS_ONLY: dict[str, ColumnSpec] = {

}

STATCAST_AT_BATS_SPEC = TableSpec(
    'statcast_at_bats',
    pk = ['game_pk', 'game_counter'],
    columns=STATCAST_AT_BATS_ONLY
)