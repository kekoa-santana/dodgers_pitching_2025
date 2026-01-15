"""update column references for new dim_player

Revision ID: 6826a82834d1
Revises: c220adb8ac66
Create Date: 2026-01-14 12:30:39.092347

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6826a82834d1'
down_revision: Union[str, Sequence[str], None] = 'c220adb8ac66'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = 'production'


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(
        constraint_name='fact_pa_pitcher_id_fkey',
        table_name='fact_pa',
        schema=SCHEMA,
        type_='foreignkey'
    )

    op.create_foreign_key(
        constraint_name='fact_pa_pitcher_id_fkey',
        source_table='fact_pa',
        referent_table='dim_player',
        local_cols=['pitcher_id'], 
        remote_cols=['player_id'],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete=None
    )

    op.drop_constraint(
        'fact_pa_batter_id_fkey',
        'fact_pa',
        schema=SCHEMA,
        type_='foreignkey'
    )

    op.create_foreign_key(
        constraint_name='fact_pa_batter_id_fkey',
        source_table='fact_pa', 
        referent_table='dim_player',
        local_cols=['pitcher_id'],
        remote_cols=['player_id'],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
    )

    op.drop_constraint(
        'fact_pitch_batter_id_fkey',
        'fact_pitch',
        schema=SCHEMA,
        type_='foreignkey'
    )

    op.create_foreign_key(
        constraint_name='fact_pitch_batter_id_fkey',
        source_table='fact_pitch',
        referent_table='dim_player',
        local_cols=['batter_id'],
        remote_cols=['player_id'],
        source_schema=SCHEMA,
        referent_schema=SCHEMA
    )

    op.drop_constraint(
        'fact_pitch_pitcher_id_fkey',
        'fact_pitch',
        schema=SCHEMA,
        type_='foreignkey'
    )

    op.create_foreign_key(
        'fact_pitch_pitcher_id_fkey',
        source_table='fact_pitch',
        referent_table='dim_player',
        local_cols=['pitcher_id'],
        remote_cols=['player_id'],
        source_schema=SCHEMA,
        referent_schema=SCHEMA
    )


def downgrade() -> None:
    """Downgrade schema."""
    for table, constraint, col in [
        ('fact_pa', 'fact_pa_pitcher_id_fkey', 'pitcher_id'),
        ('fact_pa', 'fact_pa_batter_id_fkey', 'batter_id'),
        ('fact_pitch', 'fact_pitch_pitcher_id_fkey', 'pitcher_id'),
        ('fact_pitch', 'fact_pitch_batter_id_fkey', 'batter_id')
    ]:
        op.drop_constraint(constraint, table, schema=SCHEMA, type_='foreignkey')
        op.create_foreign_key(
            constraint,
            table,
            'dim_player_old',
            [col],
            ['player_id'],
            source_schema=SCHEMA,
            referent_schema=SCHEMA
        )
