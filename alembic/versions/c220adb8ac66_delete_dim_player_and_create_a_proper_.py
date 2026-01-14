"""delete dim_player and create a proper dim_player

Revision ID: c220adb8ac66
Revises: 97a387a26dc7
Create Date: 2026-01-14 10:03:42.660898

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c220adb8ac66'
down_revision: Union[str, Sequence[str], None] = '97a387a26dc7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.rename_table('dim_player', 'dim_player_old', schema='production')
    op.create_table(
        'dim_player',
        sa.Column('player_id', sa.BigInteger, primary_key=True, nullable=False),
        sa.Column('full_name', sa.Text, nullable=False),
        sa.Column('team_id', sa.SmallInteger, nullable=False),
        sa.Column('first_name', sa.Text),
        sa.Column('last_name', sa.Text),
        sa.Column('birth_date', sa.DATE),
        sa.Column('age', sa.SmallInteger),
        sa.Column('height', sa.Text),
        sa.Column('weight', sa.SmallInteger),
        sa.Column('active', sa.Boolean),
        sa.Column('primary_position_code', sa.SmallInteger),
        sa.Column('primary_position', sa.String(4)),
        sa.Column('draft_year', sa.SmallInteger),
        sa.Column('mlb_debut_date', sa.DATE),
        sa.Column('bat_side', sa.String(1)),
        sa.Column('pitch_hand', sa.String(1)),
        sa.Column('sz_top', sa.REAL),
        sa.Column('sz_bot', sa.REAL),
        schema='production'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('dim_player', schema='production')
    op.rename_table('dim_player_old', 'dim_player', schema='production')