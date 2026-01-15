"""drop dim_player_old and add stand on pitch

Revision ID: 9c3b97382f9f
Revises: 6826a82834d1
Create Date: 2026-01-14 14:53:53.002228

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9c3b97382f9f'
down_revision: Union[str, Sequence[str], None] = '6826a82834d1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_table(
        'dim_player_old',
        schema='production'
    )
    op.add_column(
        'fact_pitch',
        sa.Column('batter_stand', sa.String(1)),
        schema='production'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.create_table(
        'dim_player_old',
        sa.Column('player_id', sa.Integer, primary_key=True, nullable=False),
        sa.Column('player_name', sa.Text),
        sa.Column('p_throws', sa.String(1)),
        sa.Column('stand', sa.String(1)),
        schema='production'
    )
