"""creating dim_teams

Revision ID: 97a387a26dc7
Revises: d2f48c974c30
Create Date: 2026-01-13 17:24:48.845929

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '97a387a26dc7'
down_revision: Union[str, Sequence[str], None] = 'd2f48c974c30'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "dim_team",
        sa.Column('team_id', sa.SmallInteger, primary_key = True),
        sa.Column('team_name', sa.Text, nullable=False),
        sa.Column('full_name', sa.Text, nullable=False),
        sa.Column('abbreviation', sa.String(4), nullable=False),
        sa.Column('venue', sa.Text),
        sa.Column('division', sa.String(35), nullable=False),
        sa.Column('division_id', sa.SmallInteger),
        sa.Column('location', sa.Text),
        schema='production'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('dim_team', schema='production')