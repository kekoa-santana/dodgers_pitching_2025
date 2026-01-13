"""add sz_top and sz_bot to production.sat_pitch_shape

Revision ID: d2f48c974c30
Revises: c36b4a79a6fb
Create Date: 2026-01-13 14:47:13.863507

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd2f48c974c30'
down_revision: Union[str, Sequence[str], None] = 'c36b4a79a6fb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "sat_pitch_shape",
        sa.Column("sz_top", sa.Float()),
        schema="production"
    )
    op.add_column(
        "sat_pitch_shape",
        sa.Column("sz_bot", sa.Float()),
        schema="production"
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("sat_pitch_shape", "sz_top", schema="production")
    op.drop_column("sat_pitch_shape", "sz_bot", schema="production")