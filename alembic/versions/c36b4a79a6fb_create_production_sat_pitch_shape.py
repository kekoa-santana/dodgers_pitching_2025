"""create production.sat_pitch_shape

Revision ID: c36b4a79a6fb
Revises: c18f3d49656f
Create Date: 2026-01-12 21:23:30.559321

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c36b4a79a6fb'
down_revision: Union[str, Sequence[str], None] = 'c18f3d49656f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    """Upgrade schema."""
    op.create_table(
        "sat_pitch_shape",
        sa.Column("pitch_id", sa.BigInteger(), primary_key=True, nullable=False),

        sa.Column('release_pos_x', sa.Float()),
        sa.Column('release_pos_y', sa.Float()),
        sa.Column('release_pos_z', sa.Float()),
        sa.Column('release_spin_rate', sa.Float()),
        sa.Column('release_extension', sa.Float()),
        sa.Column('release_speed', sa.Float()),
        sa.Column('spin_axis', sa.Float()),
        sa.Column('pfx_x', sa.Float()),
        sa.Column('pfx_z', sa.Float()),
        sa.Column('vx0', sa.Float()),
        sa.Column('vy0', sa.Float()),
        sa.Column('vz0', sa.Float()),
        sa.Column('ax', sa.Float()),
        sa.Column('ay', sa.Float()),
        sa.Column('az', sa.Float()),
        sa.Column('plate_x', sa.Float()),
        sa.Column('plate_z', sa.Float()),

        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),

        sa.ForeignKeyConstraint(
            ['pitch_id'],
            ['production.fact_pitch.pitch_id'],
            name='sat_pitch_shape_pitch_id_fkey',
            ondelete="CASCADE"
        ),

        schema = "production",
    )
    
    op.create_index(
        "idx_sat_pitch_shape_spin",
        "sat_pitch_shape",
        ['release_spin_rate'],
        schema='production'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('idx_sat_pitch_shape_spin', table_name='sat_pitch_shape', schema='production')
    op.drop_table('sat_pitch_shape', schema='production')
