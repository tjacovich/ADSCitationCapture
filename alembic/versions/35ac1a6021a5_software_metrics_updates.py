"""software_metrics_updates

Revision ID: 35ac1a6021a5
Revises: 7021071e5e63
Create Date: 2022-03-30 09:25:44.211054

"""
from alembic import op
import sqlalchemy as sa
import adsputils
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '35ac1a6021a5'
down_revision = '7021071e5e63'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('reader_changes',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('new_bibcode', sa.String(), nullable=True),
    sa.Column('new_reader', sa.Text(), nullable=True),
    sa.Column('previous_bibcode', sa.String(), nullable=True),
    sa.Column('previous_reader', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='public'
    )
    op.create_table('readers',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('bibcode', sa.String(), nullable=True),
    sa.Column('reader', sa.Text(), nullable=True),
     sa.Column('timestamp', adsputils.UTCDateTime(timezone=True), nullable=True),
    sa.Column('status', postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='reader_status_type'), nullable=True),
    sa.Column('created', adsputils.UTCDateTime(timezone=True), nullable=True),
    sa.Column('updated', adsputils.UTCDateTime(timezone=True), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='public'
    )



def downgrade():
    op.drop_table('readers', schema='public')
    op.drop_table('reader_changes', schema='public')
    op.execute("DROP TYPE reader_status_type")

