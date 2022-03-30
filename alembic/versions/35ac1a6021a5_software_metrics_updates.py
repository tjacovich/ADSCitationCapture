"""software_metrics_updates

Revision ID: 35ac1a6021a5
Revises: 7021071e5e63
Create Date: 2022-03-30 09:25:44.211054

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '35ac1a6021a5'
down_revision = '7021071e5e63'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('reader_changes',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('new_bibcode', sa.String(), nullable=True),
    sa.Column('new_reader', sa.Text(), nullable=True),
    sa.Column('previous_bibcode', sa.String(), nullable=True),
    sa.Column('previous_reader', sa.Text(), nullable=True),
    sa.Column('status', postgresql.ENUM('NEW', 'DELETED', name='reader_change_type'), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='public'
    )
    op.create_table('readers',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('bibcode', sa.String(), nullable=True),
    sa.Column('reader', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='public'
    )
    op.drop_constraint('citation_content_fkey', 'citation', type_='foreignkey')
    op.create_foreign_key(None, 'citation', 'citation_target', ['content'], ['content'], source_schema='public', referent_schema='public')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'citation', schema='public', type_='foreignkey')
    op.create_foreign_key('citation_content_fkey', 'citation', 'citation_target', ['content'], ['content'])
    op.drop_table('reader_data', schema='public')
    op.drop_table('reader_changes', schema='public')
    # ### end Alembic commands ###
