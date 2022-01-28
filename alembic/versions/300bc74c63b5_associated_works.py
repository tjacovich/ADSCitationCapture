"""associated works

Revision ID: 300bc74c63b5
Revises: 5ba8c7af7acc
Create Date: 2021-12-23 14:39:51.018091

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '300bc74c63b5'
down_revision = '5ba8c7af7acc'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('citation_target',sa.Column('associated_works', postgresql.JSONB()))
    op.add_column('citation_target_version',sa.Column('associated_works', postgresql.JSONB()))



def downgrade():
    op.drop_column('citation_target','associated_works')
    op.drop_column('citation_target_version','associated_works')
