"""merging manual curation and associated works alembic revisions

Revision ID: cd66a979846d
Revises: 7021071e5e63, b86c733447a5
Create Date: 2022-03-22 12:54:09.234007

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cd66a979846d'
down_revision = ('7021071e5e63', 'b86c733447a5')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
