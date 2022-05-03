"""alt_bibcodes_fix_and_doi_sanitization

Revision ID: 02d38ac44872
Revises: 8e83568feb1d, fae6c4a0716e
Create Date: 2022-05-03 10:50:10.250392

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '02d38ac44872'
down_revision = ('8e83568feb1d', 'fae6c4a0716e')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
