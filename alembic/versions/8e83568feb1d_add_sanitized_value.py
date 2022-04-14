"""add_sanitized_value

Revision ID: 8e83568feb1d
Revises: 7021071e5e63
Create Date: 2022-04-14 09:24:42.277371

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '8e83568feb1d'
down_revision = '7021071e5e63'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('COMMIT')
    op.execute("ALTER TYPE target_status_type ADD VALUE 'SANITIZED'")
    op.execute("ALTER TYPE citation_status_type ADD VALUE 'SANITIZED'")


def downgrade():
#Move expanded status types to old
    op.execute("ALTER TYPE target_status_type RENAME TO  target_status_type_old")
    op.execute("CREATE TYPE  target_status_type AS ENUM('REGISTERED', 'DELETED', 'UPDATED', 'EMITTABLE')")
    
    #instantiate original status types
    op.execute("ALTER TYPE citation_status_type RENAME TO  citation_status_type_old")
    op.execute("CREATE TYPE  citation_status_type AS ENUM('REGISTERED', 'DELETED', 'UPDATED', 'EMITTABLE')")
    
    #DROP expanded status columns
    op.drop_column('citation_target_version','status')
    op.drop_column('citation_target','status')    
    op.drop_column('citation_version','status')
    op.drop_column('citation','status')
    
    #DROP old ENUM types
    op.execute("DROP TYPE target_status_type_old")
    op.execute("DROP TYPE citation_status_type_old")
    
    #ADD original status columns
    op.add_column('citation_target',sa.Column('status', postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', 'EMITTABLE', name='target_status_type'), nullable=True))
    op.add_column('citation_target_version',sa.Column('status', postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', 'EMITTABLE', name='target_status_type'), nullable=True))
    op.add_column('citation_version',sa.Column('status', postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', 'EMITTABLE', name='citation_status_type'), nullable=True))
    op.add_column('citation',sa.Column('status', postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', 'EMITTABLE', name='citation_status_type'), nullable=True))


