"""urls

Revision ID: 0b6a01b03d4d
Revises: 5ba8c7af7acc
Create Date: 2021-11-16 17:10:14.702803

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import adsputils

# revision identifiers, used by Alembic.
revision = '0b6a01b03d4d'
down_revision = '5ba8c7af7acc'
branch_labels = None
depends_on = None

def upgrade():
    #Add EMITTABLE to status ENUM types
    op.execute("ALTER TYPE target_status_type ADD VALUE 'EMITTABLE'")
    op.execute("ALTER TYPE citation_status_type ADD VALUE 'EMITTABLE'")

def downgrade():
    #Move expanded status types to old
    op.execute("ALTER TYPE target_status_type RENAME TO  target_status_type_old")
    op.execute("CREATE TYPE  target_status_type AS ENUM('REGISTERED', 'DELETED','UPDATED')")
    #instantiate original status types
    op.execute("ALTER TYPE citation_status_type RENAME TO  citation_status_type_old")
    op.execute("CREATE TYPE  citation_status_type AS ENUM('REGISTERED', 'DELETED','UPDATED')")
    
    #DROP old ENUM types
    op.execute("DROP TYPE target_status_type_old")
    op.execute("DROP TYPE citation_status_type_old")
    
    #DROP expanded status columns
    op.drop_column('citation_target_version','status')
    op.drop_column('citation_target','status')    
    op.drop_column('citation_version','status')
    op.drop_column('citation','status')
    
    #ADD original status columns
    op.add_column('citation_target',Column('status', postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'), nullable=True))
    op.add_column('citation_target_version',Column('status', postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'), nullable=True))
    op.add_column('citation_version',Column('status', postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='citation_status_type'), nullable=True))
    op.add_column('citation',Column('status', postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='citation_status_type'), nullable=True))

