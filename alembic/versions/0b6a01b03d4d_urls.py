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
    op.alter_columns('citation_target','status' type_=postgresql.ENUM('EMITTABLE','REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'),nullable=True)
    op.alter_columns('citation_target_version','status' type_=postgresql.ENUM('EMITTABLE','REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'),nullable=True)
    op.alter_columns('citation_version','status' type_=postgresql.ENUM('EMITTABLE','REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'),nullable=True)
    op.alter_columns('citation','status' type_=postgresql.ENUM('EMITTABLE','REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'),nullable=True)


def downgrade():
    op.alter_columns('citation_target','status' type_=postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'),nullable=True)
    op.alter_columns('citation_target_version','status' type_=postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'),nullable=True)
    op.alter_columns('citation_version','status' type_=postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'),nullable=True)
    op.alter_columns('citation','status' type_=postgresql.ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type'),nullable=True)   
