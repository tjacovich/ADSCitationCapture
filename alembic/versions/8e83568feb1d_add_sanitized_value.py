"""add_sanitized_value

Revision ID: 8e83568feb1d
Revises: 7021071e5e63
Create Date: 2022-04-14 09:24:42.277371

"""
from alembic import op
import sqlalchemy as sa
from ADSCitationCapture import db
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '8e83568feb1d'
down_revision = '7021071e5e63'
branch_labels = None
depends_on = None


def upgrade():
    connection = None
    if not op.get_context().as_sql:
        """
        ALTERING ENUM values cannot be done from transaction blocks. 
        Changing the isolation_level to autocommit puts the specific calls in individually, preventing the issue.
        Alembic will warn you about changing isolation level because it has already opened a transaction which is committed.
        """
        connection = op.get_bind()
        connection.execution_options(isolation_level='AUTOCOMMIT')
    
    op.execute("ALTER TYPE target_status_type ADD VALUE 'SANITIZED'")
    op.execute("ALTER TYPE citation_status_type ADD VALUE 'SANITIZED'")
    op.add_column('citation', sa.Column('raw_content', sa.Text(), nullable=True))
    op.add_column('citation_version', sa.Column('raw_content', sa.Text(), nullable=True))

def downgrade():
#Move expanded status types to old
    op.execute("ALTER TYPE target_status_type RENAME TO  target_status_type_old")
    op.execute("CREATE TYPE  target_status_type AS ENUM('REGISTERED', 'DELETED', 'DISCARDED', 'UPDATED', 'EMITTABLE')")
    
    #instantiate original status types
    op.execute("ALTER TYPE citation_status_type RENAME TO  citation_status_type_old")
    op.execute("CREATE TYPE  citation_status_type AS ENUM('REGISTERED', 'DELETED', 'DISCARDED', 'UPDATED', 'EMITTABLE')")
    
    def pgsql_change_type(table_name, column_name, new_enum):
        return  f"ALTER TABLE {table_name} \
                ALTER COLUMN {column_name} \
                SET DATA TYPE {new_enum} \
                USING ( \
                    CASE {column_name}::text \
                        WHEN 'SANITIZED' THEN 'DISCARDED' \
                        ELSE {column_name}::text \
                    END \
                )::{new_enum}"

    #Reset to original ENUM type
    op.execute(pgsql_change_type('citation_target', 'status', 'target_status_type'))
    op.execute(pgsql_change_type('citation_target_version', 'status', 'target_status_type'))
    op.execute(pgsql_change_type('citation', 'status', 'citation_status_type'))
    op.execute(pgsql_change_type('citation_version', 'status', 'citation_status_type'))
    op.drop_column('citation', sa.Column('raw_content', sa.Text(), nullable=True))
    op.drop_column('citation_version', sa.Column('raw_content', sa.Text(), nullable=True))


    #DROP old (SANITIZED) ENUM types
    op.execute("DROP TYPE target_status_type_old")
    op.execute("DROP TYPE citation_status_type_old")
    