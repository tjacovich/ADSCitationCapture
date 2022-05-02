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
    op.execute('COMMIT')
    op.execute("ALTER TYPE target_status_type ADD VALUE 'EMITTABLE'")
    op.execute("ALTER TYPE citation_status_type ADD VALUE 'EMITTABLE'")

def downgrade():
    #Move expanded status types to old
    op.execute("ALTER TYPE target_status_type RENAME TO  target_status_type_old")
    op.execute("CREATE TYPE  target_status_type AS ENUM('REGISTERED','DISCARDED','DELETED','UPDATED')")
    
    #instantiate original status types
    op.execute("ALTER TYPE citation_status_type RENAME TO  citation_status_type_old")
    op.execute("CREATE TYPE  citation_status_type AS ENUM('REGISTERED','DISCARDED','DELETED','UPDATED')")
    

    def pgsql_change_type(table_name, column_name, new_enum):
        return  f"ALTER TABLE {table_name} \
                ALTER COLUMN {column_name} \
                SET DATA TYPE {new_enum} \
                USING ( \
                    CASE {column_name}::text \
                        WHEN 'EMITTABLE' THEN 'NULL' \
                        ELSE {column_name}::text \
                    END \
                )::{new_enum}"
  
    #ALTER column types to original ENUM type.
    op.execute(pgsql_change_type('citation_target', 'status', 'target_status_type'))
    op.execute(pgsql_change_type('citation_target_version', 'status', 'target_status_type'))
    op.execute(pgsql_change_type('citation', 'status', 'citation_status_type'))
    op.execute(pgsql_change_type('citation_version', 'status', 'citation_status_type'))

    
    #DROP old ENUM types
    op.execute("DROP TYPE target_status_type_old")
    op.execute("DROP TYPE citation_status_type_old")
 