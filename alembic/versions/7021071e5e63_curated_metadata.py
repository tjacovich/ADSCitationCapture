"""curated_metadata

Revision ID: 7021071e5e63
Revises: 0b6a01b03d4d
Create Date: 2022-01-19 13:59:50.640443

"""
from alembic import op
import os
from ADSCitationCapture import models, db
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import adsputils


# revision identifiers, used by Alembic.
revision = '7021071e5e63'
down_revision = '0b6a01b03d4d'
branch_labels = None
depends_on = None

def upgrade():
    session = sa.orm.Session(bind=op.get_bind())
    op.add_column('citation_target',sa.Column('curated_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('citation_target_version',sa.Column('curated_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('citation_target',sa.Column('bibcode', sa.Text(), nullable=True))
    op.add_column('citation_target_version',sa.Column('bibcode', sa.Text(), nullable=True))
    
    pgrsql_populate_bibcode_column ="UPDATE public.citation_target \
	SET bibcode = parsed_cited_metadata->>'bibcode' \
	WHERE parsed_cited_metadata->>'bibcode' is not NULL"
    op.execute(pgrsql_populate_bibcode_column)

def downgrade():
    op.drop_column('citation_target','curated_metadata')
    op.drop_column('citation_target_version','curated_metadata')
    op.drop_column('citation_target','bibcode')
    op.drop_column('citation_target_version','bibcode')