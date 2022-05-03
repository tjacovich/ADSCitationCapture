"""lowercase_alt_bibcodes_fix

Revision ID: fae6c4a0716e
Revises: 7021071e5e63
Create Date: 2022-05-02 15:44:20.630755

"""
from alembic import op
import sqlalchemy as sa
from ADSCitationCapture import db

# revision identifiers, used by Alembic.
revision = 'fae6c4a0716e'
down_revision = '7021071e5e63'
branch_labels = None
depends_on = None

def correct_alternate_bibcodes(main_session, curated = False):
    """
    Pulls all citation targets from DB and corrects any lowercase final letters in the alternate bibcodes.
    """
    records = db._get_citation_targets_session(main_session, only_status = None)
    for record in records:
        bibcode = record.get('bibcode', None)
        content = record.get('content', None)
        citation_in_db = False
        metadata = {}
        metadata = db._get_citation_target_metadata_session(main_session, content, citation_in_db, metadata, curate=curated)
        if metadata:
            raw_metadata = metadata.get('raw', {})
            parsed_metadata = metadata.get('parsed', {})
            curated_metadata = metadata.get('curated',{})
            status = metadata.get('status', None)
            _update_citation_target_alt_bibcodes_alembic(main_session, content, raw_metadata, parsed_metadata, curated_metadata, status=status, bibcode=bibcode)

def _update_citation_target_alt_bibcodes_alembic(session, content, raw_metadata, parsed_metadata, curated_metadata={}, status=None, bibcode=None):
    """
    Correct alternate bibcode format for a citation target when we do not need to
    close the session after completion
    """
    metadata_updated = False
    if not bibcode:
        msg = "bibcode should not be None. Please check entry for {}. Skipping.".format(content)
        return metadata_updated
    alt_bibcodes = parsed_metadata.get('alternate_bibcode', [])
    if alt_bibcodes:
        #Make sure alt bibcodes have a capital final letter, and remove current bibcode form alt bibcode list
        alt_bibcodes = [bib[:-1]+bib[-1].upper() for bib in alt_bibcodes if bib[:-1]+bib[-1].upper() != bibcode]
        #Remove any duplicates
        alt_bibcodes = list(set(alt_bibcodes))
        parsed_metadata['alternate_bibcode'] = alt_bibcodes
        metadata_updated = db._update_citation_target_metadata_session(session, content, raw_metadata, parsed_metadata, curated_metadata, status, bibcode)    
    return metadata_updated

def upgrade():
    session = sa.orm.Session(bind=op.get_bind())    
    correct_alternate_bibcodes(session, curated = False)

def downgrade():
   pass
