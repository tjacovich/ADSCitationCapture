import os
from psycopg2 import IntegrityError
from dateutil.tz import tzutc
from ADSCitationCapture.models import Citation, CitationTarget, Event
from adsmsg import CitationChange
from adsputils import setup_logging

# ============================= INITIALIZATION ==================================== #
# - Use app logger:
#import logging
#logger = logging.getLogger('ads-citation-capture')
# - Or individual logger for this file:
from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
logger = setup_logging(__name__, proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))


# =============================== FUNCTIONS ======================================= #
def store_event(app, data):
    """
    Stores a new event in the DB
    """
    stored = False
    with app.session_scope() as session:
        event = Event()
        event.data = data
        session.add(event)
        try:
            session.commit()
        except:
            logger.exception("Problem storing event '%s'", str(event))
        else:
            stored = True
    return stored

def store_citation_target(app, citation_change, content_type, raw_metadata, parsed_metadata, status, associated=None):
    """
    Stores a new citation target in the DB
    """
    stored = False
    with app.session_scope() as session:
        citation_target = CitationTarget()
        citation_target.content = citation_change.content
        citation_target.content_type = content_type
        citation_target.raw_cited_metadata = raw_metadata
        citation_target.parsed_cited_metadata = parsed_metadata
        citation_target.curated_metadata = {}
        citation_target.status = status
        citation_target.bibcode = parsed_metadata.get("bibcode", None)
        citation_target.associated_works = associated
        session.add(citation_target)
        try:
            session.commit()
        except IntegrityError as e:
            # IntegrityError: (psycopg2.IntegrityError) duplicate key value violates unique constraint "citing_content_unique_constraint"
            logger.error("Ignoring new citation target (citing '%s', content '%s' and timestamp '%s') because it already exists in the database (another new citation may have been processed before this one): '%s'", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString(), str(e))
        else:
            logger.info("Stored new citation target (citing '%s', content '%s' and timestamp '%s')", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
            stored = True
    return stored

def _update_citation_target_metadata_session(session, content, raw_metadata, parsed_metadata, curated_metadata = {}, status=None, bibcode = None, associated = None):
    """
    Actual calls to database session for update_citation_target_metadata
    """
    citation_target = session.query(CitationTarget).filter(CitationTarget.content == content).first()
    if type(raw_metadata) is bytes:
        try:
            raw_metadata = raw_metadata.decode('utf-8')
        except UnicodeEncodeError:
            pass
    if citation_target.raw_cited_metadata != raw_metadata or citation_target.parsed_cited_metadata != parsed_metadata or \
            (status is not None and citation_target.status != status) or citation_target.curated_metadata != curated_metadata or \
            citation_target.bibcode != bibcode or citation_target.associated_works != associated:
        citation_target.raw_cited_metadata = raw_metadata
        citation_target.parsed_cited_metadata = parsed_metadata
        citation_target.curated_metadata = curated_metadata
        citation_target.bibcode = bibcode
        if(citation_target.associated_works != associated):
                logger.debug("associated works set for {} set from {} to {}".format(citation_target.content, citation_target.associated_works, associated))
                citation_target.associated_works = associated
        if status is not None:
            citation_target.status = status
        session.add(citation_target)
        session.commit()
        logger.info("Updated metadata for citation target '%s' (alternative bibcodes '%s')", content, ", ".join(parsed_metadata.get('alternate_bibcode', [])))
        metadata_updated = True
        return metadata_updated

def update_citation_target_metadata(app, content, raw_metadata, parsed_metadata, curated_metadata = {}, status=None, bibcode = None, associated = None):
    """
    Update metadata for a citation target
    """
    metadata_updated = False
    if not bibcode: bibcode = parsed_metadata.get('bibcode', None)
    with app.session_scope() as session:
        metadata_updated =  _update_citation_target_metadata_session(session, content, raw_metadata, parsed_metadata, curated_metadata, status=status, bibcode=bibcode, associated=associated)
    return metadata_updated


def store_citation(app, citation_change, content_type, raw_metadata, parsed_metadata, status):
    """
    Stores a new citation in the DB
    """
    stored = False
    with app.session_scope() as session:
        citation = Citation()
        citation.citing = citation_change.citing
        citation.cited = citation_change.cited
        citation.content = citation_change.content
        citation.resolved = citation_change.resolved
        citation.timestamp = citation_change.timestamp.ToDatetime().replace(tzinfo=tzutc())
        citation.status = status
        session.add(citation)
        try:
            session.commit()
        except IntegrityError as e:
            # IntegrityError: (psycopg2.IntegrityError) duplicate key value violates unique constraint "citing_content_unique_constraint"
            logger.error("Ignoring new citation (citing '%s', content '%s' and timestamp '%s') because it already exists in the database when it is not supposed to (race condition?): '%s'", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString(), str(e))
        else:
            logger.info("Stored new citation (citing '%s', content '%s' and timestamp '%s')", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
            stored = True
    return stored

def get_citation_target_count(app):
    """
    Return the number of citation targets registered in the database
    """
    citation_target_count = 0
    with app.session_scope() as session:
        citation_target_count = session.query(CitationTarget).count()
    return citation_target_count

def get_citation_count(app):
    """
    Return the number of citations registered in the database
    """
    citation_count = 0
    with app.session_scope() as session:
        citation_count = session.query(Citation).count()
    return citation_count

def _extract_key_citation_target_data(records_db, disable_filter=False):
    """
    Convert list of CitationTarget to a list of dictionaries with key data
    """
    records = [
        {
            'bibcode': record_db.bibcode,
            'alternate_bibcode': record_db.parsed_cited_metadata.get('alternate_bibcode', []),
            'content': record_db.content,
            'content_type': record_db.content_type,
            'curated_metadata': record_db.curated_metadata if record_db.curated_metadata is not None else {},
            'associated_works': record_db.associated_works,
            'version': record_db.parsed_cited_metadata.get('version', '')
        }
        for record_db in records_db
        if disable_filter or record_db.parsed_cited_metadata.get('bibcode', None) is not None
    ]
    return records

def get_citation_targets_by_bibcode(app, bibcodes, only_status='REGISTERED'):
    """
    Return a list of dict with the requested citation targets based on their bibcode
    """
    with app.session_scope() as session:
        records_db = []
        for bibcode in bibcodes:
            if only_status:
                record_db = session.query(CitationTarget).filter(CitationTarget.parsed_cited_metadata["bibcode"].astext == bibcode).filter_by(status=only_status).first()
            else:
                record_db = session.query(CitationTarget).filter(CitationTarget.parsed_cited_metadata["bibcode"].astext == bibcode).first()
            if record_db:
                records_db.append(record_db)

        if only_status:
            disable_filter = only_status == 'DISCARDED'
        else:
            disable_filter = True
        records = _extract_key_citation_target_data(records_db, disable_filter=disable_filter)
    return records

def get_citation_targets_by_doi(app, dois, only_status='REGISTERED'):
    """
    Return a list of dict with the requested citation targets based on their DOI
    - Records without a bibcode in the database will not be returned
    """
    with app.session_scope() as session:
        if only_status:
            records_db = session.query(CitationTarget).filter(CitationTarget.content.in_(dois)).filter_by(status=only_status).all()
            disable_filter = only_status == 'DISCARDED'
        else:
            records_db = session.query(CitationTarget).filter(CitationTarget.content.in_(dois)).all()
            disable_filter = True

        records = _extract_key_citation_target_data(records_db, disable_filter=disable_filter)
    return records

def _get_citation_targets_session(session, only_status='REGISTERED'):
    """
    Actual calls to database session for get_citation_targets
    """
    if only_status:
        records_db = session.query(CitationTarget).filter_by(status=only_status).all()
        disable_filter = only_status in ['DISCARDED','EMITTABLE']
    else:
        records_db = session.query(CitationTarget).all()
        disable_filter = True
    records = _extract_key_citation_target_data(records_db, disable_filter=disable_filter)
    return records

def get_associated_works(app, all_versions_doi, only_status='REGISTERED'):
    
def get_associated_works_by_doi(app, all_versions_doi, only_status='REGISTERED'):
    dois = all_versions_doi['versions']
    root_doi = all_versions_doi['all_doi'].lower()
    try:
        versions = {records['version']:records['bibcode'] for records in get_citation_targets_by_doi(app, dois, only_status)}
        root_ver = get_citation_targets_by_doi(app, [root_doi], only_status)
        if root_ver != []:
            root_record = {'Concept Record':root_ver[0]['bibcode']}
            versions.update(root_record)
        return versions
    except:
        logger.info('No associated works for {} in database', dois[0])
        return [None]
        
def get_citation_targets(app, only_status='REGISTERED'):
    """
    Return a list of dict with all citation targets (or only the registered ones)
    - Records without a bibcode in the database will not be returned
    """
    with app.session_scope() as session:
        records = _get_citation_targets_session(session, only_status)
    return records

def _get_citation_target_metadata_session(session, doi, citation_in_db, metadata, curate=True):
    """
    Actual calls to database session for get_citation_target_metadata
    """
    citation_target = session.query(CitationTarget).filter_by(content=doi).first()
    citation_target_in_db = citation_target is not None
    if citation_target_in_db:
        metadata['raw'] = citation_target.raw_cited_metadata
        metadata['curated'] = citation_target.curated_metadata if citation_target.curated_metadata is not None else {}
        metadata['status'] = citation_target.status
        if curate:
            #modified metadata updates every field that isn't the doi or the canonical bibcode
            metadata['parsed'] = generate_modified_metadata(citation_target.parsed_cited_metadata, metadata['curated']) if citation_target.parsed_cited_metadata is not None else {}
            #This line replaces the parsed bibcode with the bibcode column
            if citation_target.bibcode: metadata['parsed'].update({'bibcode': citation_target.bibcode})
        else:
            metadata['parsed'] = citation_target.parsed_cited_metadata if citation_target.parsed_cited_metadata is not None else {}
    return metadata

def get_citation_target_metadata(app, doi, curate=True):
    """
    If the citation target already exists in the database, return the raw and
    parsed metadata together with the status of the citation target in the
    database.
    If not, return an empty dictionary.
    """
    citation_in_db = False
    metadata = {}
    with app.session_scope() as session:
        metadata = _get_citation_target_metadata_session(session, doi, citation_in_db, metadata, curate) 
    return metadata

def get_citation_target_entry_date(app, doi):
    """
    If the citation target already exists in the database, return the entry date.
    If not, return None.
    """
    citation_in_db = False
    entry_date = None
    with app.session_scope() as session:
        citation_target = session.query(CitationTarget).filter_by(content=doi).first()
        citation_target_in_db = citation_target is not None
        if citation_target_in_db:
            entry_date = citation_target.created
    return entry_date

def get_citations_by_bibcode(app, bibcode):
    """
    Transform bibcode into content and get all the citations by content.
    It will ignore DELETED and DISCARDED citations and citations targets.
    """
    citations = []
    if bibcode is not None:
        with app.session_scope() as session:
            #bibcode = "2015zndo.....14475J"
            citation_target = session.query(CitationTarget).filter(CitationTarget.parsed_cited_metadata['bibcode'].astext == bibcode).filter_by(status="REGISTERED").first()
            if citation_target:
                dummy_citation_change = CitationChange(content=citation_target.content)
                citations = get_citations(app, dummy_citation_change)
    return citations

def get_citations(app, citation_change):
    """
    Return all the citations (bibcodes) to a given content.
    It will ignore DELETED and DISCARDED citations.
    """
    with app.session_scope() as session:
        citation_bibcodes = [r.citing for r in session.query(Citation).filter_by(content=citation_change.content, status="REGISTERED").all()]
    return citation_bibcodes

def generate_modified_metadata(parsed_metadata, curated_entry):
    """
    modify parsed_metadata with any curated metadata. return results.
    """
    modified_metadata = parsed_metadata.copy()
    bad_keys=[]
    if not modified_metadata.get('alternate_bibcode', None): modified_metadata.update({'alternate_bibcode':[]})
    for key in curated_entry.keys():
        if key not in ['bibcode', 'doi']:
            if key in modified_metadata.keys():
                try:
                    modified_metadata[key] = curated_entry[key]
                except Exception as e:
                    logger.error("Failed setting {} for {} with Exception: {}.".format(key, parsed_metadata.get('bibcode'), e))
            else:
                logger.warn("{} is not a valid entry for parsed_cited_metadata. Flagging key for removal.".format(key))
                bad_keys.append(key)
    #remove bad keys from curated entries.
    for key in bad_keys:
        curated_entry.pop(key)
    return modified_metadata

def citation_already_exists(app, citation_change):
    """
    Is this citation already stored in the DB?
    """
    citation_in_db = False
    with app.session_scope() as session:
        citation = session.query(Citation).filter_by(citing=citation_change.citing, content=citation_change.content).first()
        citation_in_db = citation is not None
    return citation_in_db

def update_citation(app, citation_change):
    """
    Update cited information
    """
    updated = False
    with app.session_scope() as session:
        citation = session.query(Citation).with_for_update().filter_by(citing=citation_change.citing, content=citation_change.content).first()
        change_timestamp = citation_change.timestamp.ToDatetime().replace(tzinfo=tzutc()) # Consider it as UTC to be able to compare it
        if citation.timestamp < change_timestamp:
            #citation.citing = citation_change.citing # This should not change
            #citation.content = citation_change.content # This should not change
            citation.cited = citation_change.cited
            citation.resolved = citation_change.resolved
            citation.timestamp = change_timestamp
            session.add(citation)
            session.commit()
            updated = True
            logger.info("Updated citation (citing '%s', content '%s' and timestamp '%s')", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
        else:
            logger.info("Ignoring citation update (citing '%s', content '%s' and timestamp '%s') because received timestamp is equal/older than timestamp in database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
    return updated

def mark_citation_as_deleted(app, citation_change):
    """
    Update status to DELETED for a given citation
    """
    marked_as_deleted = False
    previous_status = None
    with app.session_scope() as session:
        citation = session.query(Citation).with_for_update().filter_by(citing=citation_change.citing, content=citation_change.content).first()
        previous_status = citation.status
        change_timestamp = citation_change.timestamp.ToDatetime().replace(tzinfo=tzutc()) # Consider it as UTC to be able to compare it
        if citation.timestamp < change_timestamp:
            citation.status = "DELETED"
            citation.timestamp = change_timestamp
            session.add(citation)
            session.commit()
            marked_as_deleted = True
            logger.info("Marked citation as deleted (citing '%s', content '%s' and timestamp '%s')", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
        else:
            logger.info("Ignoring citation deletion (citing '%s', content '%s' and timestamp '%s') because received timestamp is equal/older than timestamp in database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
    return marked_as_deleted, previous_status

def mark_all_discarded_citations_as_registered(app, content):
    """
    Update status to REGISTERED for all discarded citations of a given content
    """
    marked_as_registered = False
    previous_status = None
    with app.session_scope() as session:
        citations = session.query(Citation).with_for_update().filter_by(status='DISCARDED', content=content).all()
        for citation in citations:
            citation.status = 'REGISTERED'
            session.add(citation)
        session.commit()

def populate_bibcode_column(main_session, curated = True):
    """
    Pulls all citation targets from DB and populates the bibcode column using parsed metadata
    """
    logger.debug("Collecting Citation Targets")
    records = _get_citation_targets_alembic(main_session, only_status = None)
    for record in records:
        content = record.get('content', None)
        logger.debug("Collecting metadata for {}".format(record.get('content')))
        metadata = _get_citation_target_metadata_alembic(main_session, content, curate = curated)
        if metadata:
            logger.debug("Updating Bibcode field for {}".format(record.get('content')))
            raw_metadata = metadata.get('raw', {})
            parsed_metadata = metadata.get('parsed', {})
            curated_metadata = metadata.get('curated',{})
            status = metadata.get('status', None)
            _update_citation_target_metadata_alembic(main_session, content, raw_metadata, parsed_metadata, curated_metadata, status)

def _update_citation_target_metadata_alembic(session, content, raw_metadata, parsed_metadata, curated_metadata = {}, status=None, bibcode = None):
    """
    Update metadata for a citation target when we do not need to
    close the session after completion
    """
    metadata_updated = False
    if not bibcode: bibcode = parsed_metadata.get('bibcode', None)
    metadata_updated = _update_citation_target_metadata_session(session, content, raw_metadata, parsed_metadata, curated_metadata, status, bibcode)    
    return metadata_updated
            
def _get_citation_target_metadata_alembic(session, doi, curate=True):
    """
    If the citation target already exists in the database, return the raw and
    parsed metadata together with the status of the citation target in the
    database.
    If not, return an empty dictionary. 
    Variation for when the function does not need to control the session
    """
    citation_in_db = False
    metadata = {}
    return _get_citation_target_metadata_session(session, doi, citation_in_db, metadata, curate)

def _get_citation_targets_alembic(session, only_status='REGISTERED'):
    """
    Return a list of dict with all citation targets (or only the registered ones)
    - Records without a bibcode in the database will not be returned
    Variation for then the function does not need to control the session.
    """
    return _get_citation_targets_session(session, only_status)
