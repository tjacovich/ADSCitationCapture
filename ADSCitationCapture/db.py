from psycopg2 import IntegrityError
from ADSCitationCapture.models import Citation, CitationTarget

def store_citation_target(app, citation_change, content_type, raw_metadata, parsed_metadata, status):
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
        citation_target.status = status
        session.add(citation_target)
        try:
            session.commit()
        except IntegrityError, e:
            # IntegrityError: (psycopg2.IntegrityError) duplicate key value violates unique constraint "citing_content_unique_constraint"
            logger.error("Ignoring new citation target (citting '%s', content '%s' and timestamp '%s') because it already exists in the database (another new citation may have been processed before this one): '%s'", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString(), str(e))
        else:
            stored = True
    return stored

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
        citation.timestamp = citation_change.timestamp.ToDatetime()
        citation.status = status
        session.add(citation)
        try:
            session.commit()
        except IntegrityError, e:
            # IntegrityError: (psycopg2.IntegrityError) duplicate key value violates unique constraint "citing_content_unique_constraint"
            logger.error("Ignoring new citation (citting '%s', content '%s' and timestamp '%s') because it already exists in the database when it is not supposed to (race condition?): '%s'", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString(), str(e))
        else:
            stored = True
    return stored

def get_citation_target_metadata(app, citation_change):
    """
    If the citation target already exists in the database, return the metadata.
    If not, return an empty dictionary.
    """
    citation_in_db = False
    metadata = {}
    with app.session_scope() as session:
        citation_target = session.query(CitationTarget).filter_by(content=citation_change.content).first()
        citation_target_in_db = citation_target is not None
        if citation_target_in_db:
            metadata['raw'] = citation_target.raw_cited_metadata
            metadata['parsed'] = citation_target.parsed_cited_metadata
    return metadata


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
        if citation.timestamp < citation_change.timestamp.ToDatetime():
            if citation.status in ("REGISTERED", "DISCARDED"):
                #citation.citing = citation_change.citing # This should not change
                #citation.content = citation_change.content # This should not change
                citation.cited = citation_change.cited
                citation.resolved = citation_change.resolved
                citation.timestamp = citation_change.timestamp.ToDatetime()
                session.add(citation)
                session.commit()
                updated = True
            else:
                logger.error("Ignoring citation update (citting '%s', content '%s' and timestamp '%s') because the record is registered as deleted in the database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
        else:
            logger.error("Ignoring citation update (citting '%s', content '%s' and timestamp '%s') because received timestamp is older than timestamp in database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
    return updated

def mark_citation_as_deleted(app, citation_change):
    """
    Update status to DELETED for a given citation
    """
    marked_as_deleted = False
    with app.session_scope() as session:
        citation = session.query(Citation).with_for_update().filter_by(citing=citation_change.citing, content=citation_change.content).first()
        if citation.timestamp < citation_change.timestamp.ToDatetime():
            citation.status = "DELETED"
            citation.timestamp = citation_change.timestamp.ToDatetime()
            session.add(citation)
            session.commit()
            marked_as_deleted = True
        else:
            logger.error("Ignoring citation deletion (citting '%s', content '%s' and timestamp '%s') because received timestamp is older than timestamp in database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
    return marked_as_deleted
