from sqlalchemy import Column, Boolean, DateTime, String, Text, Integer, func, UniqueConstraint, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import ENUM, JSON, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_continuum import make_versioned
from adsputils import UTCDateTime, get_date

# Must be called before defining all the models
make_versioned(user_cls=None)

Base = declarative_base()

citation_content_type = ENUM('DOI', 'PID', 'URL', name='citation_content_type')
citation_change_type = ENUM('NEW', 'DELETED', 'UPDATED', name='citation_change_type')
    
from adsputils import load_config
import os
#import pipeline configuration
proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
    
#sets citation_status and target_status depending on whether urls are being passed to broker
if config.get('URL_HOOK',False):
    citation_status_type = ENUM('REGISTERED', 'DELETED', 'DISCARDED', 'EMITTABLE', name='citation_status_type')
    target_status_type = ENUM('REGISTERED', 'DELETED', 'DISCARDED', 'EMITTABLE', name='target_status_type')
else:
    citation_status_type = ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='citation_status_type')
    target_status_type = ENUM('REGISTERED', 'DELETED', 'DISCARDED', name='target_status_type')
        
class RawCitation(Base):
    __tablename__ = 'raw_citation'
    __table_args__ = ({"schema": "public"})
    id = Column(Integer, primary_key=True)
    bibcode = Column(String(19))
    payload = Column(JSONB) # Binary, faster than JSON (requires postgres >9.4)

class CitationChanges(Base):
    __tablename__ = 'citation_changes'
    __table_args__ = ({"schema": "public"})
    id = Column(Integer, primary_key=True)
    new_id = Column(Integer)
    new_citing = Column(Text())
    new_cited = Column(Text())
    new_doi = Column(Boolean())
    new_pid = Column(Boolean())
    new_url = Column(Boolean())
    new_content = Column(Text())
    new_resolved = Column(Boolean())
    previous_citing = Column(Text())
    previous_cited = Column(Text())
    previous_doi = Column(Boolean())
    previous_pid = Column(Boolean())
    previous_url = Column(Boolean())
    previous_content = Column(Text())
    previous_resolved = Column(Boolean())
    status = Column(citation_change_type)

class Citation(Base):
    __tablename__ = 'citation'
    __table_args__ = (
        UniqueConstraint('citing', 'content', name='citing_content_unique_constraint'),
        {"schema": "public"}
    )
    __versioned__ = {}  # Must be added to all models that are to be versioned
    id = Column(Integer, primary_key=True)
    content = Column(Text(), ForeignKey('public.citation_target.content'))
    citing = Column(Text())                         # Bibcode of the article that is citing a target
    cited = Column(Text())                          # Probably not necessary to keep
    resolved = Column(Boolean())                    # Probably not necessary to keep
    timestamp = Column(UTCDateTime)
    status = Column(citation_status_type)
    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, onupdate=get_date)


class CitationTarget(Base):
    __tablename__ = 'citation_target'
    __table_args__ = ({"schema": "public"})
    __versioned__ = {}  # Must be added to all models that are to be versioned
    content = Column(Text(), primary_key=True)      # DOI/URL/PID value: we assume it is unique independently what content type is
    content_type = Column(citation_content_type)
    raw_cited_metadata = Column(Text())
    parsed_cited_metadata = Column(JSONB)
    status = Column(target_status_type)
    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, onupdate=get_date)
    citations = relationship("Citation", primaryjoin="CitationTarget.content==Citation.content")

class Event(Base):
    __tablename__ = 'event'
    __table_args__ = ({"schema": "public"})
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, onupdate=get_date)

# Must be called after defining all the models
orm.configure_mappers()
