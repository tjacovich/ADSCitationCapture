from sqlalchemy import Column, Boolean, DateTime, String, Text, Integer, func
from sqlalchemy.dialects.postgresql import ENUM, JSON, JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

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
    status = Column(ENUM('NEW', 'DELETED', 'UPDATED', name='status_type'))

