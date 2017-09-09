from sqlalchemy import Column, DateTime, String, Text, Integer, func
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
    new_doi = Column(Text())
    new_pid = Column(Text())
    new_url = Column(Text())
    new_data = Column(Text())
    new_score = Column(Text())
    previous_citing = Column(Text())
    previous_cited = Column(Text())
    previous_doi = Column(Text())
    previous_pid = Column(Text())
    previous_url = Column(Text())
    previous_data = Column(Text())
    previous_score = Column(Text())
    status = Column(ENUM('NEW', 'DELETED', 'UPDATED', name='status_type'))

