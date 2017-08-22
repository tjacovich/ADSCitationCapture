from sqlalchemy import Column, DateTime, String, Integer, func
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class RawCitation(Base):
    __tablename__ = 'raw_citation'
    id = Column(Integer, primary_key=True)
    bibcode = Column(String(19))
    payload = Column(JSON)
