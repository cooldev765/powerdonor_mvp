"""
SQLAlchemy ORM models — only tables the app writes to.

mvp_charities and irs_990 are read-only and queried via raw SQL in
routes_matching.py (JSONB operators, pgvector, LATERAL joins).
No ORM model for them keeps create_all from touching those tables.
"""
from sqlalchemy import Column, Integer, String, DateTime, Boolean, JSON, func
from database import Base


class Donor(Base):
    __tablename__ = "donors"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    session_id   = Column(String, unique=True, index=True)
    firstname    = Column(String)
    email        = Column(String)
    created_at   = Column(DateTime, server_default=func.now())
    updated_at   = Column(DateTime, server_default=func.now(), onupdate=func.now())
    profile_data = Column(JSON, default=dict)   # { interview, per_category, outputs, _state }
    is_complete  = Column(Boolean, default=False)
