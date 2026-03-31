"""
Shared database helpers used across route modules.
"""
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from models import Donor


def get_or_404(db: Session, session_id: str, lock: bool = False) -> Donor:
    """Fetch a Donor by session_id or raise 404. Pass lock=True for write paths."""
    q = db.query(Donor).filter(Donor.session_id == session_id)
    if lock:
        q = q.with_for_update()
    donor = q.first()
    if not donor:
        raise HTTPException(status_code=404, detail="Session not found")
    return donor


def save_profile(db: Session, donor: Donor) -> None:
    """Persist a mutated profile_data back to the database."""
    flag_modified(donor, "profile_data")
    db.add(donor)
    db.commit()
