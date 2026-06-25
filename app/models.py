from sqlalchemy import Column, Integer, String, Date, Numeric, ForeignKey
from sqlalchemy.orm import relationship
from pydantic import BaseModel
from typing import Optional
from datetime import date
from app.database import Base
from sqlalchemy import Column, Integer, String, DateTime, JSON
from datetime import datetime

# ─── SQLAlchemy ORM Models (map to DB tables) ────────────────────────────────

class Lead(Base):
    __tablename__ = "leads"

    lead_id      = Column(Integer, primary_key=True, index=True)
    lead_name    = Column(String(100), nullable=False)
    company_name = Column(String(100))
    email        = Column(String(150))
    phone        = Column(String(30))
    source       = Column(String(50))
    status       = Column(String(30), default="New")
    created_date = Column(Date)

    opportunities = relationship("Opportunity", back_populates="lead", cascade="all, delete")
    activities    = relationship("Activity",    back_populates="lead", cascade="all, delete")


class Opportunity(Base):
    __tablename__ = "opportunities"

    opp_id              = Column(Integer, primary_key=True, index=True)
    lead_id             = Column(Integer, ForeignKey("leads.lead_id"), nullable=False)
    opp_name            = Column(String(150))
    deal_value          = Column(Numeric(12, 2), default=0)
    stage               = Column(String(50), default="Prospecting")
    probability         = Column(Numeric(5, 2), default=10)
    expected_close_date = Column(Date)
    created_date        = Column(Date)

    lead = relationship("Lead", back_populates="opportunities")


class Activity(Base):
    __tablename__ = "activities"

    activity_id   = Column(Integer, primary_key=True, index=True)
    lead_id       = Column(Integer, ForeignKey("leads.lead_id"), nullable=False)
    activity_type = Column(String(50), nullable=False)
    notes         = Column(String(4000))
    activity_date = Column(Date)

    lead = relationship("Lead", back_populates="activities")


# ─── Pydantic Schemas (request/response shapes) ───────────────────────────────

class LeadCreate(BaseModel):
    lead_name:    str
    company_name: Optional[str] = None
    email:        Optional[str] = None
    phone:        Optional[str] = None
    source:       Optional[str] = None
    status:       str = "New"


class LeadResponse(BaseModel):
    lead_id:      int
    lead_name:    str
    company_name: Optional[str]
    email:        Optional[str]
    phone:        Optional[str]
    source:       Optional[str]
    status:       str
    created_date: Optional[date]

    class Config:
        from_attributes = True


class OpportunityResponse(BaseModel):
    opp_id:              int
    lead_id:             int
    opp_name:            Optional[str]
    deal_value:          Optional[float]
    stage:               Optional[str]
    probability:         Optional[float]
    expected_close_date: Optional[date]

    class Config:
        from_attributes = True


class ActivityCreate(BaseModel):
    lead_id:       int
    activity_type: str
    notes:         Optional[str] = None


class ActivityResponse(BaseModel):
    activity_id:   int
    lead_id:       int
    activity_type: str
    notes:         Optional[str]
    activity_date: Optional[date]

    class Config:
        from_attributes = True


class APIResponse(BaseModel):
    success: bool
    message: str
    data:    Optional[dict] = None


class EventLog(Base):
    __tablename__ = "event_log"

    id           = Column(Integer, primary_key=True, index=True)
    event_type   = Column(String(100), nullable=False)
    routing_key  = Column(String(100))
    payload      = Column(JSON)
    processed_at = Column(DateTime, default=datetime.utcnow)
    status       = Column(String(20), default="processed")
    error        = Column(String(500), nullable=True)