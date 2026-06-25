from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Lead, LeadCreate, LeadResponse, APIResponse
from typing import List
from datetime import date
from app.messaging import publish

router = APIRouter(prefix="/leads", tags=["Leads"])


@router.get("/", response_model=List[LeadResponse])
def get_all_leads(db: Session = Depends(get_db)):
    """Return all leads ordered by most recently created."""
    leads = db.query(Lead).order_by(Lead.created_date.desc()).all()
    return leads


@router.get("/{lead_id}", response_model=LeadResponse)
def get_lead(lead_id: int, db: Session = Depends(get_db)):
    """Return a single lead by ID."""
    lead = db.query(Lead).filter(Lead.lead_id == lead_id).first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return lead


@router.post("/", response_model=APIResponse)
def create_lead(lead: LeadCreate, db: Session = Depends(get_db)):
    """Create a new lead and publish a lead.created event."""
    new_lead = Lead(
        **lead.model_dump(),
        created_date=date.today()
    )
    db.add(new_lead)
    db.commit()
    db.refresh(new_lead)

    # Publish event to RabbitMQ
    publish(
        routing_key="lead.created",
        message={
            "event":        "lead.created",
            "lead_id":      new_lead.lead_id,
            "lead_name":    new_lead.lead_name,
            "company_name": new_lead.company_name,
            "source":       new_lead.source,
            "status":       new_lead.status
        }
    )

    return APIResponse(
        success=True,
        message="Lead created successfully",
        data={"lead_id": new_lead.lead_id}
    )


@router.put("/{lead_id}/status", response_model=APIResponse)
def update_lead_status(lead_id: int, status: str, db: Session = Depends(get_db)):
    """Update a lead's status."""
    lead = db.query(Lead).filter(Lead.lead_id == lead_id).first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    valid_statuses = ["New", "Contacted", "Qualified", "Lost"]
    if status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {valid_statuses}"
        )
    lead.status = status
    db.commit()
    return APIResponse(success=True, message=f"Lead status updated to {status}")


@router.delete("/{lead_id}", response_model=APIResponse)
def delete_lead(lead_id: int, db: Session = Depends(get_db)):
    """Delete a lead and all linked opportunities and activities."""
    lead = db.query(Lead).filter(Lead.lead_id == lead_id).first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    db.delete(lead)
    db.commit()
    return APIResponse(success=True, message="Lead deleted successfully")