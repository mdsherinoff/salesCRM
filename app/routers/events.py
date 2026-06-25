from fastapi          import APIRouter, HTTPException, Depends
from sqlalchemy.orm   import Session
from app.database     import get_db
from app.models       import Lead, Opportunity, Activity, APIResponse
from app.messaging    import publish
from datetime         import date
from dateutil.relativedelta import relativedelta

router = APIRouter(prefix="/events", tags=["Events"])


@router.post("/leads/{lead_id}/convert", response_model=APIResponse)
def convert_lead(lead_id: int, db: Session = Depends(get_db)):
    """
    Convert a lead to an opportunity.
    1. Validates lead exists and isn't already converted
    2. Updates lead status to Qualified
    3. Creates a linked opportunity
    4. Logs an activity
    5. Publishes lead.converted event to RabbitMQ
    """
    # 1. Get the lead
    lead = db.query(Lead).filter(Lead.lead_id == lead_id).first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    # 2. Check not already converted
    existing = db.query(Opportunity).filter(
        Opportunity.lead_id == lead_id
    ).first()
    if existing:
        raise HTTPException(
            status_code=400,
            detail="Lead already has a linked opportunity"
        )

    # 3. Update lead status
    lead.status = "Qualified"

    # 4. Create opportunity
    opp = Opportunity(
        lead_id             = lead_id,
        opp_name            = f"{lead.lead_name} — New Opportunity",
        deal_value          = 0,
        stage               = "Prospecting",
        probability         = 10,
        expected_close_date = date.today() + relativedelta(months=3),
        created_date        = date.today()
    )
    db.add(opp)

    # 5. Log activity
    activity = Activity(
        lead_id       = lead_id,
        activity_type = "Follow-up",
        notes         = "Lead converted to opportunity via FastAPI.",
        activity_date = date.today()
    )
    db.add(activity)
    db.commit()
    db.refresh(opp)

    # 6. Publish event — happens AFTER db commit so data is safe
    publish(
        routing_key="lead.converted",
        message={
            "event":        "lead.converted",
            "lead_id":      lead_id,
            "lead_name":    lead.lead_name,
            "company_name": lead.company_name,
            "email":        lead.email,
            "opp_id":       opp.opp_id,
            "opp_name":     opp.opp_name
        }
    )

    return APIResponse(
        success=True,
        message=f"Lead converted successfully",
        data={
            "lead_id": lead_id,
            "opp_id":  opp.opp_id
        }
    )


@router.post("/opportunities/{opp_id}/won", response_model=APIResponse)
def mark_deal_won(opp_id: int, db: Session = Depends(get_db)):
    """Mark a deal as Won and publish deal.won event."""
    opp = db.query(Opportunity).filter(Opportunity.opp_id == opp_id).first()
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    opp.stage       = "Won"
    opp.probability = 100
    db.commit()

    publish(
        routing_key="deal.won",
        message={
            "event":      "deal.won",
            "opp_id":     opp_id,
            "opp_name":   opp.opp_name,
            "deal_value": float(opp.deal_value),
            "lead_id":    opp.lead_id
        }
    )

    return APIResponse(
        success=True,
        message="Deal marked as Won",
        data={"opp_id": opp_id}
    )


@router.post("/opportunities/{opp_id}/lost", response_model=APIResponse)
def mark_deal_lost(opp_id: int, db: Session = Depends(get_db)):
    """Mark a deal as Lost and publish deal.lost event."""
    opp = db.query(Opportunity).filter(Opportunity.opp_id == opp_id).first()
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    opp.stage       = "Lost"
    opp.probability = 0
    db.commit()

    publish(
        routing_key="deal.lost",
        message={
            "event":    "deal.lost",
            "opp_id":   opp_id,
            "opp_name": opp.opp_name,
            "lead_id":  opp.lead_id
        }
    )

    return APIResponse(
        success=True,
        message="Deal marked as Lost",
        data={"opp_id": opp_id}
    )