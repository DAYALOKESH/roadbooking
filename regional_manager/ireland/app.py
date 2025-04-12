from fastapi import FastAPI, HTTPException
from typing import List, Tuple
from database import SessionLocal
from models import SegmentRequest, RegionalSegment, BookingSegment
import uvicorn
from service.segment_service import SegmentService

from database import Base, engine


Base.metadata.create_all(bind=engine, checkfirst=True)

app = FastAPI(title="Regional Manager - Ireland")

@app.post("/process_segment")
async def process_segment(segment_request: SegmentRequest):
    booking_id = segment_request.booking_id
    coordinates = segment_request.coordinates
    name = segment_request.name
    email = segment_request.email
    start_time = segment_request.start_time

    db = SessionLocal()
    segment_service = SegmentService(db)
    try:
        # Convert route coordinates to segment IDs
        segment_ids = segment_service.convert_route_to_segments(coordinates)

        # Check if all segments have sufficient capacity
        if not segment_service.check_segments_capacity(segment_ids):
            raise HTTPException(status_code=400, detail="Insufficient capacity on one or more segments")

        # Reserve segments for the booking
        segment_service.reserve_segments(booking_id, segment_ids)

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to process segment: {str(e)}")
    finally:
        db.close()

    return {"status": "success", "message": "Segment processed successfully"}

@app.post("/confirm_booking")
async def confirm_booking(booking_id: str):
    db = SessionLocal()
    try:
        segment_service = SegmentService(db)
        segment_service.confirm_booking(booking_id)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to confirm booking: {str(e)}")
    finally:
        db.close()

    return {"status": "success", "message": "Booking confirmed"}

@app.post("/cancel_booking")
async def cancel_booking(booking_id: str):
    db = SessionLocal()
    try:
        segment_service = SegmentService(db)
        segment_service.cancel_booking(booking_id)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to cancel booking: {str(e)}")
    finally:
        db.close()

    return {"status": "success", "message": "Booking cancelled"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)

