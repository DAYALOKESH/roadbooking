from fastapi import FastAPI, HTTPException, Body
from typing import List, Tuple, Dict
from database import SessionLocal
from models import SegmentRequest, RegionalSegment, BookingSegment
import uvicorn
from service.segment_service import SegmentService
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from database import Base, engine


Base.metadata.create_all(bind=engine, checkfirst=True)

app = FastAPI(title="Regional Manager - London")

@app.post("/process_segment")
async def process_segment(segment_request: SegmentRequest):
    booking_id = segment_request.booking_id
    coordinates = segment_request.coordinates
    name = segment_request.name
    email = segment_request.email
    start_time = segment_request.start_time
    print(coordinates)

    db = SessionLocal()
    segment_service = SegmentService(db)
    try:
        # Convert route coordinates to segment IDs
        segment_ids = segment_service.convert_route_to_segments(coordinates)
        print(f"Found {len(segment_ids)} segments: {segment_ids}")
        # Check if all segments have sufficient capacity
        if not segment_service.check_segments_capacity(segment_ids):
            raise HTTPException(status_code=400, detail="Insufficient capacity on one or more segments")

        # Reserve segments for the booking
        segment_service.reserve_segments(booking_id, segment_ids)
        print(f"Successfully reserved {len(segment_ids)} segments for booking {booking_id}")

    except ValueError as ve:
        db.rollback()
        logger.error(f"ValueError: {str(ve)}")
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(ve)}")
    except HTTPException as he:
        db.rollback()
        logger.error(f"HTTPException: {str(he)}")
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to process segment: {str(e)}")
    finally:
        db.close()

    return {"status": "success", "message": "Segment processed successfully"}

@app.post("/confirm_booking")
async def confirm_booking(data: Dict[str, str] = Body(...)):
    booking_id = data.get("booking_id")
    if not booking_id:
        raise HTTPException(status_code=400, detail="booking_id is required")
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
async def cancel_booking(data: Dict[str, str] = Body(...)):
    booking_id = data.get("booking_id")
    if not booking_id:
        raise HTTPException(status_code=400, detail="booking_id is required")
    db = SessionLocal()
    try:
        segment_service = SegmentService(db)
        result = segment_service.cancel_booking(booking_id)
        return result
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to cancel booking: {str(e)}")
    finally:
        db.close()




# this gen points gets the booking id from the user then query the segments from the database and return the segments that are associated with that booking id
# if the booking id is not found return a error message
# also we will return the current load of the segments
@app.get("/get_segments/{booking_id}")
async def get_segments(booking_id: str):
    db = SessionLocal()
    try:
        segment_service = SegmentService(db)
        result = segment_service.get_segments(booking_id)
        return result
    finally:
        db.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)

