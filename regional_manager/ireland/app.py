from fastapi import FastAPI, HTTPException, Body, Depends
from typing import List, Tuple, Dict, Any
from database import get_db
from models import SegmentRequest, RegionalSegment, BookingSegment
import uvicorn
from service.segment_service import SegmentService
import logging
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time
from database import Base, engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("ireland_manager")

# Create tables if they don't exist
Base.metadata.create_all(bind=engine, checkfirst=True)

# FastAPI app
app = FastAPI(title="Regional Manager - Ireland")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add route for checking capacity that was missing
@app.post("/check_capacity")
async def check_capacity(data: Dict[str, Any] = Body(...), db = Depends(get_db)):
    """
    Check if all segments in a list have available capacity.
    Returns OK only if all segments have sufficient capacity.
    """
    start_time = time.time()
    booking_id = data.get("booking_id")
    segments = data.get("segments", [])
    
    if not booking_id:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "booking_id is required"}
        )
    
    if not segments:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "segments list is required"}
        )
    
    try:
        segment_service = SegmentService(db)
        has_capacity = segment_service.check_segments_capacity(segments)
        
        processing_time = time.time() - start_time
        logger.info(f"Capacity check for booking {booking_id} took {processing_time:.3f}s, result: {has_capacity}")
        
        if has_capacity:
            return {"status": "ok", "message": "All segments have capacity"}
        else:
            # Record failed segments for tracking
            segment_service.record_failed_segments(booking_id, segments)
            return {"status": "failed", "message": "Some segments are at capacity"}
            
    except Exception as e:
        logger.error(f"Error checking capacity: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

@app.post("/process_segment")
async def process_segment(segment_request: SegmentRequest, db = Depends(get_db)):
    """
    Process a route and convert it to segment IDs.
    Returns the list of segment IDs for the route.
    """
    start_time = time.time()
    booking_id = segment_request.booking_id
    coordinates = segment_request.coordinates
    
    try:
        segment_service = SegmentService(db)
        
        # Convert route coordinates to segment IDs, don't reserve yet
        segment_ids = segment_service.convert_route_to_segments(coordinates)
        
        processing_time = time.time() - start_time
        logger.info(f"Route processing for booking {booking_id} took {processing_time:.3f}s")
        
        return {
            "booking_id": booking_id,
            "segments": segment_ids
        }
        
    except Exception as e:
        logger.error(f"Error processing segments: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

@app.post("/confirm_booking")
async def confirm_booking(data: Dict[str, str] = Body(...), db = Depends(get_db)):
    """
    Confirm a booking by updating all segments to 'success' status.
    """
    start_time = time.time()
    booking_id = data.get("booking_id")
    
    if not booking_id:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "booking_id is required"}
        )
        
    try:
        segment_service = SegmentService(db)
        segment_service.confirm_booking(booking_id)
        
        processing_time = time.time() - start_time
        logger.info(f"Booking confirmation for {booking_id} took {processing_time:.3f}s")
        
        return {"status": "success", "message": f"Booking {booking_id} confirmed"}
        
    except Exception as e:
        logger.error(f"Error confirming booking: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

@app.post("/cancel_booking")
async def cancel_booking(data: Dict[str, str] = Body(...), db = Depends(get_db)):
    """
    Cancel a booking and release all reserved segments.
    """
    start_time = time.time()
    booking_id = data.get("booking_id")
    
    if not booking_id:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "booking_id is required"}
        )
        
    try:
        segment_service = SegmentService(db)
        result = segment_service.cancel_booking(booking_id)
        
        processing_time = time.time() - start_time
        logger.info(f"Booking cancellation for {booking_id} took {processing_time:.3f}s")
        
        return result
        
    except Exception as e:
        logger.error(f"Error cancelling booking: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

@app.get("/get_segments/{booking_id}")
async def get_segments(booking_id: str, db = Depends(get_db)):
    """
    Get all segments for a booking with their current status and load.
    """
    start_time = time.time()
    
    try:
        segment_service = SegmentService(db)
        result = segment_service.get_segments(booking_id)
        
        processing_time = time.time() - start_time
        logger.info(f"Get segments for booking {booking_id} took {processing_time:.3f}s")
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting segments: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)

