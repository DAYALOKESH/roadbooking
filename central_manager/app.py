from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Any, Union, Tuple
import httpx
import asyncio
import uuid
import time
import logging
from contextlib import asynccontextmanager
import json
from cachetools import TTLCache
from database import SessionLocal
from models import UserRequest, BookingInfo
from database import Base, engine
from sqlalchemy.exc import IntegrityError
import polyline
import uvicorn

Base.metadata.create_all(bind=engine)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = FastAPI(title="Multi-region service Manger")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration for regional endpoints
REGIONAL_ENDPOINTS = {
    "ireland": {
        "base_url": "http://localhost:8001",
        "check_capacity": "/check_capacity",
        "process_segment": "/process_segment",
        "confirm_booking": "/confirm_booking",
        "cancel_booking": "/cancel_booking",
        "get_segments": "/get_segments"
    },
    "london": {
        "base_url": "http://localhost:8002",
        "check_capacity": "/check_capacity",
        "process_segment": "/process_segment",
        "confirm_booking": "/confirm_booking",
        "cancel_booking": "/cancel_booking",
        "get_segments": "/get_segments"
    }
}

# Create rate limiters for each region
regional_semaphores = {
    "ireland": asyncio.Semaphore(20),  # Allow 20 concurrent requests
    "london": asyncio.Semaphore(20)
}

# Cache for route calculations (1 hour TTL, 1000 max size)
route_cache = TTLCache(maxsize=1000, ttl=3600)

# Models
class RouteRequest(BaseModel):
    start_coordinates: str  # Format: "lat,lon"
    destination_coordinates: str  # Format: "lat,lon"
    user_id: Optional[str] = None
    name: Optional[str] = None
    email: Optional[str] = None
    start_time: Optional[str] = None

class BookingResponse(BaseModel):
    booking_id: str
    status: str
    results: Dict[str, Any]

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create HTTP client with optimized settings
    app.state.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20,
            keepalive_expiry=60
        )
    )
    yield
    # Clean up resources
    await app.state.http_client.aclose()

async def make_regional_request(region: str, endpoint: str, data: Dict[str, Any], client: httpx.AsyncClient) -> Tuple[bool, Dict[str, Any]]:
    """
    Make a rate-limited request to a regional endpoint with retry logic.
    Returns (success, response_data)
    """
    # Use semaphore to rate limit requests to each region
    async with regional_semaphores[region]:
        # Get the full URL
        base_url = REGIONAL_ENDPOINTS[region]["base_url"]
        path = REGIONAL_ENDPOINTS[region][endpoint]
        url = f"{base_url}{path}"

        # Retry logic
        max_retries = 3
        retry_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Making request to {region} {endpoint} (attempt {attempt+1})")
                response = await client.post(url, json=data, timeout=10.0)
                
                if response.status_code == 200:
                    return True, response.json()
                else:
                    logger.warning(f"Error response from {region} {endpoint}: {response.status_code}, {response.text}")
                    
                    # Don't retry on client errors (4xx)
                    if response.status_code < 500:
                        return False, {"error": f"Error from {region} {endpoint}: {response.status_code}"}
                    
            except httpx.RequestError as e:
                logger.warning(f"Request error to {region} {endpoint}: {str(e)}")
                
            # Wait before retrying, with exponential backoff
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (2 ** attempt))
        
        # All retries failed
        return False, {"error": f"Failed to connect to {region} {endpoint} after {max_retries} attempts"}

def parse_coordinates(coordinates_str: str) -> List[float]:
    """Parse coordinates from string format 'lat,lon'"""
    try:
        lat, lon = map(float, coordinates_str.split(","))
        return [lat, lon]
    except ValueError:
        raise ValueError(f"Invalid coordinates format: {coordinates_str}. Expected 'lat,lon'")

def calculate_route(start: List[float], destination: List[float]) -> List[List[float]]:
    """
    Calculate a route between start and destination coordinates.
    Returns a list of coordinate pairs along the route.
    
    This is a simplified straight-line implementation. In a real system, 
    you would use a routing engine like OSRM, GraphHopper, or a mapping API.
    """
    # For simplicity, create a route with just start and end points
    return [start, destination]

@app.post("/send_request", response_model=BookingResponse)
async def send_request(request: RouteRequest):
    """
    Handle a new route request:
    1. Generate a unique booking ID
    2. Calculate route between start and destination
    3. Get segments from each region
    4. Check capacity for segments in each region
    5. If capacity available, reserve segments in each region
    6. Return booking details
    """
    booking_id = str(uuid.uuid4())
    start_time = time.time()
    logger.info(f"Processing booking {booking_id}")

    # Get HTTP client
    http_client = app.state.http_client
    
    try:
        # Parse coordinates
        start_coords = parse_coordinates(request.start_coordinates)
        dest_coords = parse_coordinates(request.destination_coordinates)
        
        # Calculate route (or retrieve from cache)
        cache_key = f"{request.start_coordinates}:{request.destination_coordinates}"
        if cache_key in route_cache:
            route = route_cache[cache_key]
            logger.info(f"Using cached route for {cache_key}")
        else:
            route = calculate_route(start_coords, dest_coords)
            route_cache[cache_key] = route
            logger.info(f"Calculated new route for {cache_key}")
        
        # Regional results for response
        regional_results = {}
        
        # Step 1: Get segments from each region
        segment_tasks = []
        for region in REGIONAL_ENDPOINTS:
            segment_tasks.append(
                make_regional_request(
                    region,
                    "process_segment",
                    {
                        "booking_id": booking_id,
                        "route": route,
                        "name": request.name or "",
                        "email": request.email or "",
                        "start_time": request.start_time or ""
                    },
                    http_client
                )
            )
        
        segment_results = await asyncio.gather(*segment_tasks)
        
        # Process segment results and prepare for capacity check
        all_segments_ok = True
        regional_segments = {}
        
        for i, region in enumerate(REGIONAL_ENDPOINTS):
            success, data = segment_results[i]
            if success and "segments" in data:
                regional_segments[region] = data["segments"]
                regional_results[region] = {"segment_count": len(data["segments"])}
            else:
                logger.warning(f"Failed to get segments for {region}: {data}")
                all_segments_ok = False
                regional_results[region] = {"error": "Failed to process segments"}
        
        if not all_segments_ok:
            processing_time = time.time() - start_time
            logger.error(f"Booking {booking_id} failed at segment processing after {processing_time:.3f}s")
            return BookingResponse(
                booking_id=booking_id,
                status="failed",
                results=regional_results
            )
        
        # Step 2: Check capacity in all regions
        capacity_tasks = []
        for region, segments in regional_segments.items():
            capacity_tasks.append(
                make_regional_request(
                    region,
                    "check_capacity",
                    {
                        "booking_id": booking_id,
                        "segments": segments
                    },
                    http_client
                )
            )
        
        capacity_results = await asyncio.gather(*capacity_tasks)
        
        # Process capacity results
        all_capacity_ok = True
        
        for i, region in enumerate(regional_segments):
            success, data = capacity_results[i]
            regional_results[region].update(data)
            
            if not success or data.get("status") != "ok":
                logger.warning(f"Capacity check failed for {region}: {data}")
                all_capacity_ok = False
        
        # Step 3: If all capacity checks pass, confirm bookings, otherwise cancel
        if all_capacity_ok:
            confirm_tasks = []
            for region in regional_segments:
                confirm_tasks.append(
                    make_regional_request(
                        region,
                        "confirm_booking",
                        {"booking_id": booking_id},
                        http_client
                    )
                )
            
            confirm_results = await asyncio.gather(*confirm_tasks)
            
            for i, region in enumerate(regional_segments):
                success, data = confirm_results[i]
                regional_results[region].update({"confirmation": data})
            
            status = "success"
            logger.info(f"Booking {booking_id} successfully confirmed")
        else:
            # Cancel any reservations that were made
            cancel_tasks = []
            for region in regional_segments:
                cancel_tasks.append(
                    make_regional_request(
                        region,
                        "cancel_booking",
                        {"booking_id": booking_id},
                        http_client
                    )
                )
            
            await asyncio.gather(*cancel_tasks)
            status = "failed"
            logger.warning(f"Booking {booking_id} failed due to capacity issues")
        
        processing_time = time.time() - start_time
        logger.info(f"Booking {booking_id} processed in {processing_time:.3f}s with status {status}")
        
        return BookingResponse(
            booking_id=booking_id,
            status=status,
            results=regional_results
        )
        
    except Exception as e:
        logger.error(f"Error processing booking {booking_id}: {str(e)}")
        
        # Try to cancel any reservations that might have been made
        try:
            cancel_tasks = []
            for region in REGIONAL_ENDPOINTS:
                cancel_tasks.append(
                    make_regional_request(
                        region,
                        "cancel_booking",
                        {"booking_id": booking_id},
                        http_client
                    )
                )
            await asyncio.gather(*cancel_tasks)
        except Exception as cancel_err:
            logger.error(f"Error during cancellation: {str(cancel_err)}")
        
        return BookingResponse(
            booking_id=booking_id,
            status="error",
            results={"error": str(e)}
        )

@app.get("/booking_status/{booking_id}")
async def booking_status(booking_id: str):
    """
    Get the status of a booking across all regions.
    """
    start_time = time.time()
    http_client = app.state.http_client
    
    try:
        # Query all regions for the booking status
        tasks = []
        for region, endpoints in REGIONAL_ENDPOINTS.items():
            url = f"{endpoints['base_url']}{endpoints['get_segments']}/{booking_id}"
            tasks.append(http_client.get(url))
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        results = {}
        for i, region in enumerate(REGIONAL_ENDPOINTS):
            response = responses[i]
            if isinstance(response, Exception):
                results[region] = {"status": "error", "message": str(response)}
            elif response.status_code == 200:
                results[region] = response.json()
            else:
                results[region] = {"status": "error", "message": f"HTTP {response.status_code}"}
        
        processing_time = time.time() - start_time
        logger.info(f"Booking status for {booking_id} retrieved in {processing_time:.3f}s")
        
        return {
            "booking_id": booking_id,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error getting booking status for {booking_id}: {str(e)}")
        return {
            "booking_id": booking_id,
            "status": "error",
            "message": str(e)
        }

@app.post("/cancel_booking/{booking_id}")
async def cancel_booking(booking_id: str):
    """
    Cancel a booking across all regions.
    """
    start_time = time.time()
    http_client = app.state.http_client
    
    try:
        # Cancel booking in all regions
        tasks = []
        for region in REGIONAL_ENDPOINTS:
            tasks.append(
                make_regional_request(
                    region,
                    "cancel_booking",
                    {"booking_id": booking_id},
                    http_client
                )
            )
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        regional_results = {}
        for i, region in enumerate(REGIONAL_ENDPOINTS):
            result = results[i]
            if isinstance(result, tuple) and len(result) == 2:
                success, data = result
                regional_results[region] = data
            else:
                regional_results[region] = {"status": "error", "message": str(result)}
        
        processing_time = time.time() - start_time
        logger.info(f"Booking {booking_id} cancelled in {processing_time:.3f}s")
        
        return {
            "booking_id": booking_id,
            "status": "cancelled",
            "results": regional_results
        }
        
    except Exception as e:
        logger.error(f"Error cancelling booking {booking_id}: {str(e)}")
        return {
            "booking_id": booking_id,
            "status": "error",
            "message": str(e)
        }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

