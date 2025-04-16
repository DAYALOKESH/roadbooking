from fastapi import FastAPI, HTTPException
import httpx
import os
from typing import Dict, Any, List, Tuple
import uuid
import polyline
import uvicorn
import asyncio
from database import SessionLocal
from models import UserRequest, BookingInfo
from database import Base, engine
from sqlalchemy.exc import IntegrityError
import logging

from fastapi.middleware.cors import CORSMiddleware


origins = [
    "*",  # or whatever your frontend address is
]



Base.metadata.create_all(bind=engine)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = FastAPI(title="Multi-region service Manger")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REGION_ENDPOINTS = {
    "ireland": os.getenv("DUBLIN_ENDPOINT", "http://localhost:8001"),
    "london": os.getenv("LONDON_ENDPOINT", "http://localhost:8002"),
    "australia": os.getenv("AUSTRALIA_ENDPOINT", "http://localhost:8003"),
    "america": os.getenv("AMERICA_ENDPOINT", "http://localhost:8004"),
}
region_boundaries = {
    "ireland": {
        "min_latitude": 51.4,
        "max_latitude": 55.4,
        "min_longitude": -10.7,
        "max_longitude": -5.4
    },
    "london": {
        "min_latitude": 49.9,
        "max_latitude": 60.9,
        "min_longitude": -8.6,
        "max_longitude": 1.8
    }
}

@app.post("/send_request")
async def get_info(user_request: UserRequest):
    # Extract user request details
    name = user_request.name
    email = user_request.email
    start_coordinates = user_request.start_coordinates
    destination_coordinates = user_request.destination_coordinates
    start_time = user_request.start_time
    booking_id = str(uuid.uuid4())

    start_latitude, start_longitude = start_coordinates.split(",")
    dest_latitude, dest_longitude = destination_coordinates.split(",")

    # Get the path
    path = await fetch_route(start_longitude, start_latitude, dest_longitude, dest_latitude)
    segments = segment_path(path, region_boundaries)

    involved_regions = set()
    tasks = []
    async with httpx.AsyncClient() as client:
        for segment_id, segment_info in segments.items():
            region = segment_info["region"]
            coordinates = segment_info["coordinates"]
            region_endpoint = REGION_ENDPOINTS.get(region)

            if region_endpoint:
                involved_regions.add(region)
                tasks.append(
                    client.post(
                        f"{region_endpoint}/process_segment",
                        json={
                            "booking_id": booking_id,
                            "coordinates": coordinates,
                            "name": name,
                            "email": email,
                            "start_time": start_time
                        },
                        timeout=10000.0
                    )
                )

        # Execute all tasks concurrently and wait for their completion
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    # Log responses for debugging
    for i, response in enumerate(responses):
        if isinstance(response, Exception):
            logger.error(f"Error in response {i}: {str(response)}")
        else:
            logger.debug(f"Response {i} status: {response.status_code}, content: {response.text}")

    # Check if all responses were successful
    all_success = all(not isinstance(response, Exception) and response.status_code == 200 for response in responses)
    
    # Create a list to hold confirmation/cancellation tasks
    confirmation_tasks = []
    
    async with httpx.AsyncClient() as client:
        for region in involved_regions:
            region_endpoint = REGION_ENDPOINTS.get(region)
            if region_endpoint:
                if all_success:
                    confirmation_tasks.append(
                        client.post(
                            f"{region_endpoint}/confirm_booking", 
                            json={"booking_id": booking_id},
                            timeout=1000.0
                        )
                    )
                else:
                    confirmation_tasks.append(
                        client.post(
                            f"{region_endpoint}/cancel_booking", 
                            json={"booking_id": booking_id},
                            timeout=120.0
                        )
                    )
        
        # Wait for all confirmation/cancellation requests to complete
        confirmation_responses = await asyncio.gather(*confirmation_tasks, return_exceptions=True)
        
        # Log confirmation/cancellation responses
        for i, response in enumerate(confirmation_responses):
            if isinstance(response, Exception):
                logger.error(f"Error in confirmation/cancellation {i}: {str(response)}")
            else:
                logger.debug(f"Confirmation/cancellation {i} status: {response.status_code}")

    db = SessionLocal()
    try:
        # Concatenate regions into a single string
        region_string = ",".join(involved_regions)
        status = "success" if all_success else "failure"

        # Store a single booking info entry
        booking_info = BookingInfo(
            booking_id=booking_id,
            start_location=start_coordinates,
            end_location=destination_coordinates,
            region=region_string,
            status=status
        )
        db.add(booking_info)
        db.commit()
    except IntegrityError as e:
        db.rollback()
        logger.error(f"IntegrityError: {str(e)}")
        raise HTTPException(status_code=400, detail="Duplicate booking ID detected. Please try again.")
    finally:
        db.close()

    results = {f"segment_{i + 1}": (response.text if not isinstance(response, Exception) else f"Error: {str(response)}")
               for i, response in enumerate(responses)}
    return {"booking_id": booking_id, "results": results}

async def fetch_route(start_longitude: float, start_latitude: float, dest_longitude: float, dest_latitude: float):
    # Construct the OSRM API URL
    osrm_url = (
        f"http://router.project-osrm.org/route/v1/driving/"
        f"{start_longitude},{start_latitude};"
        f"{dest_longitude},{dest_latitude}?overview=full"
    )
    print(f"Requesting OSRM URL: {osrm_url}")
    # Make an asynchronous request to the OSRM server
    async with httpx.AsyncClient() as client:
        response = await client.get(osrm_url)

    # Check if the request was successful
    if response.status_code == 200:
        route_data = response.json()
        print(f"OSRM Response: {route_data}")
        # Extract the path from the route data
        if 'routes' in route_data and len(route_data['routes']) > 0:
            return route_data['routes'][0]['geometry']
        else:
            raise HTTPException(status_code=404, detail="No route found")
    else:
        raise HTTPException(status_code=response.status_code, detail="Failed to fetch route from OSRM server")


def segment_path(path: str, boundaries: Dict[str, Dict[str, float]]) -> Dict[str, List[Tuple[float, float]]]:
    # Decode the polyline to get the list of coordinates
    coordinates = polyline.decode(path)

    segments = {}
    current_region = None
    current_segment = []

    for coord in coordinates:
        lat, lon = coord
        for region, bounds in boundaries.items():
            if (bounds['min_latitude'] <= lat <= bounds['max_latitude'] and
                    bounds['min_longitude'] <= lon <= bounds['max_longitude']):
                if current_region != region:
                    if current_region is not None:
                        # Save the current segment under the current region
                        segments[f"segment_{len(segments) + 1}"] = {"region": current_region,
                                                                    "coordinates": current_segment}
                    current_region = region
                    current_segment = []
                current_segment.append(coord)
                break

    if current_region is not None:
        segments[f"segment_{len(segments) + 1}"] = {"region": current_region, "coordinates": current_segment}

    return segments


@app.get("/booking_status/{booking_id}")
async def get_booking_status(booking_id: str):
    # we will get the booking id from the user and we will check the status of the booking and return status
    # we will check the status of the booking in the database
    db = SessionLocal()
    booking_info = db.query(BookingInfo).filter(BookingInfo.booking_id == booking_id).first()
    db.close()
    if booking_info:
        return {"booking_id": booking_id, "status": booking_info.status}
    else:
        raise HTTPException(status_code=404, detail="Booking not found")


@app.get("/get_segments/{booking_id}")
async def get_segments(booking_id: str):
    # Query the database for all booking entries with this booking ID
    db = SessionLocal()
    booking_infos = db.query(BookingInfo).filter(BookingInfo.booking_id == booking_id).first()
    db.close()

    if not booking_infos:
        raise HTTPException(status_code=404, detail="Booking not found")

    # Get unique regions associated with this booking
    regions = set(booking_infos.region.split(","))

    # Prepare tasks for querying each regional manager
    tasks = []
    region_responses = {}  # Map task index to region name

    async with httpx.AsyncClient() as client:
        for region in regions:
            region_endpoint = REGION_ENDPOINTS.get(region)
            if region_endpoint:
                # Add timeout to prevent hanging indefinitely
                tasks.append(
                    client.get(
                        f"{region_endpoint}/get_segments/{booking_id}",
                        timeout=30.0  # Add appropriate timeout value
                    )
                )
                region_responses[len(tasks) - 1] = region

        # Execute all tasks concurrently and wait for completion
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    # Process responses from all regions
    all_segments = {}
    all_successful = True

    for i, response in enumerate(responses):
        region = region_responses[i]
        if isinstance(response, Exception):
            all_successful = False
            all_segments[region] = {"error": str(response)}
            logger.error(f"Error getting segments from {region}: {str(response)}")
        elif response.status_code != 200:
            all_successful = False
            all_segments[region] = {"error": f"Status code: {response.status_code}"}
            logger.error(f"Error status from {region}: {response.status_code}")
        else:
            try:
                all_segments[region] = response.json()
                logger.debug(f"Successfully got segments from {region}")
            except Exception as e:
                all_successful = False
                all_segments[region] = {"error": f"Failed to parse response: {str(e)}"}
                logger.error(f"Failed to parse response from {region}: {str(e)}")

    logger.info(f"Completed get_segments request for booking {booking_id}")
    return {
        "booking_id": booking_id,
        "complete": all_successful,
        "segments": all_segments
    }


@app.post("/cancel_booking/{booking_id}")
async def cancel_booking(booking_id: str):
    # Get booking information from database
    db = SessionLocal()
    try:
        booking = db.query(BookingInfo).filter(BookingInfo.booking_id == booking_id).first()
        if not booking:
            raise HTTPException(status_code=404, detail="Booking not found")
        
        # Get regions involved in this booking
        regions = booking.region.split(",") if booking.region else []
        
        # Send cancellation requests to all involved regions
        cancellation_tasks = []
        async with httpx.AsyncClient() as client:
            for region in regions:
                region_endpoint = REGION_ENDPOINTS.get(region.strip())
                if region_endpoint:
                    cancellation_tasks.append(
                        client.post(
                            f"{region_endpoint}/cancel_booking",
                            json={"booking_id": booking_id},
                            timeout=120.0
                        )
                    )
            
            # Execute all cancellation tasks concurrently
            responses = await asyncio.gather(*cancellation_tasks, return_exceptions=True)
            
        # Update booking status in database
        booking.status = "cancelled"
        db.commit()
        
        # Log responses
        for i, response in enumerate(responses):
            if isinstance(response, Exception):
                logger.error(f"Error in cancellation response {i}: {str(response)}")
            else:
                logger.debug(f"Cancellation response {i} status: {response.status_code}")
                
        return {"status": "Booking cancelled successfully", "booking_id": booking_id}
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error cancelling booking: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error cancelling booking: {str(e)}")
    finally:
        db.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
