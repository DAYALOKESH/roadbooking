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

Base.metadata.create_all(bind=engine)
app = FastAPI(title="Multi-region service Manger")

REGION_ENDPOINTS = {
    "ireland": os.getenv("EUROPE_ENDPOINT", "http://localhost:8001"),
    "london": os.getenv("ASIA_ENDPOINT", "http://localhost:8002"),
    "australia": os.getenv("AUSTRALIA_ENDPOINT", "http://localhost:8003"),
    "america": os.getenv("AMERICA_ENDPOINT", "http://localhost:8004"),
}
region_boundaries = {
        "ireland": {
            "min_latitude": 51.0,
            "max_latitude": 55.5,
            "min_longitude": -10.5,
            "max_longitude": -5.5
        },
        "london": {
            "min_latitude": 51.28,
            "max_latitude": 51.686,
            "min_longitude": -0.510,
            "max_longitude": 0.334
        }
        # Add other regions as needed
    }

# user will give me the Name, Email, Start Coordinates and Destination Cordinates and start time
# pydantic model for the request body



# route endpoint for user to make request to send the info
@app.post("/send_request")
async def get_info(user_request: UserRequest):
    #user will give me the Name, Email, Start Coordinates and Destination Cordinates and start time
    name = user_request.name
    email = user_request.email
    start_coordinates = user_request.start_coordinates
    destination_coordinates = user_request.destination_coordinates
    start_time = user_request.start_time
    booking_id = str(uuid.uuid4())

    start_latitude, start_longitude = start_coordinates.split(",")
    dest_latitude, dest_longitude = destination_coordinates.split(",")

    # get the path
    path = await fetch_route(start_longitude, start_latitude, dest_longitude, dest_latitude)

    segments = segment_path(path, region_boundaries)
    # Prepare tasks for sending segments to regional managers
    tasks = []
    async with httpx.AsyncClient() as client:
        for segment_id, segment_info in segments.items():
            region = segment_info["region"]
            coordinates = segment_info["coordinates"]
            region_endpoint = REGION_ENDPOINTS.get(region)

            if region_endpoint:
                tasks.append(
                    client.post(
                        f"{region_endpoint}/process_segment",
                        json={
                            "booking_id": booking_id,
                            "coordinates": coordinates,
                            "name": name,
                            "email": email,
                            "start_time": start_time
                        }
                    )
                )

        # Execute all tasks concurrently and wait for their completion
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    # Check if all responses were successful
    all_success = all(not isinstance(response, Exception) and response.status_code == 200 for response in responses)
    async with httpx.AsyncClient() as client:
        if all_success:
            for region_endpoint in REGION_ENDPOINTS.values():
                await client.post(f"{region_endpoint}/confirm_booking", json={"booking_id": booking_id})
        else:
            for region_endpoint in REGION_ENDPOINTS.values():
                await client.post(f"{region_endpoint}/cancel_booking", json={"booking_id": booking_id})

    db = SessionLocal()
    for segment_id, segment_info in segments.items():
        region = segment_info["region"]
        status = "success" if all_success else "failure"
        booking_info = BookingInfo(
            booking_id=booking_id,
            start_location=start_coordinates,
            end_location=destination_coordinates,
            region=region,
            status=status
        )
        db.add(booking_info)
    db.commit()
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

    # Make an asynchronous request to the OSRM server
    async with httpx.AsyncClient() as client:
        response = await client.get(osrm_url)

    # Check if the request was successful
    if response.status_code == 200:
        route_data = response.json()
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
                        segments[f"segment_{len(segments) + 1}"] = {"region": current_region, "coordinates": current_segment}
                    current_region = region
                    current_segment = []
                current_segment.append(coord)
                break

    if current_region is not None:
        segments[f"segment_{len(segments) + 1}"] = {"region": current_region, "coordinates": current_segment}

    return segments

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

