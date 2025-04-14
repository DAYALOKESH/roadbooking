from typing import List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from geoalchemy2.shape import to_shape
from shapely.geometry import LineString
import logging
from shapely.wkt import loads

# Update this import to match your actual models location


# If you're using a different import structure, adjust accordingly
from regional_manager.london.models import RoadSegment, BookingSegment

logger = logging.getLogger(__name__)


class SegmentService:
    def __init__(self, db: Session):
        self.db = db

    def convert_route_to_segments(self, coordinates: List[Tuple[float, float]]) -> List[str]:
        """
        Convert a route (list of coordinates) into a list of segment IDs.

        Args:
            coordinates: List of (longitude, latitude) tuples representing the route.

        Returns:
            List of segment IDs in the order they appear along the route.

        Raises:
            ValueError: If fewer than 2 coordinates are provided.
            Exception: If the database query fails.
        """
        if len(coordinates) < 2:
            raise ValueError("Route must have at least two coordinates")
        coordinates = [(lon, lat) for lat, lon in coordinates]
        segment_ids = []
        route_line = LineString(coordinates)
        route_wkt = route_line.wkt

        logger.debug(f"Route geometry (WKT): {route_wkt}")

        # Revised query: Use ST_DWithin and order by projection of segment midpoint
        query = text("""
            WITH route AS (
                SELECT ST_SetSRID(ST_GeomFromText(:route_wkt), 4326) AS geom
            ),
            candidates AS (
                SELECT 
                    rs.segment_id,
                    ST_LineLocatePoint(
                        route.geom,
                        ST_LineInterpolatePoint(rs.geom, 0.5)  -- Midpoint of the segment
                    ) AS fraction
                FROM road_segments rs, route
                WHERE ST_DWithin(rs.geom, route.geom, 0.0001)  -- Approx. 10 meters
            )
            SELECT segment_id
            FROM candidates
            ORDER BY fraction
        """)

        try:
            result = self.db.execute(query, {"route_wkt": route_wkt})

            for row in result:
                logger.debug(f"Matched Segment ID: {row.segment_id}")
                segment_ids.append(row.segment_id)

            if not segment_ids:
                logger.warning("No segments matched the given route")

        except Exception as e:
            logger.error(f"Error executing segment query: {str(e)}")
            raise

        return segment_ids

    def check_segments_capacity(self, segment_ids: List[str]) -> bool:
        """
        Check if all segments in the list have available capacity.
        """
        for segment_id in segment_ids:
            segment = self.db.query(RoadSegment).filter(RoadSegment.segment_id == segment_id).first()

            if not segment:
                logger.warning(f"Segment {segment_id} not found in database")
                return False

            if segment.current_load >= segment.capacity:
                logger.info(
                    f"Segment {segment_id} at capacity (current: {segment.current_load}, max: {segment.capacity})")
                return False

        return True

    def reserve_segments(self, booking_id: str, segment_ids: List[str]) -> None:
        """
        Reserve all segments for a given booking ID.
        """
        try:
            for i, segment_id in enumerate(segment_ids):
                segment = self.db.query(RoadSegment).filter(RoadSegment.segment_id == segment_id).first()

                if segment:
                    segment.current_load += 1

                    booking_segment = BookingSegment(
                        booking_id=booking_id,
                        segment_id=segment_id,
                        segment_order=i,
                        status="waiting"
                    )

                    self.db.add(booking_segment)

            self.db.commit()
        except Exception as e:
            logger.error(f"Error reserving segments: {str(e)}")
            self.db.rollback()
            raise

    def record_failed_segments(self, booking_id: str, segment_ids: List[str]) -> None:
        """
        Mark segments as failed for a given booking ID.
        """
        try:
            for i, segment_id in enumerate(segment_ids):
                booking_segment = BookingSegment(
                    booking_id=booking_id,
                    segment_id=segment_id,
                    segment_order=i,
                    status="failed"
                )
                self.db.add(booking_segment)

            self.db.commit()
        except Exception as e:
            logger.error(f"Error recording failed segments: {str(e)}")
            self.db.rollback()
            raise

    def confirm_booking(self, booking_id: str) -> None:
        """
        Confirm all segments for a booking ID by updating their status to 'success'.
        """
        try:
            segments = self.db.query(BookingSegment).filter(BookingSegment.booking_id == booking_id).all()
            for segment in segments:
                segment.status = "success"
            self.db.commit()
        except Exception as e:
            logger.error(f"Error confirming booking: {str(e)}")
            self.db.rollback()
            raise

    def cancel_booking(self, booking_id: str) -> None:
        """
        Cancel a booking and release all reserved segments.
        """
        try:
            # Find all booking segments for this booking ID
            booking_segments = self.db.query(BookingSegment).filter(
                BookingSegment.booking_id == booking_id
            ).all()

            if not booking_segments:
                return {
                    "status": "not_found",
                    "message": f"No segments found for booking {booking_id}",
                    "segments_cancelled": 0
                }

            segments_cancelled = 0
            segments_freed = 0

            # For each booking segment, decrease the current load of the corresponding road segment
            for booking_segment in booking_segments:
                # Get the corresponding road segment
                road_segment = self.db.query(RoadSegment).filter(
                    RoadSegment.segment_id == booking_segment.segment_id
                ).first()

                if road_segment:
                    # Only decrease load if the booking was in 'waiting' or 'success' status
                    if booking_segment.status in ['waiting', 'success']:
                        road_segment.current_load = max(0, road_segment.current_load - 1)
                        segments_freed += 1

                    # Update the booking segment status to cancelled
                    booking_segment.status = "cancelled"
                    segments_cancelled += 1

            self.db.commit()

            return {
                "status": "success",
                "message": f"Successfully cancelled booking {booking_id}",
                "segments_cancelled": segments_cancelled,
                "segments_freed": segments_freed
            }

        except Exception as e:
            logger.error(f"Error canceling booking: {str(e)}")
            self.db.rollback()
            raise

    def get_segments(self, booking_id: str) -> dict:
        """
        Get all segments associated with a booking ID along with their current load information.
        """
        try:
            # Query all booking segments for this booking ID
            booking_segments = self.db.query(BookingSegment).filter(BookingSegment.booking_id == booking_id).all()

            # Prepare the result data
            result = {
                "booking_id": booking_id,
                "segments": []
            }
            if not booking_segments:
                return result

            # For each booking segment, get the road segment information
            for booking_segment in booking_segments:
                road_segment = self.db.query(RoadSegment).filter(
                    RoadSegment.segment_id == booking_segment.segment_id
                ).first()

                if road_segment:
                    # Convert geometry to WKT format for readability
                    shape = to_shape(road_segment.geom)
                    if shape.geom_type == 'LineString':
                        coordinates = [[p[0], p[1]] for p in shape.coords]
                    else:
                        coordinates = []



                    segment_info = {
                        "segment_id": booking_segment.segment_id,
                        "segment_order": booking_segment.segment_order,
                        "status": booking_segment.status,
                        "current_load": road_segment.current_load,
                        "capacity": road_segment.capacity,
                        "coordinates": coordinates,
                        "name": road_segment.name or "Unnamed Road",
                        "osm_id": road_segment.osm_id
                    }
                    result["segments"].append(segment_info)

            # Sort segments by their order

            result["segments"].sort(key=lambda x: x["segment_order"])
            print(result)

            return result

        except Exception as e:
            logger.error(f"Error getting segments: {str(e)}")
        raise