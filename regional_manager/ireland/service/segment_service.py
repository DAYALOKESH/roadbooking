from typing import List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from geoalchemy2.shape import from_shape
from shapely.geometry import LineString
from regional_manager.ireland.models import RoadSegment, BookingSegment
import logging

logger = logging.getLogger(__name__)

class SegmentService:
    def __init__(self, db: Session):
        self.db = db

    def convert_route_to_segments(self, coordinates: List[Tuple[float, float]]) -> List[str]:
        if len(coordinates) < 2:
            raise ValueError("Route must have at least two coordinates")

        segment_ids = []
        route_line = LineString(coordinates)
        route_geom = from_shape(route_line, srid=4326)

        query = text("""
            WITH route AS (
                SELECT ST_Transform(ST_SetSRID(ST_GeomFromText(:route_wkt), 4326), 4326) AS geom
            )
            SELECT 
                rs.segment_id
            FROM road_segments rs, route
            WHERE ST_Intersects(rs.geom, route.geom)
            ORDER BY ST_LineLocatePoint(route.geom, ST_StartPoint(ST_Intersection(rs.geom, route.geom)))
        """)

        result = self.db.execute(query, {"route_wkt": route_line.wkt})

        for row in result:
            segment_ids.append(row.segment_id)

        return segment_ids

    def check_segments_capacity(self, segment_ids: List[str]) -> bool:
        for segment_id in segment_ids:
            segment = self.db.query(RoadSegment).filter(RoadSegment.segment_id == segment_id).first()

            if not segment:
                logger.warning(f"Segment {segment_id} not found in database")
                return False

            if segment.current_load >= segment.capacity:
                logger.info(f"Segment {segment_id} at capacity (current: {segment.current_load}, max: {segment.capacity})")
                return False

        return True

    def reserve_segments(self, booking_id: str, segment_ids: List[str]) -> None:
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

    def record_failed_segments(self, booking_id: str, segment_ids: List[str]) -> None:
        for i, segment_id in enumerate(segment_ids):
            booking_segment = BookingSegment(
                booking_id=booking_id,
                segment_id=segment_id,
                segment_order=i,
                status="failed"
            )
            self.db.add(booking_segment)

        self.db.commit()

    def confirm_booking(self, booking_id: str) -> None:
        segments = self.db.query(BookingSegment).filter(BookingSegment.booking_id == booking_id).all()
        for segment in segments:
            segment.status = "success"
        self.db.commit()

    def cancel_booking(self, booking_id: str) -> None:
        segments = self.db.query(BookingSegment).filter(BookingSegment.booking_id == booking_id).all()
        for segment in segments:
            if segment.status == "waiting":
                road_segment = self.db.query(RoadSegment).filter(RoadSegment.segment_id == segment.segment_id).first()
                if road_segment:
                    road_segment.current_load = max(road_segment.current_load - 1, 0)
            segment.status = "failed"
        self.db.commit()
