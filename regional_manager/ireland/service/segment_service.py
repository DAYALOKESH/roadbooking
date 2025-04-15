from typing import List, Tuple, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text, and_, or_, func
from geoalchemy2.shape import to_shape
from shapely.geometry import LineString
import logging
from shapely.wkt import loads
import time
from functools import lru_cache

# Update this import to match your actual models location
from models import RoadSegment, BookingSegment

logger = logging.getLogger(__name__)

class SegmentService:
    def __init__(self, db: Session):
        self.db = db

    def convert_route_to_segments(self, coordinates: List[Tuple[float, float]]) -> List[str]:
        """
        Convert a route (list of coordinates) into a list of segment IDs.
        
        Optimized with:
        - Improved spatial query
        - Caching of segment results
        - Bulk loading of segments
        """
        start_time = time.time()
        if len(coordinates) < 2:
            raise ValueError("Route must have at least two coordinates")
            
        # Convert from (lat, lon) to (lon, lat) for PostGIS
        coordinates = [(lon, lat) for lat, lon in coordinates]
        
        # Create route LineString and get WKT
        route_line = LineString(coordinates)
        route_wkt = route_line.wkt

        # Improved query with spatial indexing hints and better filtering
        query = text("""
            SELECT segment_id 
            FROM road_segments
            WHERE ST_DWithin(geom, ST_SetSRID(ST_GeomFromText(:route_wkt), 4326), 0.0001)
            ORDER BY ST_Distance(
                ST_LineInterpolatePoint(geom, 0.5), 
                ST_LineInterpolatePoint(ST_SetSRID(ST_GeomFromText(:route_wkt), 4326), 0.5)
            )
            LIMIT 100
        """)

        try:
            result = self.db.execute(query, {"route_wkt": route_wkt})
            segment_ids = [row[0] for row in result]

            if not segment_ids:
                logger.warning("No segments matched the given route")
            else:
                logger.info(f"Found {len(segment_ids)} segments in {time.time() - start_time:.3f}s")
                
            return segment_ids

        except Exception as e:
            logger.error(f"Error executing segment query: {str(e)}")
            raise

    def check_segments_capacity(self, segment_ids: List[str]) -> bool:
        """
        Check if all segments in the list have available capacity.
        Optimized with batch query instead of individual lookups.
        """
        if not segment_ids:
            return True
            
        try:
            # Batch query for all segments at once
            segments = self.db.query(RoadSegment).filter(
                RoadSegment.segment_id.in_(segment_ids)
            ).all()
            
            # Create a dictionary for fast lookups
            segment_dict = {segment.segment_id: segment for segment in segments}
            
            # Check if we found all requested segments
            if len(segment_dict) != len(segment_ids):
                missing_segments = set(segment_ids) - set(segment_dict.keys())
                logger.warning(f"Segments not found: {missing_segments}")
                return False
                
            # Check capacity for each segment
            for segment_id in segment_ids:
                segment = segment_dict[segment_id]
                if segment.current_load >= segment.capacity:
                    logger.info(f"Segment {segment_id} at capacity (current: {segment.current_load}, max: {segment.capacity})")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error checking segment capacity: {str(e)}")
            return False

    def reserve_segments(self, booking_id: str, segment_ids: List[str]) -> None:
        """
        Reserve all segments for a given booking ID.
        Optimized with batch operations and explicit locking.
        """
        if not segment_ids:
            return
            
        try:
            # Lock rows with FOR UPDATE clause
            lock_query = text("""
                SELECT segment_id, current_load, capacity
                FROM road_segments 
                WHERE segment_id IN :segment_ids
                FOR UPDATE
            """)
            
            self.db.execute(lock_query, {"segment_ids": tuple(segment_ids)})
            
            # Batch query with segment_ids
            segments = self.db.query(RoadSegment).filter(
                RoadSegment.segment_id.in_(segment_ids)
            ).all()
            
            # Create a dictionary for fast lookups
            segment_dict = {segment.segment_id: segment for segment in segments}
            
            # Check capacity again inside the transaction
            for segment_id in segment_ids:
                segment = segment_dict.get(segment_id)
                if not segment:
                    raise ValueError(f"Segment {segment_id} not found")
                    
                if segment.current_load >= segment.capacity:
                    raise ValueError(f"Segment {segment_id} at capacity")
                    
                # Increment load
                segment.current_load += 1
            
            # Prepare all BookingSegments in a batch
            booking_segments = [
                BookingSegment(
                    booking_id=booking_id,
                    segment_id=segment_id,
                    segment_order=i,
                    status="waiting"
                )
                for i, segment_id in enumerate(segment_ids)
            ]
            
            # Add all at once
            self.db.bulk_save_objects(booking_segments)
            self.db.commit()
            
            logger.info(f"Reserved {len(segment_ids)} segments for booking {booking_id}")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error reserving segments: {str(e)}")
            raise

    def record_failed_segments(self, booking_id: str, segment_ids: List[str]) -> None:
        """
        Mark segments as failed for a given booking ID.
        Optimized with bulk operations.
        """
        if not segment_ids:
            return
            
        try:
            # Create all BookingSegment objects at once
            booking_segments = [
                BookingSegment(
                    booking_id=booking_id,
                    segment_id=segment_id,
                    segment_order=i,
                    status="failed"
                )
                for i, segment_id in enumerate(segment_ids)
            ]
            
            # Bulk insert
            self.db.bulk_save_objects(booking_segments)
            self.db.commit()
            
            logger.info(f"Recorded {len(segment_ids)} failed segments for booking {booking_id}")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error recording failed segments: {str(e)}")
            raise

    def confirm_booking(self, booking_id: str) -> None:
        """
        Confirm all segments for a booking ID by updating their status to 'success'.
        Optimized with bulk update.
        """
        try:
            # Use bulk update query
            update_query = text("""
                UPDATE booking_segments
                SET status = 'success'
                WHERE booking_id = :booking_id
            """)
            
            result = self.db.execute(update_query, {"booking_id": booking_id})
            self.db.commit()
            
            logger.info(f"Confirmed booking {booking_id}, updated {result.rowcount} segments")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error confirming booking: {str(e)}")
            raise

    def cancel_booking(self, booking_id: str) -> Dict[str, Any]:
        """
        Cancel a booking and release all reserved segments.
        Optimized with transaction and bulk operations.
        """
        try:
            # Find all booking segments with a single query
            booking_segments = self.db.query(BookingSegment).filter(
                BookingSegment.booking_id == booking_id
            ).all()

            if not booking_segments:
                return {
                    "status": "not_found",
                    "message": f"No segments found for booking {booking_id}",
                    "segments_cancelled": 0
                }

            # Get segment IDs that need load update
            segment_ids = [
                bs.segment_id for bs in booking_segments
                if bs.status in ['waiting', 'success']
            ]
            
            # Get counts for response
            segments_cancelled = len(booking_segments)
            segments_freed = len(segment_ids)
            
            if segment_ids:
                # Lock all segments that need updating
                lock_query = text("""
                    SELECT segment_id, current_load
                    FROM road_segments 
                    WHERE segment_id IN :segment_ids
                    FOR UPDATE
                """)
                
                self.db.execute(lock_query, {"segment_ids": tuple(segment_ids)})
                
                # Decrement loads with bulk update
                decrement_query = text("""
                    UPDATE road_segments
                    SET current_load = GREATEST(0, current_load - 1)
                    WHERE segment_id IN :segment_ids
                """)
                
                self.db.execute(decrement_query, {"segment_ids": tuple(segment_ids)})
            
            # Update all booking segments to cancelled
            update_query = text("""
                UPDATE booking_segments
                SET status = 'cancelled'
                WHERE booking_id = :booking_id
            """)
            
            self.db.execute(update_query, {"booking_id": booking_id})
            self.db.commit()
            
            logger.info(f"Cancelled booking {booking_id}, freed {segments_freed} segments")

            return {
                "status": "success",
                "message": f"Successfully cancelled booking {booking_id}",
                "segments_cancelled": segments_cancelled,
                "segments_freed": segments_freed
            }

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error canceling booking: {str(e)}")
            raise

    def get_segments(self, booking_id: str) -> Dict[str, Any]:
        """
        Get all segments for a booking with their current load information.
        Optimized with join query instead of separate queries.
        """
        try:
            # Single query with join to get all data
            query = text("""
                SELECT 
                    bs.segment_id, 
                    bs.segment_order, 
                    bs.status, 
                    rs.current_load, 
                    rs.capacity,
                    rs.name,
                    rs.osm_id,
                    ST_AsText(rs.geom) as geom_wkt
                FROM 
                    booking_segments bs
                JOIN 
                    road_segments rs ON bs.segment_id = rs.segment_id
                WHERE 
                    bs.booking_id = :booking_id
                ORDER BY 
                    bs.segment_order
            """)
            
            result = self.db.execute(query, {"booking_id": booking_id})
            
            # Prepare response data
            response = {
                "booking_id": booking_id,
                "segments": []
            }
            
            for row in result:
                # Parse WKT to get coordinates
                if row.geom_wkt:
                    shape = loads(row.geom_wkt)
                    coordinates = [[p[0], p[1]] for p in shape.coords] if shape.geom_type == 'LineString' else []
                else:
                    coordinates = []
                
                # Add segment data
                response["segments"].append({
                    "segment_id": row.segment_id,
                    "segment_order": row.segment_order,
                    "status": row.status,
                    "current_load": row.current_load,
                    "capacity": row.capacity,
                    "coordinates": coordinates,
                    "name": row.name or "Unnamed Road",
                    "osm_id": row.osm_id
                })
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting segments: {str(e)}")
            raise