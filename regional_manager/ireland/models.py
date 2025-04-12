from pydantic import BaseModel
from typing import List, Tuple
from sqlalchemy import Column, String, Integer, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from database import Base

class SegmentRequest(BaseModel):
    booking_id: str
    coordinates: List[Tuple[float, float]]
    name: str
    email: str
    start_time: str

class RoadSegment(Base):
    __tablename__ = "road_segments"
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True, autoincrement=True)
    segment_id = Column(String, unique=True, nullable=False)
    geom = Column(Geometry('LINESTRING', srid=4326), nullable=False)
    capacity = Column(Integer, default=100, nullable=False)
    current_load = Column(Integer, default=0, nullable=False)
    osm_id = Column(BigInteger)
    name = Column(String)

    # Define relationships if needed
    booking_segments = relationship("BookingSegment", back_populates="road_segment")

class RegionalSegment(Base):
    __tablename__ = 'regional_segments'
    __table_args__ = {'extend_existing': True}  # Add this line
    id = Column(Integer, primary_key=True, index=True)
    booking_id = Column(String, index=True)
    coordinates = Column(String)  # Store coordinates as a string
    name = Column(String)
    email = Column(String)
    start_time = Column(String)

class BookingSegment(Base):
    __tablename__ = 'booking_segments'
    __table_args__ = {'extend_existing': True}  # Add this line
    id = Column(Integer, primary_key=True, index=True)
    booking_id = Column(String, index=True)
    segment_id = Column(String, ForeignKey('road_segments.segment_id'), index=True)
    segment_order = Column(Integer)
    status = Column(String)  # Status can be 'waiting', 'success', or 'failed'

    # Define relationships if needed
    road_segment = relationship("RoadSegment", back_populates="booking_segments")
