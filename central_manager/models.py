from pydantic import BaseModel
from sqlalchemy import Column, String
from database import Base

class UserRequest(BaseModel):
    name: str
    email: str
    start_coordinates: str
    destination_coordinates: str
    start_time: str

class BookingInfo(Base):
    __tablename__ = 'booking_info'

    booking_id = Column(String, primary_key=True, index=True)
    start_location = Column(String)
    end_location = Column(String)
    region = Column(String)
    status = Column(String)