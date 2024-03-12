import datetime
from sqlalchemy import Column, DateTime, Float, Integer, String
from ..database import Base

class Sensor(Base):
    __tablename__ = "sensors"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    joined_at = Column(DateTime, default=datetime.datetime.utcnow)
    type = Column(String, default="Dummy")
    mac_address = Column(String,unique=True, index=True)
    manufacturer = Column(String)
    model = Column(String)
    serie_number = Column(String)
    firmware_version = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)