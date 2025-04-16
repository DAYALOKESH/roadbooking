from sqlalchemy import Column, String, create_engine, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()



# Database setup

DATABASE_URL = "postgresql://postgres:sustainablecitymanagement0@34.46.53.83:5432/osm"
# DATABASE_URL = "postgresql://postgres:psotgres@192.168.118.5:5434/london_road_capacity"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


