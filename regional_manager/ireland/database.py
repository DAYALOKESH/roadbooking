from sqlalchemy import Column, String, create_engine, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()



# Database setup
DATABASE_URL = "postgresql://postgres:12345678@35.189.66.196:5432/osm"
# DATABASE_URL = "postgresql://postgres:psotgres@192.168.118.5:5433/road_capacity"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


