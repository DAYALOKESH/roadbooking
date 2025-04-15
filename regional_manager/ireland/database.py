from sqlalchemy import Column, String, create_engine, Integer, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()

# Database setup with optimized connection pool
DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/road_capacity"
engine = create_engine(
    DATABASE_URL,
    pool_size=20,               # Increased pool size for concurrent requests
    max_overflow=10,            # Allow up to 10 additional connections
    pool_timeout=30,            # Wait up to 30 seconds for a connection
    pool_recycle=1800,          # Recycle connections after 30 minutes
    pool_pre_ping=True,         # Check connection health before use
    connect_args={
        "options": "-c statement_timeout=30000 -c lock_timeout=10000"  # PostgreSQL timeouts
    }
)

# Configure session with optimized settings
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Set isolation level to READ COMMITTED for better concurrency
@event.listens_for(engine, "connect")
def set_isolation_level(dbapi_connection, connection_record):
    dbapi_connection.set_session(isolation_level=2)  # READ COMMITTED

@contextmanager
def get_db():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        session.close()


