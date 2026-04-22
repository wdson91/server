import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL must be set in .env")

# Clean DATABASE_URL for SQLAlchemy/psycopg2 (remove Prisma specific params like ?schema=public)
if "schema=" in DATABASE_URL:
    import re
    DATABASE_URL = re.sub(r'([?&])schema=[^&]+', r'\1', DATABASE_URL)
    DATABASE_URL = DATABASE_URL.rstrip('?&')

# Create SQLAlchemy engine
# pool_pre_ping=True helps handle dropped connections
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Create a session factory
session_factory = sessionmaker(bind=engine)

# Scoped session for thread-safety (important for Flask and Celery)
Session = scoped_session(session_factory)

def get_session():
    """Returns a new session instance"""
    return Session()
