from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import os

# SQLALCHEMY_DATABASE_URL = os.getenv("DB_URL", "postgresql://megelis:megelis@10.128.190.52:5433/megelis")
SQLALCHEMY_DATABASE_URL = os.getenv("DB_URL", "postgresql://postgres:123@localhost/megelis")

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_old_db():
    engine_for_old = create_engine(os.getenv("OLD_DB_URL", "postgresql://postgres:123@localhost/MegelisTest"))
    # engine_for_old = create_engine(os.getenv("OLD_DB_URL", "postgresql://postgres:secret@10.128.190.133/postgres"))
    session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine_for_old)
    old_db = session_local()
    return old_db
