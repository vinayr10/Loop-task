from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

SQLALCHEMY_DATABASE_URL = "sqlite:///./store_monitoring.db"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Store(Base):
    __tablename__ = "stores"

    id = Column(Integer, primary_key=True, index=True)
    timestamp_utc = Column(DateTime)
    status = Column(String)

class BusinessHours(Base):
    __tablename__ = "business_hours"

    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, index=True)
    day_of_week = Column(Integer)
    start_time_local = Column(String)
    end_time_local = Column(String)

class Timezone(Base):
    __tablename__ = "timezones"

    store_id = Column(Integer, primary_key=True, index=True)
    timezone_str = Column(String)

class Report(Base):
    __tablename__ = "reports"

    id = Column(String, primary_key=True)
    status = Column(String)
