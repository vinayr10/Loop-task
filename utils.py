import pandas as pd
from database import SessionLocal, Store, BusinessHours, Timezone, Report
from datetime import datetime, timedelta
import pytz
import csv
import os
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import select
import asyncio

def load_csv_data():
    db = SessionLocal()
    
    try:
        # Load store status data
        df_status = pd.read_csv("data/store_status.csv")
        for _, row in df_status.iterrows():
            stmt = insert(Store).values(id=row['store_id'], timestamp_utc=pd.to_datetime(row['timestamp_utc']), status=row['status'])
            stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_=dict(timestamp_utc=stmt.excluded.timestamp_utc, status=stmt.excluded.status)
            )
            db.execute(stmt)
        
        # Load business hours data
        df_hours = pd.read_csv("data/business_hours.csv")
        for _, row in df_hours.iterrows():
            stmt = insert(BusinessHours).values(
                store_id=row['store_id'],
                day_of_week=row['day'],
                start_time_local=row['start_time_local'],
                end_time_local=row['end_time_local']
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['store_id', 'day_of_week'],
                set_=dict(start_time_local=stmt.excluded.start_time_local, end_time_local=stmt.excluded.end_time_local)
            )
            db.execute(stmt)
        
        # Load timezone data
        df_timezone = pd.read_csv("data/timezones.csv")
        for _, row in df_timezone.iterrows():
            stmt = insert(Timezone).values(store_id=row['store_id'], timezone_str=row['timezone_str'])
            stmt = stmt.on_conflict_do_update(
                index_elements=['store_id'],
                set_=dict(timezone_str=stmt.excluded.timezone_str)
            )
            db.execute(stmt)
        
        db.commit()
        print("CSV data loaded successfully")
    except Exception as e:
        print(f"Error loading CSV data: {e}")
        db.rollback()
    finally:
        db.close()

async def generate_report(report_id):
    db = SessionLocal()
    
    try:
        report = db.query(Report).filter(Report.id == report_id).first()
        if not report:
            print(f"Report {report_id} not found in the database")
            return

        # Get the latest timestamp from the store status data
        max_timestamp = db.query(Store.timestamp_utc).order_by(Store.timestamp_utc.desc()).first()[0]
        
        # Calculate uptime and downtime for each store
        stores = db.query(Store.id).distinct().all()
        results = []
        
        for store in stores:
            store_id = store[0]
            timezone = db.query(Timezone).filter(Timezone.store_id == store_id).first()
            tz = pytz.timezone(timezone.timezone_str if timezone else 'America/Chicago')
            
            uptime_last_hour, downtime_last_hour = await calculate_uptime_downtime(db, store_id, max_timestamp - timedelta(hours=1), max_timestamp, tz)
            uptime_last_day, downtime_last_day = await calculate_uptime_downtime(db, store_id, max_timestamp - timedelta(days=1), max_timestamp, tz)
            uptime_last_week, downtime_last_week = await calculate_uptime_downtime(db, store_id, max_timestamp - timedelta(weeks=1), max_timestamp, tz)
            
            results.append({
                'store_id': store_id,
                'uptime_last_hour': round(uptime_last_hour, 2),
                'uptime_last_day': round(uptime_last_day, 2),
                'uptime_last_week': round(uptime_last_week, 2),
                'downtime_last_hour': round(downtime_last_hour, 2),
                'downtime_last_day': round(downtime_last_day, 2),
                'downtime_last_week': round(downtime_last_week, 2)
            })

        # Generate CSV file
        os.makedirs('reports', exist_ok=True)
        file_path = f"reports/{report_id}.csv"
        with open(file_path, 'w', newline='') as csvfile:
            fieldnames = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week',
                          'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow(row)

        # Update report status
        report.status = "Complete"
        db.commit()
        print(f"Report {report_id} generated successfully")
    except Exception as e:
        print(f"Error generating report {report_id}: {e}")
        report.status = "Error"
        db.commit()
    finally:
        db.close()

async def calculate_uptime_downtime(db, store_id, start_time, end_time, tz):
    # Get business hours for the store
    business_hours = db.query(BusinessHours).filter(BusinessHours.store_id == store_id).all()
    
    # If no business hours data, assume 24/7
    if not business_hours:
        business_hours = [BusinessHours(store_id=store_id, day_of_week=i, start_time_local='00:00:00', end_time_local='23:59:59') for i in range(7)]
    
    # Get store status data for the given time range
    status_data = db.query(Store).filter(Store.id == store_id, Store.timestamp_utc.between(start_time, end_time)).order_by(Store.timestamp_utc).all()
    
    uptime = timedelta()
    downtime = timedelta()
    last_status = None
    last_timestamp = None
    
    for status in status_data:
        local_time = status.timestamp_utc.replace(tzinfo=pytz.UTC).astimezone(tz)
        day_of_week = local_time.weekday()
        
        # Check if the current time is within business hours
        is_business_hours = any(
            bh.day_of_week == day_of_week and
            datetime.strptime(bh.start_time_local, '%H:%M:%S').time() <= local_time.time() <= datetime.strptime(bh.end_time_local, '%H:%M:%S').time()
            for bh in business_hours
        )
        
        if is_business_hours:
            if last_status:
                duration = status.timestamp_utc - last_timestamp
                if last_status == 'active':
                    uptime += duration
                else:
                    downtime += duration
            
            last_status = status.status
            last_timestamp = status.timestamp_utc
    
    # Handle the last interval
    if last_status:
        duration = end_time - last_timestamp
        if last_status == 'active':
            uptime += duration
        else:
            downtime += duration
    
    # Convert to hours
    uptime_hours = uptime.total_seconds() / 3600
    downtime_hours = downtime.total_seconds() / 3600
    
    return uptime_hours, downtime_hours
