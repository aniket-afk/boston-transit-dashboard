import os
import json
import logging
import pandas as pd
import boto3
from sqlalchemy import create_engine, text

# --------------------------
# LOGGING CONFIGURATION
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# --------------------------
# ENVIRONMENT VARIABLE LOADING
# --------------------------
# Local: pip install python-dotenv, and add a .env file (ignored in git)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Fetch secrets (works for GitHub Actions secrets and .env files)
AWS_BUCKET = os.environ['AWS_BUCKET']
REGION = os.environ.get('AWS_REGION', 'us-east-1')
RDS_USER = os.environ['RDS_USER']
RDS_PASS = os.environ['RDS_PASS']
RDS_HOST = os.environ['RDS_HOST']
RDS_PORT = os.environ.get('RDS_PORT', '5432')
RDS_DB = os.environ['RDS_DB']

# --------------------------
# CONNECTION SETUP
# --------------------------
engine = create_engine(f'postgresql://{RDS_USER}:{RDS_PASS}@{RDS_HOST}:{RDS_PORT}/{RDS_DB}')
s3 = boto3.client('s3', region_name=REGION)

# --------------------------
# FLATTEN FUNCTIONS
# --------------------------
def flatten_trip_updates(json_data):
    rows = []
    for entity in json_data.get('entity', []):
        trip_update = entity.get('trip_update', {})
        trip = trip_update.get('trip', {})
        route_id = trip.get('route_id')
        trip_id = trip.get('trip_id')
        direction_id = trip.get('direction_id')
        start_time = trip.get('start_time')
        start_date = trip.get('start_date')
        timestamp = trip_update.get('timestamp')
        for stu in trip_update.get('stop_time_update', []):
            stop_sequence = stu.get('stop_sequence')
            stop_id = stu.get('stop_id')
            arrival_time = stu.get('arrival', {}).get('time')
            departure_time = stu.get('departure', {}).get('time')
            rows.append({
                'trip_id': trip_id,
                'route_id': route_id,
                'direction_id': direction_id,
                'start_time': start_time,
                'start_date': start_date,
                'timestamp': timestamp,
                'stop_sequence': stop_sequence,
                'stop_id': stop_id,
                'arrival_time': arrival_time,
                'departure_time': departure_time
            })
    df = pd.DataFrame(rows)
    for col in ['arrival_time', 'departure_time', 'timestamp']:
        df[col] = pd.to_datetime(df[col], unit='s', errors='coerce')
    return df

def flatten_vehicle_positions(json_data):
    rows = []
    for entity in json_data.get('entity', []):
        vp = entity.get('vehicle', {})
        trip = vp.get('trip', {})
        position = vp.get('position', {})
        vehicle = vp.get('vehicle', {})
        rows.append({
            'vehicle_id': vehicle.get('id'),
            'route_id': trip.get('route_id'),
            'trip_id': trip.get('trip_id'),
            'latitude': position.get('latitude'),
            'longitude': position.get('longitude'),
            'bearing': position.get('bearing'),
            'speed': position.get('speed'),
            'timestamp': vp.get('timestamp')
        })
    df = pd.DataFrame(rows)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
    return df

def flatten_alerts(json_data):
    rows = []
    for entity in json_data.get('entity', []):
        alert = entity.get('alert', {})
        rows.append({
            'alert_id': entity.get('id'),
            'cause': alert.get('cause'),
            'effect': alert.get('effect'),
            'header_text': alert.get('header_text', {}).get('translation', [{}])[0].get('text'),
            'description_text': alert.get('description_text', {}).get('translation', [{}])[0].get('text'),
            'severity': alert.get('severity_level'),
            'start': alert.get('active_period', [{}])[0].get('start'),
            'end': alert.get('active_period', [{}])[0].get('end')
        })
    df = pd.DataFrame(rows)
    df['start'] = pd.to_datetime(df['start'], unit='s', errors='coerce')
    df['end'] = pd.to_datetime(df['end'], unit='s', errors='coerce')
    return df

# --------------------------
# RDS TABLE CREATION
# --------------------------
with engine.connect() as conn:
    logger.info("Checking/creating tables if needed...")
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS trip_updates (
        id SERIAL PRIMARY KEY,
        trip_id VARCHAR(50),
        route_id VARCHAR(20),
        direction_id INTEGER,
        start_time VARCHAR(20),
        start_date VARCHAR(20),
        timestamp TIMESTAMP,
        stop_sequence INTEGER,
        stop_id VARCHAR(20),
        arrival_time TIMESTAMP,
        departure_time TIMESTAMP
    );"""))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS vehicle_positions (
        id SERIAL PRIMARY KEY,
        vehicle_id VARCHAR(50),
        route_id VARCHAR(20),
        trip_id VARCHAR(50),
        latitude FLOAT,
        longitude FLOAT,
        bearing FLOAT,
        speed FLOAT,
        timestamp TIMESTAMP
    );"""))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS alerts (
        id SERIAL PRIMARY KEY,
        alert_id VARCHAR(50),
        cause VARCHAR(100),
        effect VARCHAR(100),
        header_text TEXT,
        description_text TEXT,
        severity VARCHAR(20),
        start TIMESTAMP,
        end TIMESTAMP
    );"""))
logger.info("Tables checked/created.")

# --------------------------
# PROCESS RAW FILES FROM S3
# --------------------------
def get_latest_file(prefix):
    objs = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=prefix)
    if 'Contents' not in objs:
        logger.warning(f"No files found in {prefix}")
        return None
    latest = sorted(objs['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]
    logger.info(f"Latest file in {prefix}: {latest['Key']}")
    return latest['Key']

def process_and_upload(prefix, flatten_func, table_name):
    key = get_latest_file(prefix)
    if not key:
        logger.warning(f"No data for {prefix}")
        return
    obj = s3.get_object(Bucket=AWS_BUCKET, Key=key)
    json_data = json.load(obj['Body'])
    df = flatten_func(json_data)
    logger.info(f"{table_name}: {len(df)} rows")
    if not df.empty:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Uploaded {len(df)} rows to {table_name}")

process_and_upload('raw/trip_updates', flatten_trip_updates, 'trip_updates')
process_and_upload('raw/vehicle_positions', flatten_vehicle_positions, 'vehicle_positions')
process_and_upload('raw/alerts', flatten_alerts, 'alerts')

logger.info("All uploads complete.")
