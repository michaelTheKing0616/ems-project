import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import logging
from pathlib import Path
from sqlalchemy import create_engine
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('firebase_fetch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def initialize_firebase(cred_path):
    """Initialize Firebase Admin SDK."""
    try:
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://energy-monitoring-a98e4-default-rtdb.firebaseio.com'
        })
        logger.info("Firebase initialized successfully")
        return db.reference('buildings')
    except Exception as e:
        logger.error(f"Failed to initialize Firebase: {e}")
        raise

def process_snapshot(snapshot, db_connection_string):
    """Process Firebase snapshot and store in TimescaleDB."""
    data = []
    try:
        for building_id, readings in snapshot.items():
            for timestamp, values in readings.items():
                meter = values.get('meter', {})
                record = {
                    'timestamp': timestamp,
                    'building_id': building_id,
                    'temperature': values.get('temperature'),
                    'humidity': values.get('humidity'),
                    'occupancy': values.get('motion'),
                    'energy': meter.get('energy'),
                    'current': meter.get('current'),
                    'frequency': meter.get('frequency'),
                    'power': meter.get('power'),
                    'power_factor': meter.get('power_factor'),
                    'voltage': meter.get('voltage')
                }
                data.append(record)
        
        if not data:
            logger.warning("No data received in snapshot")
            return
        
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
        
        # Store in TimescaleDB
        engine = create_engine(db_connection_string)
        df.to_sql('sensor_data', engine, if_exists='append', index=False)
        logger.info(f"Stored {len(df)} rows in TimescaleDB")
        
        # Save locally for debugging
        output_file = Path('./data/sensor_data.csv')
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(df)} rows to {output_file}")
    except Exception as e:
        logger.error(f"Failed to process snapshot: {e}")
        raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cred-path', type=str, default='./firebase_credentials.json', help='Path to Firebase credentials JSON')
    parser.add_argument('--db-connection', type=str, required=True, help='TimescaleDB connection string')
    args = parser.parse_args()
    
    # Initialize Firebase
    ref = initialize_firebase(args.cred_path)
    
    # Listen for real-time updates
    def listener(event):
        logger.info(f"Received Firebase event: {event.event_type}")
        process_snapshot(ref.get(), args.db_connection)
    
    ref.listen(listener)
    
    # Keep the script running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping Firebase listener")
        firebase_admin.delete_app(firebase_admin.get_app())

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise