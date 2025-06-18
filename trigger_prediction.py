import pandas as pd
import numpy as np
import requests
import json
import logging
from pathlib import Path
from sqlalchemy import create_engine

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trigger_prediction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def preprocess_data(input_file, min_length=24):
    """Preprocess sensor data for DeepAR."""
    dynamic_features = ['temperature', 'humidity', 'occupancy', 'current', 'frequency', 'power', 'power_factor', 'voltage']
    try:
        df = pd.read_csv(input_file)
        logger.info(f"Loaded sensor data: {len(df)} rows")
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp', 'building_id', 'energy'])
        
        # Numeric conversion
        for col in ['energy'] + dynamic_features:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Fill missing dynamic features
        df[dynamic_features] = df[dynamic_features].fillna(0.0)
        df['building_area'] = df.get('building_area', 1000.0)
        
        # Resample to hourly
        agg_dict = {col: 'mean' for col in ['energy'] + dynamic_features}
        agg_dict['building_area'] = 'first'
        agg_dict['occupancy'] = 'max'
        df = df.set_index('timestamp').groupby('building_id').resample('1H').agg(agg_dict).reset_index()
        
        logger.info(f"Preprocessed data: {len(df)} rows, {len(df['building_id'].unique())} buildings")
        return df
    except Exception as e:
        logger.error(f"Failed to preprocess data: {e}")
        raise

def call_ml_endpoint(endpoint_url, api_key, data):
    """Call Azure ML endpoint."""
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    payload = {'data': data}
    try:
        response = requests.post(endpoint_url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Endpoint call failed: {e}")
        raise

def store_predictions(predictions, db_connection_string):
    """Store predictions in TimescaleDB."""
    try:
        engine = create_engine(db_connection_string)
        predictions_df = pd.DataFrame(predictions)
        predictions_df.to_sql('predictions', engine, if_exists='append', index=False)
        logger.info(f"Stored {len(predictions_df)} predictions in TimescaleDB")
    except Exception as e:
        logger.error(f"Failed to store predictions: {e}")
        raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-file', type=str, default='./data/sensor_data.csv', help='Path to sensor data CSV')
    parser.add_argument('--endpoint-url', type=str, required=True, help='Azure ML endpoint URL')
    parser.add_argument('--api-key', type=str, required=True, help='Azure ML endpoint API key')
    parser.add_argument('--db-connection', type=str, required=True, help='TimescaleDB connection string')
    parser.add_argument('--min-length', type=int, default=24, help='Minimum data length per building')
    args = parser.parse_args()
    
    # Preprocess data
    df = preprocess_data(args.input_file, args.min_length)
    
    # Prepare data for DeepAR
    dynamic_features = ['temperature', 'humidity', 'occupancy', 'current', 'frequency', 'power', 'power_factor', 'voltage']
    building_ids = df['building_id'].unique()
    id_to_idx = {bid: idx for idx, bid in enumerate(building_ids)}
    data = []
    for bid in building_ids:
        df_bldg = df[df['building_id'] == bid].sort_values('timestamp').tail(args.min_length)
        if len(df_bldg) < args.min_length:
            logger.warning(f"Skipping building {bid}: insufficient data ({len(df_bldg)})")
            continue
        data.append({
            'start': df_bldg['timestamp'].iloc[0].isoformat(),
            'target': df_bldg['energy'].values.tolist(),
            'feat_dynamic_real': df_bldg[dynamic_features].values.T.tolist(),
            'feat_static_cat': [id_to_idx[bid]],
            'feat_static_real': [float(df_bldg['building_area'].iloc[0])]
        })
    
    if not data:
        raise ValueError("No valid data for prediction")
    
    # Call endpoint
    predictions = call_ml_endpoint(args.endpoint_url, args.api_key, data)
    
    # Process predictions
    results = []
    for idx, pred in enumerate(predictions):
        building_id = building_ids[pred['feat_static_cat'][0]]
        start_time = pred['start']
        if isinstance(start_time, dict):  # Handle Period serialization
            start_time = pd.Timestamp(start_time['value'])
        timestamps = pd.date_range(start=start_time, periods=len(pred['mean']), freq='1H')
        for ts, val in zip(timestamps, pred['mean']):
            results.append({
                'timestamp': ts,
                'building_id': building_id,
                'predicted_energy': val
            })
    
    # Store in TimescaleDB
    store_predictions(results, args.db_connection)
    
    # Save locally
    predictions_df = pd.DataFrame(results)
    output_file = Path('./predictions/latest_predictions.csv')
    output_file.parent.mkdir(parents=True, exist_ok=True)
    predictions_df.to_csv(output_file, index=False)
    logger.info(f"Predictions saved to {output_file}")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise