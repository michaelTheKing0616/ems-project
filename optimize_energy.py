import pandas as pd
import logging
from pathlib import Path
from sqlalchemy import create_engine

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('optimization.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def generate_recommendations(predictions_file, sensor_file, db_connection_string, output_dir):
    """Generate energy reduction recommendations."""
    pred_path = Path(predictions_file)
    sensor_path = Path(sensor_file)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Load data
        pred_df = pd.read_csv(pred_path)
        sensor_df = pd.read_csv(sensor_path)
        logger.info(f"Loaded predictions: {len(pred_df)} rows, sensors: {len(sensor_df)} rows")
        
        pred_df['timestamp'] = pd.to_datetime(pred_df['timestamp'])
        sensor_df['timestamp'] = pd.to_datetime(sensor_df['timestamp'])
        
        # Merge data
        df = pd.merge(pred_df, sensor_df, on=['timestamp', 'building_id'], how='inner')
        if df.empty:
            raise ValueError("No matching data")
        
        # Generate recommendations
        recommendations = []
        for _, row in df.iterrows():
            energy = row['predicted_energy']
            occupancy = row['occupancy']
            temperature = row['temperature']
            power_factor = row.get('power_factor', 1.0)
            voltage = row.get('voltage', 220.0)
            
            rec = "Maintain current settings"
            if occupancy == 0 and energy > 20:
                rec = "Reduce HVAC and lighting (low occupancy)"
            elif temperature > 24 and energy > 30:
                rec = "Lower thermostat to 22°C"
            elif power_factor < 0.9:
                rec = "Improve power factor correction"
            elif voltage > 230 and energy > 40:
                rec = "Check voltage regulation"
            elif energy > 50:
                rec = "Optimize equipment scheduling"
            
            recommendations.append({
                'timestamp': row['timestamp'],
                'building_id': row['building_id'],
                'predicted_energy': energy,
                'recommendation': rec
            })
        
        rec_df = pd.DataFrame(recommendations)
        
        # Store in TimescaleDB
        engine = create_engine(db_connection_string)
        rec_df.to_sql('recommendations', engine, if_exists='append', index=False)
        logger.info(f"Stored {len(rec_df)} recommendations in TimescaleDB")
        
        # Save locally
        output_file = output_dir / 'recommendations.csv'
        rec_df.to_csv(output_file, index=False)
        logger.info(f"Recommendations saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Optimization failed: {e}")
        raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--predictions-file', type=str, default='./predictions/latest_predictions.csv')
    parser.add_argument('--sensor-file', type=str, default='./data/sensor_data.csv')
    parser.add_argument('--db-connection', type=str, required=True)
    parser.add_argument('--output-dir', type=str, default='./optimization')
    args = parser.parse_args()
    
    generate_recommendations(args.predictions_file, args.sensor_file, args.db_connection, args.output_dir)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise