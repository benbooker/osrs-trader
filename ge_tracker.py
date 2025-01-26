import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from decouple import config
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from time import sleep

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler("ge_tracker_log.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_price_data():
    """Fetches 5m price data from OSRS Wiki API"""
    api_url = 'http://prices.runescape.wiki/api/v1/osrs/5m'
    headers = {
        'User-Agent': 'osrs_ge_price_tracker',
        'From': '@mulch333 on Discord'
    }

    for fetch_attempt in range(3):
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            logger.info("Successfully fetched 5m data.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching 5m data (attempt {fetch_attempt+1}): {e}")
            if fetch_attempt < 2:
                sleep(2 ** fetch_attempt)
    return None

def process_data(json_data):
    """Process raw JSON into database-ready tuples"""
    if not json_data:
        return []
    
    timestamp = datetime.fromtimestamp(json_data['timestamp'])
    processed_data = []
    
    for item_id, item_data in json_data['data'].items():
        processed_data.append((
            timestamp,
            int(item_id),
            item_data.get('avgHighPrice'),
            item_data.get('highPriceVolume'),
            item_data.get('avgLowPrice'),
            item_data.get('lowPriceVolume')
        ))

    logger.info(f"Processed {len(processed_data)} items")
    return processed_data

def store_data(processed_data, db_params):
    """Store processed data in PostgreSQL"""
    if not processed_data:
        logger.warning("No processed data to store.")
        return
    
    insert_query = """
        INSERT INTO item_prices (
            timestamp, item_id, avg_high_price, 
            high_price_volume, avg_low_price, low_price_volume
        ) VALUES %s
        ON CONFLICT (timestamp, item_id) DO UPDATE 
        SET 
            avg_high_price = EXCLUDED.avg_high_price,
            high_price_volume = EXCLUDED.high_price_volume,
            avg_low_price = EXCLUDED.avg_low_price,
            low_price_volume = EXCLUDED.low_price_volume;
    """

    try:
        with psycopg2.connect(db_params) as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, processed_data)
        logger.info(f"Stored {len(processed_data)} price records")
    except Exception as e:
        logger.error(f"Error storing data: {e}")

def task(db_params):
    """Main fetch, process, upload task for 5m data"""
    try:
        data = fetch_price_data()
        if data:
            processed_data = process_data(data)
            store_data(processed_data, db_params)
    except Exception as e:
        logger.error(f"Error in data task: {e}")

def run_scheduler(db_params):
    """Scheduler for data task"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(task, 'interval', minutes=5, args=[db_params], id='5m_task')
    scheduler.start()
    logger.info("Scheduler started. Press Ctrl+C to exit.")

    try:
        while True:
            sleep(60)
            if not scheduler.running:
                logger.error("Scheduler stopped! Restarting...")
                scheduler.shutdown(wait=False)
                scheduler = BackgroundScheduler()
                scheduler.add_job(task, 'interval', minutes=5, args=[db_params])
                scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
        logger.info("Script stopped.")

if __name__ == "__main__":
    db_params = {
        'dbname': config('DB_NAME'),
        'user': config('DB_USER'),
        'password': config('DB_PASSWORD'),
        'host': config('DB_HOST'),
        'port': config('DB_PORT', cast=int)
    }
    run_scheduler(db_params)
