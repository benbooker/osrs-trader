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

def fetch_price_data(time_interval):
    # Fetches Runelite/OSRS Wiki pricing data from public API
    # https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices

    api_url = f'http://prices.runescape.wiki/api/v1/osrs/{time_interval}'

    headers = {
        'User-Agent': 'osrs_ge_price_tracker',
        'From': '@mulch333 on Discord'
    }

    for fetch_attempt in range (3):
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            logger.info(f"Successfully fetched {time_interval} data.")
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {time_interval} data: {e}")
            if fetch_attempt < 2:
                sleep(2 ** fetch_attempt)
    # Return none if all fetch attempts fail
    return None

def process_data(json_data, time_interval):
    # Process raw json data into tuples ready to be stored in database
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
            item_data.get('lowPriceVolume'),
            time_interval
        ))

    logger.info(f"Processed {len(processed_data)} items from {time_interval} interval.")
    return processed_data

def store_data(processed_data, db_params):
    # Store processed data in PostgreSQL database
    if not processed_data:
        logger.warning("No processed data to store.")
        return
    
    insert_query = """
        INSERT INTO item_prices (
        timestamp, item_id, avg_high_price, high_price_volume,
        avg_low_price, low_price_volume, time_interval
    ) VALUES %s
    ON CONFLICT (timestamp, item_id, time_interval) DO UPDATE 
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
        logger.info(f"Successfully stored {len(processed_data)} price records.")
    except Exception as e:
        logger.error(f"Error storing data: {e}")


def task(db_params, time_interval):
    # Main task function to fetch, process, and store data for a given time interval
    try:
        data = fetch_price_data(time_interval)
        if data:
            processed_data = process_data(data, time_interval)
            store_data(processed_data, db_params)
    except Exception as e:
        logger.error(f"Error in task for {time_interval}: {e}")

def run_scheduler(db_params):
    # Task schedule manager for executing 5m and 1h data tasks at appropriate intervals
    def create_scheduler():
        """Helper function to create and start a scheduler with tasks."""
        new_scheduler = BackgroundScheduler()
        new_scheduler.add_job(task, 'interval', minutes=5, args=[db_params, '5m'], id='5m_task')
        new_scheduler.add_job(task, 'interval', hours=1, args=[db_params, '1h'], id='1h_task')
        new_scheduler.start()
        logger.info("Scheduler started successfully.")
        return new_scheduler

    scheduler = create_scheduler()
    logger.info("Starting ge_tracker.py script. Press Ctrl+C to exit.")

    try:
        while True:
            sleep(60)  # Check the scheduler status every 60 seconds
            if not scheduler.running:
                logger.error("Scheduler stopped running unexpectedly. Restarting...")
                scheduler.shutdown(wait=False)  # Gracefully stop the current scheduler
                scheduler = create_scheduler()  # Create and start a new scheduler
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down the scheduler gracefully...")
        scheduler.shutdown()
        logger.info("Script stopped.")


if __name__ == "__main__":
    # Database connection parameters via python-decouple:
    db_params = {
        'dbname': config('DB_NAME'),
        'user': config('DB_USER'),
        'password': config('DB_PASSWORD'),
        'host': config('DB_HOST'),
        'port': config('DB_PORT', cast=int)
    }
    
    run_scheduler(db_params)


