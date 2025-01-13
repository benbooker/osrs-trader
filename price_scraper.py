import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from decouple import config

def fetch_price_data(time_interval):
    # Fetches Runelite/OSRS Wiki pricing data from public API
    # https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices

    api_url = f'http://prices.runescape.wiki/api/v1/osrs/{time_interval}'

    headers = {
        'User-Agent': 'osrs_trader',
        'From': '@mulch333 on Discord'
    }

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching {time_interval} data: {e}")
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
    
    return processed_data

def store_data(processed_data, db_params):
    # Store processed data in PostgreSQL database
    if not processed_data:
        print("No data to store")
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
        print(f"Successfully stored {len(processed_data)} price records")
    except Exception as e:
        print(f"Error storing data: {e}")

def run(db_params):
    try:
        data = fetch_price_data() #TODO: ADD SCHEDULER FOR TIME INTERVALS
        if data:
            processed_data = process_data(data)
            store_data(processed_data, db_params)
    except Exception as e:
        print(f"Error in collection run: {e}")


if __name__ == "__main__":

    # Database connection parameters via python-decouple:
    db_params = {
        'dbname': config('DB_NAME'),
        'user': config('DB_USER'),
        'password': config('DB_PASSWORD'),
        'host': config('DB_HOST'),
        'port': config('DB_PORT', cast=int)
    }
    
    run(db_params)
