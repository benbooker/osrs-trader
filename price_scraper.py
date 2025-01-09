import requests
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime


class PriceCollector:
    def __init__(self, db_params):
        self.db_params = db_params
        self.setup_database()

    "Database setup method. Creates necessary database tables if they don't already exist"
    def setup_database(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS item_prices (
            timestamp TIMESTAMP NOT NULL,
            item_id INTEGER NOT NULL,
            avg_high_price INTEGER,
            high_price_volume INTEGER,
            avg_low_price INTEGER,
            low_price_volume INTEGER,
            PRIMARY KEY (timestamp, item_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_item_prices_timestamp 
        ON item_prices(timestamp);
        
        CREATE INDEX IF NOT EXISTS idx_item_prices_item_id 
        ON item_prices(item_id);
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)

        
    """
    Method for fetching Runelite/OSRS Wiki pricing data via API
    See: https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices
    """
    def fetch_5m_data(self):
        api_url = 'http://prices.runescape.wiki/api/v1/osrs/5m'

        headers = {
            'User-Agent': 'osrs_trader',
            'From': '@mulch333 on Discord'
        }

        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
    
    """Method to convert API response JSON data into database-friendly format"""
    def process_data(self, json_data):
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
        
        return processed_data

    """
    Method to store processed data in PostgreSQL database
    """
    def store_data(self, processed_data):
        if not processed_data:
            print("No data to store")
            return
        
        insert_query = """
            INSERT INTO item_prices (
            timestamp, item_id, avg_high_price, high_price_volume,
            avg_low_price, low_price_volume
        ) VALUES %s
        ON CONFLICT (timestamp, item_id) DO UPDATE 
        SET 
            avg_high_price = EXCLUDED.avg_high_price,
            high_price_volume = EXCLUDED.high_price_volume,
            avg_low_price = EXCLUDED.avg_low_price,
            low_price_volume = EXCLUDED.low_price_volume;
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    execute_values(cur, insert_query, processed_data)
            print(f"Successfully stored {len(processed_data)} price records")
        except Exception as e:
            print(f"Error storing data: {e}")

    """
    Main execution method
    """
    def run(self):
        try:
            data = self.fetch_5m_data()
            if data:
                processed_data = self.process_data(data)
                self.store_data(processed_data)
        except Exception as e:
            print(f"Error in collection run: {e}")



if __name__ == "__main__":

    # Database connection parameters:
    db_params = {
        'dbname': '*****',
        'user': '*****',
        'password': '*****',
        'host': '*****',
        'port': '*****'
    }

    collector = PriceCollector(db_params)
    collector.run()
