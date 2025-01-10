import psycopg2
from decouple import config

def setup_database(db_params):
    # Database setup: Creates necessary database tables if they don't already exist.
    create_table_query = """
    CREATE TABLE IF NOT EXISTS item_prices (
        timestamp TIMESTAMPTZ NOT NULL,
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
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
            print("Database setup completed successfully.")
    except Exception as e:
        print(f"Error during database setup: {e}")

if __name__ == "__main__":
    # Database connection parameters via python-decouple:
    db_params = {
        'dbname': config('DB_NAME'),
        'user': config('DB_USER'),
        'password': config('DB_PASSWORD'),
        'host': config('DB_HOST'),
        'port': config('DB_PORT', cast=int)
    }
    
    setup_database(db_params)
