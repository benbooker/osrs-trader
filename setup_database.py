# setup_database.py
import logging
import psycopg2
from decouple import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_database():
    """One-time setup for OSRS price database"""
    try:
        # Connect using basic credentials
        conn = psycopg2.connect(
            dbname=config('DB_NAME'),
            user=config('DB_USER'),
            password=config('DB_PASSWORD'),
            host=config('DB_HOST'),
            port=config('DB_PORT', default=5432, cast=int)
        )
        
        with conn.cursor() as cur:
            # Create core table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS item_prices (
                    timestamp TIMESTAMPTZ NOT NULL,
                    item_id INTEGER NOT NULL,
                    avg_high_price INTEGER,
                    high_price_volume INTEGER,
                    avg_low_price INTEGER,
                    low_price_volume INTEGER,
                    PRIMARY KEY (timestamp, item_id)
                );
            """)
            
            # Enable TimescaleDB features
            cur.execute("SELECT create_hypertable('item_prices', 'timestamp')")
            cur.execute("""
                ALTER TABLE item_prices SET (
                    timescaledb.compress,
                    timescaledb.compress_orderby = 'timestamp DESC'
                );
            """)
            
            logger.info("Database setup complete!")

    except Exception as e:
        logger.error(f"Setup failed: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    setup_database()
