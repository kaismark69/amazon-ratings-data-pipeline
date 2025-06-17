import pandas as pd
import psycopg2
import os
import sys
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
                    
# This tells Python to connect to the PostgreSQL container using the credentials defined.
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "storage-db"),# the name of the PostgreSQL container from docker-compose.yml
    "port": int(os.environ.get("DB_PORT", 5432)),# default PostgreSQL port
    "dbname": os.environ.get("DB_NAME", "ratings"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "postgres") # try to get the DB_PASSWORD value from the environment. Otherwise, use 'postgres' as a backup default.
}

def filter_monthly_data(file_path, month):
    """
    Reads the full ratings CSV and filters rows by the specified month (6 for June).   
    It returns a DataFrame containing only rows from the selected month.
    """
    try:
        logging.info(f"Reading file: {file_path}")
        df = pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Failed to read CSV: {e}")
        sys.exit(1)   
    df.columns = [col.lower() for col in df.columns]# convert column names to lowercase
    df = df[["userid", "productid", "rating", "timestamp"]]  # Ensure consistent column names  #  Print first 3 original timestamps
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')# convert the UNIX timestamp in seconds to datetime, unit='s' for seconds
    # A UNIX timestamp counts the number of seconds since January 1, 1970, 00:00:00 UTC: 1369699200 ==> 2013-05-28
    filtered_df = df[df['timestamp'].dt.month == month]
    logging.info(f"Filtered {len(filtered_df)} rows for month {month}")
    return filtered_df

def send_to_postgres(df, table_name): 
    try:
        logging.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        logging.info(f"Dropping and creating table: {table_name}")
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        # Create a new table in PostgreSQL
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userId TEXT,
                productId TEXT,
                rating FLOAT,
                timestamp TIMESTAMP
            );
        """)
        logging.info(f"Inserting {len(df)} rows into {table_name}...")
        # Insert data into the PostgreSQL table
        for _, row in df.iterrows():
            # %s is a placeholder for parameterized queries in psycopg2.
            # First %s → row['userid']
            # Second %s → row['productid']
            cur.execute(f"""
                INSERT INTO {table_name} (userId, productId, rating, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (row['userid'], row['productid'], row['rating'], row['timestamp']))
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Data successfully sent to table: {table_name}")
    except Exception as e:
        logging.error(f"Failed to send data to PostgreSQL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Get month from command line (default to June if not provided)
    month = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    filtered = filter_monthly_data("/data/ratings_beauty.csv", month)
    send_to_postgres(filtered, f"ratings_month_{month}")

