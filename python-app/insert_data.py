import os
import psycopg2
import random
import time
from datetime import datetime

time.sleep(120)

# Fetch connection settings from environment variables
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
dbname = os.getenv('POSTGRES_DB', 'data_engineer')
user = os.getenv('POSTGRES_USER', 'password')
password = os.getenv('POSTGRES_PASSWORD', 'password')

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password
)
cur = conn.cursor()

# Ensure the bank schema exists
cur.execute("CREATE SCHEMA IF NOT EXISTS bank;")
conn.commit()

# Create table in the bank schema if it does not exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS bank.holding (
        price INT,
        volume INT,
        datetime_created timestamp DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(datetime_created)
    );
""")
conn.commit()

def insert_random_data(interval=1):
    while True:
        # Generate random data
        price = round(random.uniform(1, 3000), 2)  # Random price between 1 and 3000
        volume = random.randint(1, 1000)  # Random volume between 1 and 1000
        
        # Insert data into the table within the bank schema
        cur.execute("""
            INSERT INTO bank.holding (price, volume)
            VALUES (%s, %s);
        """, (price, volume))
        conn.commit()
            
        # Print confirmation
        print(f"Inserted data: price={price}, volume={volume}, timestamp={datetime.now()}")
        
        # Wait for the specified interval before next insertion
        time.sleep(interval)

if __name__ == "__main__":
    try:
        # Fetch the interval from an environment variable
        interval = float(os.getenv('INSERT_INTERVAL', 10))  # Default interval is 10 seconds
        insert_random_data(interval=interval)
    except KeyboardInterrupt:
        print("Script interrupted. Closing the connection.")
    finally:
        cur.close()
        conn.close()
