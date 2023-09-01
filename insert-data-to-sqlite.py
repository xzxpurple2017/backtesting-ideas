import os
import sqlite3
import argparse
import pandas as pd

def validate_columns(df):
    required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' is missing in the CSV file.")

# Argument parsing
parser = argparse.ArgumentParser(description='Insert data from a CSV file into a SQLite database.')
parser.add_argument('-f', '--file', required=True, help='Path to the CSV file.')

args = parser.parse_args()
data_file = args.file

# Validate file existence
if not os.path.exists(data_file):
    raise FileNotFoundError(f"File {data_file} does not exist.")

# Read your data
df = pd.read_csv(data_file)

# Validate columns
validate_columns(df)

# Convert 'timestamp' column to a Pandas datetime type
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d.%m.%Y %H:%M:%S.%f').dt.tz_localize('UTC')
df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Connect to SQLite3 database
sqlite_db = "hk40cfd"
conn = sqlite3.connect(f"C:\\Users\\philip\\hkstocks\\{sqlite_db}.db")

# Create table if not exists
conn.execute("""
    CREATE TABLE IF NOT EXISTS hk40cfd (
        timestamp TIMESTAMP PRIMARY KEY,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL
    );
""")

# Insert or skip data
for _, row in df.iterrows():
    exists = conn.execute("SELECT COUNT(*) FROM hk40cfd WHERE timestamp = ?", (row['timestamp'],)).fetchone()[0]
    if not exists:
        conn.execute("""
            INSERT INTO hk40cfd (timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume']))

conn.commit()

# Verify the table creation
result_df = pd.read_sql('SELECT * FROM hk40cfd LIMIT 3', conn)
print(result_df)

# Check for duplicates
print("--- Duplicate checks ---")
duplicate_check = pd.read_sql('SELECT timestamp, COUNT(*) FROM hk40cfd GROUP BY timestamp HAVING COUNT(*) > 1;', conn)
print(duplicate_check)
