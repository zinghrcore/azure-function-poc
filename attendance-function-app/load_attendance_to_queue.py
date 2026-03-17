import os
import json
import pyodbc
import pandas as pd
from datetime import datetime
from azure.storage.queue import QueueClient

# -----------------------------
# Load environment variables
# -----------------------------
def load_env():
    SQL_CONNECTION_STRING = os.getenv("SQL_CONNECTION_STRING")
    QUEUE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
    QUEUE_NAME = "attendance-queue"

    # If running outside Azure Functions, read local.settings.json
    if not SQL_CONNECTION_STRING or not QUEUE_CONNECTION_STRING:
        try:
            with open("local.settings.json", "r") as f:
                settings = json.load(f).get("Values", {})
                SQL_CONNECTION_STRING = SQL_CONNECTION_STRING or settings.get("SQL_CONNECTION_STRING")
                QUEUE_CONNECTION_STRING = QUEUE_CONNECTION_STRING or settings.get("AzureWebJobsStorage")
        except FileNotFoundError:
            raise Exception("local.settings.json not found and environment variables missing.")

    if not SQL_CONNECTION_STRING:
        raise Exception("SQL_CONNECTION_STRING is not set.")
    if not QUEUE_CONNECTION_STRING:
        raise Exception("AzureWebJobsStorage is not set.")

    return SQL_CONNECTION_STRING, QUEUE_CONNECTION_STRING, QUEUE_NAME

# -----------------------------
# Watermark file
# -----------------------------
WATERMARK_FILE = "last_timestamp.txt"

def get_last_timestamp():
    try:
        with open(WATERMARK_FILE, "r") as f:
            ts_str = f.read().strip()
            return pd.to_datetime(ts_str)
    except Exception:
        return None

def update_last_timestamp(new_ts):
    with open(WATERMARK_FILE, "w") as f:
        f.write(str(new_ts))

# -----------------------------
# SQL Fetch
# -----------------------------
def fetch_updated_attendance(SQL_CONNECTION_STRING, latest_timestamp=None):
    conn = pyodbc.connect(SQL_CONNECTION_STRING)
    base_query = "SELECT DISTINCT EmpCode, Date AS AttendanceDate, UpdatedDate FROM TNA.Rostering_Z2 WITH (NOLOCK)"

    if latest_timestamp:
        ts_str = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        query = f"{base_query} WHERE UpdatedDate >= ?"
        df = pd.read_sql_query(query, conn, params=[ts_str])
    else:
        initial_date = (datetime.now() - pd.Timedelta(days=90)).strftime('%Y-%m-%d')
        query = f"{base_query} WHERE Date >= ?"
        df = pd.read_sql_query(query, conn, params=[initial_date])

    conn.close()
    if not df.empty:
        df['UpdatedDate'] = pd.to_datetime(df['UpdatedDate'])
    return df

# -----------------------------
# Send to Azure Queue
# -----------------------------
BATCH_SIZE = 1000

def send_to_queue(conn_str, queue_name, df):

    queue_client = QueueClient.from_connection_string(conn_str, queue_name)

    # Create queue if it does not exist
    try:
        queue_client.create_queue()
    except:
        pass

    empcodes = df["EmpCode"].drop_duplicates().tolist()

    batch_number = 1

    for i in range(0, len(empcodes), BATCH_SIZE):

        batch = empcodes[i:i + BATCH_SIZE]

        message = {
            "batch_id": batch_number,
            "emp_codes": batch
        }

        queue_client.send_message(json.dumps(message))

        print(f"Sent batch {batch_number} with {len(batch)} employees")

        batch_number += 1
# -----------------------------
# Main
# -----------------------------
def main():
    SQL_CONNECTION_STRING, QUEUE_CONNECTION_STRING, QUEUE_NAME = load_env()
    print(f"SQL_CONNECTION_STRING loaded: {SQL_CONNECTION_STRING[:30]}...")
    last_ts = get_last_timestamp()
    print(f"Last timestamp: {last_ts}")
    df_new = fetch_updated_attendance(SQL_CONNECTION_STRING, last_ts)
    print(f"Fetched {len(df_new)} new records from SQL.")
    send_to_queue(QUEUE_CONNECTION_STRING, QUEUE_NAME, df_new)
    if not df_new.empty:
        new_ts = df_new['UpdatedDate'].max()
        update_last_timestamp(new_ts)
        print(f"Updated last timestamp to {new_ts}")

# -----------------------------
# Run if script is executed directly
# -----------------------------
if __name__ == "__main__":
    main()