import os
import json
import pyodbc
import pandas as pd
from datetime import datetime

# ---------------- DB CONFIG ----------------

SQL_CONFIG = json.loads(os.getenv("SQL_CONFIG", "[]"))

# SQL_CONFIG = [{
#     "name": "ELCM_BURGERKINGGROWTH",
#     "server": "tcp:172.16.5.19,1433",
#     "database": "ELCM_BURGERKINGGROWTH",
#     "username": "Temp",
#     "password": "Temp@123",
#     "driver": "{ODBC Driver 18 for SQL Server}",
#     "subscription_name": "BURGERKINGGROWTH"
#     },
#     {
#     "name": "ELCM_SBFCP2",
#     "server": "tcp:172.16.5.7,1433",
#     "database": "ELCM_SBFCP2",
#     "username": "Temp",
#     "password": "Temp@123",
#     "driver": "{ODBC Driver 18 for SQL Server}",
#     "subscription_name": "SBFCP2"
#     }
# ]

# ---------------- CONNECTION ----------------

def get_db_connection(db):

    conn_str = (
        f"DRIVER={db['driver']};"
        f"SERVER={db['server']};"
        f"DATABASE={db['database']};"
        f"UID={db['username']};"
        f"PWD={db['password']};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )

    return pyodbc.connect(conn_str)

# ---------------- WATERMARK ----------------

def get_last_timestamp(db, source_db):

    query = """
    SELECT TOP 1 last_timestamp 
    FROM Attendance_Watermark 
    WHERE source_db = ? 
    ORDER BY ID DESC
    """

    conn = get_db_connection(db)
    df = pd.read_sql_query(query, conn, params=[source_db])
    conn.close()

    if not df.empty:
        return df.iloc[0]["last_timestamp"]

    return None


def update_last_timestamp(db, new_timestamp, source_db):

    query = """
    UPDATE Attendance_Watermark
    SET last_timestamp = ?
    WHERE source_db = ?
    """

    conn = get_db_connection(db)
    cursor = conn.cursor()

    cursor.execute(query, (new_timestamp, source_db))
    conn.commit()
    cursor.close()
    conn.close()

# ---------------- GET RECORDS ----------------

def get_updated_records(db, last_ts):

    conn = get_db_connection(db)
    cursor = conn.cursor()

    if last_ts:
        cursor.execute("EXEC dbo.uspGetAttendanceData @LastTimestamp = ?", last_ts)
    else:
        cursor.execute("EXEC dbo.uspGetAttendanceData @LastTimestamp = NULL")

    columns = [column[0] for column in cursor.description]

    records = []
    for row in cursor.fetchall():
        records.append(dict(zip(columns, row)))

    conn.close()

    return records

# ---------------- LOGGING ----------------

def log_batch(db, batch_size, status, details,IsBatchComplete,payload):

    conn = get_db_connection(db)
    cursor = conn.cursor()

    if not isinstance(details, str):
        details = json.dumps(details, default=str)

    if not isinstance(payload, str):
        payload = json.dumps(payload, default=str)

    query = """
    INSERT INTO Attendance_Log
    (Timestamp,BatchSize,Status,Details,IsBatchComplete,payload)
    VALUES (?,?,?,?,?,?)
    """

    cursor.execute(
        query,
        datetime.now(),
        batch_size,
        status,
        details[:1000],
        IsBatchComplete,
        payload
    )

    conn.commit()
    conn.close()