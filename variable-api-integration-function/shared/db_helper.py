import pyodbc
import pandas as pd
from datetime import datetime
from shared.config import SQL_CONFIG

def get_db_connection():

    conn_str = (
        f"DRIVER={SQL_CONFIG['driver']};"
        f"SERVER={SQL_CONFIG['server']};"
        f"DATABASE={SQL_CONFIG['database']};"
        f"UID={SQL_CONFIG['username']};"
        f"PWD={SQL_CONFIG['password']};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )

    return pyodbc.connect(conn_str)

# ---------------- WATERMARK ----------------

def get_last_timestamp():

    query = "SELECT TOP 1 last_timestamp FROM dbo.variable_watermark ORDER BY ID DESC"
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()

    if not df.empty:
        return df.iloc[0]["last_timestamp"]
    return None


def update_last_timestamp(new_timestamp):

    query = """
    Update dbo.variable_watermark 
    set last_timestamp = ?
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, (new_timestamp,))
    conn.commit()
    conn.close()

# ---------------- GET RECORDS ----------------

def get_variable_data(last_ts):

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.usp_GetVariableData @LastTimestamp = ?", last_ts)

    columns = [column[0] for column in cursor.description]
    records = []

    for row in cursor.fetchall():
        records.append(dict(zip(columns, row)))

    conn.close()

    return records


# ---------------- LOGGING ----------------

def log_batch(batch_size, status, details,IsBatchComplete,Payload):

    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO dbo.Variable_Log
    (Timestamp,BatchSize,Status,Details,IsBatchComplete,Payload)
    VALUES (?,?,?,?,?,?)
    """

    cursor.execute(
        query,
        datetime.now(),
        batch_size,
        status,
        details,
        IsBatchComplete,
        Payload
    )

    conn.commit()
    conn.close()