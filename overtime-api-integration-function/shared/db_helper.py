import pyodbc
import pandas as pd
from datetime import datetime

SQL_CONFIG = {
    "server": "tcp:172.16.2.4,1433",
    "database": "ELCM_FBINCQA5",
    "username": "Owner_Nikhilteam",
    "password": "Mac#2580",
    "driver": "{ODBC Driver 17 for SQL Server}"
}

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

    query = "SELECT TOP 1 last_timestamp FROM dbo.overtime_watermark ORDER BY ID DESC"
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()

    if not df.empty:
        return df.iloc[0]["last_timestamp"]
    return None


def update_last_timestamp(new_timestamp):

    query = """
    Update dbo.overtime_watermark 
    Set last_timestamp = ?
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, (new_timestamp,))
    conn.commit()
    conn.close()

# ---------------- GET RECORDS ----------------

def get_overtime_data(last_ts):

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.usp_GetOvertimeData @LastTimestamp = ?", last_ts)

    columns = [column[0] for column in cursor.description]
    records = [dict(zip(columns, row)) for row in cursor.fetchall()]
    # records = []
    # for row in cursor.fetchall():
    #    records.append(dict(zip(columns, row)))

    conn.close()

    return records

# ---------------- LOGGING ----------------

def log_batch(batch_size, status, details):

    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO dbo.overtime_Log
    (Timestamp,BatchSize,Status,Details)
    VALUES (?,?,?,?)
    """

    cursor.execute(
        query,
        datetime.now(),
        batch_size,
        status,
        details[:1000]
    )

    conn.commit()
    conn.close()