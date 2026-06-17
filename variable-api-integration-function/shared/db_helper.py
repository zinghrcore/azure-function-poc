import pyodbc
import pandas as pd
from datetime import datetime

def get_db_connection(db_config):

    conn_str = (
        f"DRIVER={db_config.get('driver','{ODBC Driver 18 for SQL Server}')};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['username']};"
        f"PWD={db_config['password']};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )

    return pyodbc.connect(conn_str)

# ---------------- WATERMARK ----------------

def get_last_timestamp(db_config):

    query = "SELECT TOP 1 last_timestamp FROM dbo.variable_watermark ORDER BY ID DESC"
    conn = get_db_connection(db_config)
    df = pd.read_sql_query(query, conn)
    conn.close()

    if not df.empty:
        return df.iloc[0]["last_timestamp"]
    return None


def update_last_timestamp(db_config,new_timestamp):

    query = """
    Update dbo.variable_watermark 
    set last_timestamp = ?
    """

    conn = get_db_connection(db_config)
    cursor = conn.cursor()
    cursor.execute(query, (new_timestamp,))
    conn.commit()
    conn.close()

# ---------------- GET RECORDS ----------------

def get_variable_data(db_config,last_ts):

    conn = get_db_connection(db_config)
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.usp_GetVariableData @LastTimestamp = ?", last_ts)

    columns = [column[0] for column in cursor.description]
    records = []

    for row in cursor.fetchall():
        records.append(dict(zip(columns, row)))

    conn.close()

    return records


# ---------------- LOGGING ----------------

def log_batch(db_config,batch_size, status, details,IsBatchComplete,Payload):

    conn = get_db_connection(db_config)
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
        details[:1000],
        IsBatchComplete,
        Payload
    )

    conn.commit()
    conn.close()