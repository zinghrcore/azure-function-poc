import pyodbc
import pandas as pd
from datetime import datetime

SQL_CONFIG = {
    "server": "tcp:172.16.5.19,1433",
    "database": "ELCM_BURGERKINGGROWTH",
    "username": "TEMP",
    "password": "Temp@123",
    "driver": "{ODBC Driver 18 for SQL Server}"
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

    query = "SELECT TOP 1 last_timestamp FROM Attendance_Watermark ORDER BY ID DESC"
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()

    if not df.empty:
        return df.iloc[0]["last_timestamp"]
    return None


def update_last_timestamp(new_timestamp):

    query = """
    INSERT INTO Attendance_Watermark (last_timestamp)
    VALUES (?)
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, (new_timestamp,))
    conn.commit()
    conn.close()


# ---------------- GET RECORDS ----------------

def get_updated_records(last_ts):
    conn = get_db_connection()
    cursor = conn.cursor()

    if last_ts:
        query = """
        SELECT
    	    ED.ED_EMPID AS EmployeeID,
            EV.EmpCode AS EmployeeCode,
            EV.Date AS AttendanceDate,
            EV.Date AS ApprovedDate,
            CASE
                WHEN EV.AttValue1 = -1 THEN 0
                WHEN EV.AttValue1 IS NULL THEN 0
                ELSE EV.AttValue1
            END AS Attendance,
            0 AS EarlyMinutes,
            0 AS LateMinutes,
            EV.UpdatedDate AS updateDate_timestamp
        FROM TNA.Rostering_Z2 EV (NOLOCK)
        INNER JOIN dbo.ReqRec_EmployeeDetails ED (NOLOCK)
            ON EV.EmpCode = ED.ED_EMPCode
        WHERE EV.UpdatedDate > ?
        """

        cursor.execute(query, last_ts)
    else:

        query = """
        SELECT
            ED.ED_EMPID AS EmployeeID,
            EV.EmpCode AS EmployeeCode,
            EV.Date AS AttendanceDate,
            EV.Date AS ApprovedDate,
            CASE
                WHEN EV.AttValue1 = -1 THEN 0
                WHEN EV.AttValue1 IS NULL THEN 0
                ELSE EV.AttValue1
            END AS Attendance,
            0 AS EarlyMinutes,
            0 AS LateMinutes,
            EV.UpdatedDate AS updateDate_timestamp
        FROM TNA.Rostering_Z2 EV (NOLOCK)
        INNER JOIN dbo.ReqRec_EmployeeDetails ED (NOLOCK)
            ON EV.EmpCode = ED.ED_EMPCode
        WHERE EV.Date >= DATEADD(day,-90,GETDATE())
        """
        cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    records = []

    for row in cursor.fetchall():
        records.append(dict(zip(columns, row)))

    conn.close()

    return records


# ---------------- PAYLOAD ----------------

def create_payload(batch):

    payload = {
        "records": [],
        "subscription_name": "BURGERKINGGROWTH"
    }

    for r in batch:
        payload["records"].append({
            "Attendance": float(r["Attendance"]),
            "AttendanceDate": str(r["AttendanceDate"]),
            "ApprovedDate": str(r["ApprovedDate"]),
            "EarlyMinutes": int(r["EarlyMinutes"]),
            "EmployeeCode": r["EmployeeCode"],
            "EmployeeID": int(r["EmployeeID"]),
            "LateMinutes": int(r["LateMinutes"]),

            "updateDate_timestamp": str(r["updateDate_timestamp"])
        })

    return payload


# ---------------- LOGGING ----------------

def log_batch(batch_id, batch_size, status, details):

    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO Attendance_Log
    (Timestamp,BatchID,BatchSize,Status,Details)
    VALUES (?,?,?,?,?)
    """

    cursor.execute(
        query,
        datetime.now(),
        batch_id,
        batch_size,
        status,
        details[:1000]
    )

    conn.commit()
    conn.close()