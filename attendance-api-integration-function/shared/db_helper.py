import pyodbc
import pandas as pd
from datetime import datetime

SQL_CONFIG = {
    "server": "tcp:172.16.2.4,1433",
    "database": "ELCM_FBINCQA5",
    "username": "Owner_Nikhilteam",
    "password": "Mac#2580",
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
            CASE
                WHEN EV.AttValue1 = -1 THEN 0
                WHEN EV.AttValue1 IS NULL THEN 0
                ELSE EV.AttValue1
            END AS Attendance,
            0 AS EarlyMinutes,
            0 AS LateMinutes,
            EV.UpdatedDate AS updateDate_timestamp
        FROM TNA.Rostering_Z2 EV (NOLOCK)
        INNER JOIN ztp.ztp_emp_configuration EC ON EV.EmpCode = EC.employee_code
        INNER JOIN dbo.ReqRec_EmployeeDetails ED (NOLOCK)
            ON EV.EmpCode = ED.ED_EMPCode
        WHERE convert(date,EV.Date) >= convert(date,'20260101')
        AND EV.EmpCode IN ('A9011382','A9025124','A9027206','A9027137','A9027128','A9028506','A9016516','A9028258','A9028135','A9019362','A9026076','A9013768','A9028509','A9027667','A9027255','A9028352','A9026744','A9028540','A9022179','A9026681','A9019766','A9026685','A9011829','A9024399','A9027220','A9028555','A9019861','A9027750','A9016136','A9028541','A9018057','A9027113','A9028276','A9026307','A9022404','A9020994','A9026375','A9024864','A9028580','A9027185','A9024659','A9027931','A9007864','A9028360','A9022195','A9016658','A9028512','A9028247','A9019374','A9007169','A1035','A9014544','A9027094','A9018739','A9009839','A9027581','A9022181','A9027838','A9024796','A9028354','A9027965','A9023328','A9027609','A9022876','A9028244','A9028287','A9013589','A9018411','A9028250','A9027621','A9011352','A9026924','A9028463','A9023242','A9028359','A9028113','A9028174','A9011325','A9026701','A9027424','A9019949','A9028019','A9017301','A9026173','A9027451','A9027320','A9027775','A9028095','A9026763','A9028284','A9027833','A9021282','A9028545','A9026342','A9024491','A9028528','A9028574','A9018831','A9028400','A9028206')
        AND EV.UpdatedDate > ?
        """

        cursor.execute(query, last_ts)
    else:

        query = """
        SELECT
            ED.ED_EMPID AS EmployeeID,
            EV.EmpCode AS EmployeeCode,
            EV.Date AS AttendanceDate,
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
        "subscription_name": "FBINCQA5"
    }

    for r in batch:
        payload["records"].append({
            "Attendance": float(r["Attendance"]),
            "AttendanceDate": str(r["AttendanceDate"]),
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