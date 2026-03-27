import azure.functions as func
import logging
import json
import uuid
import time
from datetime import datetime
from collections import defaultdict

from shared.db_helper import (
    get_last_timestamp,
    get_overtime_data,
    update_last_timestamp,
    log_batch
)

from shared.queue_helper import send_to_queue
from shared.api_helper import call_attendance_api
from datetime import datetime, date, timedelta
from decimal import Decimal

app = func.FunctionApp()

# ---------------- TIMER TRIGGER ----------------

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer")
def overtime_timer(myTimer: func.TimerRequest):

    logging.info("========== OVERTIME TIMER STARTED ==========")

    try:
        last_ts = get_last_timestamp()
        logging.info(f"Last watermark timestamp: {last_ts}")

        records = get_overtime_data(last_ts)

        if not records:
            logging.info("No new overtime records found")
            return

        logging.info(f"Total records fetched: {len(records)}")

        emp_groups = defaultdict(list)

        for r in records:
            emp_groups[r["EmployeeCode"]].append(r)

        grouped_records = list(emp_groups.values())

        batch_size = 300

        for i in range(0, len(grouped_records), batch_size):

            batch_group = grouped_records[i:i + batch_size]
            flat_batch = []

            for sublist in batch_group:
                for item in sublist:

                    new_item = {}

                    for k, v in item.items():

                        if isinstance(v, (datetime, date)):
                            new_item[k] = v.isoformat()

                        elif isinstance(v, Decimal):
                            new_item[k] = float(v)

                        else:
                            new_item[k] = v

                    flat_batch.append(new_item)

            payload = {"records": flat_batch}

            send_to_queue(payload)

        logging.info("All batches pushed to queue")

    except Exception as e:
        logging.error(f"Timer error: {str(e)}")

    logging.info("========== OVERTIME TIMER FINISHED ==========")


# ---------------- QUEUE TRIGGER ----------------

@app.queue_trigger(
    arg_name="azqueue",
    queue_name="overtime-queue",
    connection="AzureWebJobsStorage"
)
def process_overtime_batch(azqueue: func.QueueMessage):

    logging.info("========== OVERTIME QUEUE STARTED ==========")

    try:
        payload = json.loads(azqueue.get_body().decode("utf-8"))
        records = payload.get("records", [])

        if not records:
            return

        batch_size = len(records)

        max_ts = max(
            datetime.fromisoformat(str(r["updateDate_timestamp"]))
            for r in records
        )

        max_ts = max_ts + timedelta(microseconds=1)

        overtime_data = []

        for row in records:

            if not row.get("EmployeeCode") or not row.get("Date"):
                continue

                overtime_data.append({
                "code": row.get("Code"),
                "conversion": row.get("Conversion"),
                "date": str(row.get("Date"))[:10],
                "employee_id": str(row.get("EmployeeCode")),
                "ot": float(row.get("ExtraHrs", 0)),
                "ot_pay": float(row.get("ExtraTimePay", 0))
                })

        final_payload = {
            "overtime_data": overtime_data,
            "subscription_name": "FBINCQA5"
        }

        logging.info(f"Final Payload: {json.dumps(final_payload)}")

        status_code, response = call_attendance_api(final_payload)

        logging.info(f"API Status: {status_code}")
        logging.info(f"API Response: {response}")

        if status_code == 200:

            log_batch(batch_size, "SUCCESS", json.dumps(final_payload))
            update_last_timestamp(max_ts)

        else:
            log_batch(batch_size, "FAILED", response)
            raise Exception(f"API failed: {status_code}")

    except Exception as e:
        logging.error(str(e))
        raise

    logging.info("========== OVERTIME QUEUE FINISHED ==========")