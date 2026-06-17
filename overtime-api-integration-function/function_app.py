import azure.functions as func
import os
import logging
import json
import uuid
import time

from decimal import Decimal
from datetime import datetime, date, timedelta

from shared.api_helper import call_attendance_api
from shared.db_helper import (
    get_last_timestamp,
    get_overtime_data,
    SQL_CONFIG,
    log_batch,
    update_last_timestamp
)

from shared.queue_helper import send_to_queue
from collections import defaultdict

app = func.FunctionApp()

# ---------------- TIMER TRIGGER ----------------

@app.timer_trigger(schedule=os.getenv("schedule"), arg_name="myTimer", run_on_startup=False)
def overtime_timer(myTimer: func.TimerRequest):

    logging.info("========== OVERTIME TIMER STARTED ==========")
    logging.info(f"SQL_CONFIG Count : {len(SQL_CONFIG)}")
    logging.info(f"SQL_CONFIG : {SQL_CONFIG}")

    time.sleep(5)

    try:

        if not SQL_CONFIG:
            logging.error("SQL_CONFIG is empty")
            return

        for db in SQL_CONFIG:

            if "name" not in db:
                logging.error(f"Missing 'name' in DB config: {db}")
                continue

            db_name = db["name"]
            logging.info(f"Starting database {db_name}")

            last_ts = get_last_timestamp(db,db_name)
            
            records = get_overtime_data(db,last_ts)

            if not records:
                logging.info(f"No new overtime records found for {db_name}")
                continue

            logging.info(f"Total records fetched: {len(records)}")

            emp_groups = defaultdict(list)

            for r in records:
                emp_groups[r["EmployeeCode"]].append(r)

            grouped_records = list(emp_groups.values())

            batch_size = int(os.getenv("BatchSize", "100"))

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

                try:
                    max_ts = max(
                        datetime.fromisoformat(r["updateDate_timestamp"])
                        for r in flat_batch
                        if r.get("updateDate_timestamp")
                    )
                except Exception as e:
                    logging.error(f"Timestamp parsing failed: {e}")
                    continue

                payload = {
                    "records": flat_batch,
                    "source_db": db_name,
                    "subscription_name": db.get("subscription_name"),
                    "max_timestamp": max_ts.isoformat()
                
                }

                logging.info(f"Pushing batch of {len(flat_batch)} records for {db_name}")
                send_to_queue(payload)

            logging.info(f"{len(records)} records pushed for {db_name}")

    except Exception as e:
        logging.error(f"Timer error: {str(e)}")

    logging.info("========== OVERTIME TIMER FINISHED ==========")

# ---------------- QUEUE TRIGGER ----------------

@app.queue_trigger(
    arg_name="azqueue",
    queue_name=os.getenv("QueueName"),
    connection="AzureWebJobsStorage"
)
def process_overtime_batch(azqueue: func.QueueMessage):

    logging.info("========== OVERTIME QUEUE STARTED ==========")

    try:
        payload = json.loads(azqueue.get_body().decode("utf-8"))

        logging.info(f"Queue Message: {payload}")

        records = payload.get("records", [])
        source_db = payload.get("source_db")
        max_ts_str = payload.get("max_timestamp")
        subscription_name = payload.get("subscription_name")

        if not records:
            logging.warning("No records found in message")
            return

        if not source_db:
            raise Exception("Missing source_db in payload")

        if not max_ts_str:
            raise Exception("Missing max_timestamp in payload")
        
        if not subscription_name:
            raise Exception(f"Missing subscription_name for DB: {source_db}")

        db_config = next((d for d in SQL_CONFIG if d["name"] == source_db), None)

        if not db_config:
            raise Exception(f"DB config not found for {source_db}")

        batch_size = len(records)

        logging.info(f"Processing {batch_size} records from {source_db}")

        max_ts = datetime.fromisoformat(max_ts_str)

        max_ts = max_ts + timedelta(microseconds=1)

        #overtime_data = []

        #for row in records:

        #    if not row.get("EmployeeCode") or not row.get("Date"):
        #        continue

        #    overtime_data.append({
        #        "employee_id": int(row.get("EmployeeCode")),
        #        "date": str(row.get("Date"))[:10],
        #        "code": row.get("Code"),
        #        "conversion": row.get("Conversion"),
        #        "ot": float(row.get("ExtraHrs", 0)),
        #        "ot_pay": float(row.get("ExtraTimePay", 0))
        #    })

        # ---------------- CLEAN DATA ----------------

        cleaned_records = []

        for r in records:
            r.pop("updateDate_timestamp", None)
            r.pop("source_db", None)
            cleaned_records.append(r)

        final_payload = {
            "overtime_data": cleaned_records,
            "subscription_name": subscription_name
        }

        logging.info(f"Final Payload: {json.dumps(final_payload)}")

        # ---------------- CALL API ----------------

        status_code, response = call_attendance_api(final_payload)

        logging.info(f"API Status: {status_code}")
        logging.info(f"API Response: {response}")

        if status_code == 200:

            logging.info("API call successful")

            log_batch(
                db_config,
                batch_size,
                "SUCCESS",
                str(response),
                0,
                0
            );

            update_last_timestamp(db_config, max_ts, db_config["name"]);
        
            logging.info(f"Watermark updated for {source_db}")

        else:

            logging.error("API returned non-200 response")

            log_batch(
                db_config,
                batch_size,
                "FAILED",
                str(response),
                1,
                final_payload
            )

            raise Exception(f"API returned status {status_code}")

    except Exception as e:

        logging.error(f"Queue processing error: {str(e)}")

        try:
            log_batch(
                db_config if 'db_config' in locals() else SQL_CONFIG[0],
                batch_size if 'batch_size' in locals() else 0,
                "ERROR",
                str(e)
            )
        except:
            logging.error("Failed to log batch error")

        raise

    logging.info("========== QUEUE TRIGGER FINISHED ==========")