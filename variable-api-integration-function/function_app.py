import azure.functions as func
import logging
import json
from datetime import datetime, date
from collections import defaultdict
from decimal import Decimal

from shared.config import (
    SCHEDULE,
    BATCH_SIZE,
    DATABASES
)

from shared.db_helper import (
    get_last_timestamp,
    get_variable_data,
    update_last_timestamp,
    log_batch
)

from shared.queue_helper import send_to_queue
from shared.api_helper import call_variable_api
from shared.payload_builder import transform_records

app = func.FunctionApp()

# =========================================================
# ---------------- TIMER TRIGGER ---------------------------
# =========================================================

@app.timer_trigger(schedule=SCHEDULE, arg_name="myTimer")
def variable_timer(myTimer: func.TimerRequest):

    logging.info("========== VARIABLE TIMER STARTED ==========")

    for db in DATABASES:

        logging.info(f"Processing DB: {db['name']}")

        try:
            last_ts = get_last_timestamp(db)
            logging.info(f"Last watermark timestamp: {last_ts}")

            records = get_variable_data(db,last_ts)

            if not records:
                logging.info(f"No new onboarding records found for {db['name']}")
                continue
            logging.info(f"Total records fetched: {len(records)}")

            emp_groups = defaultdict(list)

            for r in records:
                emp_groups[r["EmployeeCode"]].append(r)

            grouped_records = list(emp_groups.values())

            batch_size = BATCH_SIZE

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

                payload = {""
                    "records": flat_batch,
                    "subscription_name": db["subscription_name"]        
                }

                send_to_queue(payload)

            logging.info("All batches pushed to queue")

        except Exception as e:
            logging.error(f"Timer error: {str(e)}")

    logging.info("========== VARIABLE TIMER FINISHED ==========")

# =========================================================
# ---------------- QUEUE TRIGGER ---------------------------
# =========================================================

@app.queue_trigger(
    arg_name="azqueue",
    queue_name="variable-queue",
    connection="AzureWebJobsStorage"
)
def process_variable_batch(azqueue: func.QueueMessage):

    logging.info("========== VARIABLE QUEUE STARTED ==========")

    try:
        payload = json.loads(azqueue.get_body().decode("utf-8"))

        logging.info(
            f"Received payload: {json.dumps(payload)}"
        )

        subscription_name = payload["subscription_name"]

        db_config = next(
            (
                db
                for db in DATABASES
                if db["subscription_name"] == subscription_name
            ),
            None
        )

        if not db_config:
            raise Exception(
                f"No DB configuration found for subscription {subscription_name}"
            )

        records = payload.get("records", [])

        if not records:
            logging.warning("Empty batch received")
            return

        batch_size = len(records)

        try:
            max_ts = max(
                datetime.fromisoformat(str(r.get("updateDate_timestamp")))
                for r in records
                if r.get("updateDate_timestamp") is not None
            )
        except Exception:
            max_ts = datetime.now()

        emp_data = transform_records(records, db_config["country"])

        final_payload = {
            "records": emp_data,
            "subscription_name": db_config["subscription_name"]
        }

        logging.info(f"Final Payload Sample: {json.dumps(final_payload)[:1000]}")

        status_code, response = call_variable_api(final_payload)

        logging.info(f"API Status: {status_code}")
        logging.info(f"API Response: {response}")

        if status_code == 200:

            log_batch(db_config,batch_size, "SUCCESS","",1,json.dumps(final_payload))
            update_last_timestamp(db_config,max_ts)

        else:
            log_batch(db_config,batch_size, "FAILED",response,0,json.dumps(final_payload))
            logging.error(f"API failed for {db_config['name']} with status {status_code}")
            return
        
    except Exception as e:
        logging.error(f"Queue processing error: {str(e)}")
        raise

    logging.info("========== VARIABLE QUEUE FINISHED ==========")