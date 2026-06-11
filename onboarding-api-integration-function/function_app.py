import azure.functions as func
import logging
import json
from datetime import datetime, date
from collections import defaultdict
from decimal import Decimal

from shared.config import (
    SCHEDULE,
    BATCH_SIZE,
    SUBSCRIPTION_NAME,
    COUNTRY
)

from shared.db_helper import (
    get_last_timestamp,
    get_onboarding_data,
    update_last_timestamp,
    log_batch
)

from shared.queue_helper import send_to_queue
from shared.api_helper import call_onboard_api
from shared.payload_builder import transform_records

app = func.FunctionApp()


# =========================================================
# ---------------- TIMER TRIGGER ---------------------------
# =========================================================

@app.timer_trigger(schedule=SCHEDULE, arg_name="myTimer")
def onboarding_timer(myTimer: func.TimerRequest):

    logging.info("========== ONBOARDING TIMER STARTED ==========")

    try:
        # Step 1: Get last watermark
        last_ts = get_last_timestamp()
        logging.info(f"Last watermark timestamp: {last_ts}")

        # Step 2: Fetch records from SP
        records = get_onboarding_data(last_ts)

        if not records:
            logging.info("No new onboarding records found")
            return

        logging.info(f"Total records fetched: {len(records)}")

        # Step 3: Group by EmployeeCode
        emp_groups = defaultdict(list)

        for r in records:
            emp_groups[r["EmployeeCode"]].append(r)

        grouped_records = list(emp_groups.values())

        # Step 4: Batch processing
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

            payload = {"records": flat_batch}

            send_to_queue(payload)

        logging.info("All batches pushed to queue")

    except Exception as e:
        logging.error(f"Timer error: {str(e)}")

    logging.info("========== ONBOARDING TIMER FINISHED ==========")


# =========================================================
# ---------------- QUEUE TRIGGER ---------------------------
# =========================================================

@app.queue_trigger(
    arg_name="azqueue",
    queue_name="onboarding-queue",
    connection="AzureWebJobsStorage"
)
def process_onboarding_batch(azqueue: func.QueueMessage):

    logging.info("========== ONBOARDING QUEUE STARTED ==========")

    try:
        payload = json.loads(azqueue.get_body().decode("utf-8"))
        records = payload.get("records", [])

        if not records:
            logging.warning("Empty batch received")
            return

        batch_size = len(records)

        # Step 1: Get max timestamp for watermark update
        try:
            max_ts = max(
                datetime.fromisoformat(str(r.get("updateDate_timestamp")))
                for r in records
                if r.get("updateDate_timestamp") is not None
            )
        except Exception:
            max_ts = datetime.now()

        # Step 2: Transform based on COUNTRY
        emp_data = transform_records(records, COUNTRY)

        # Step 3: Final payload
        final_payload = {
            "emp_data": emp_data,
            "subscription_name": SUBSCRIPTION_NAME
        }

        logging.info(f"Final Payload Sample: {json.dumps(final_payload)[:1000]}")

        # Step 4: Call API
        status_code, response = call_onboard_api(final_payload)

        logging.info(f"API Status: {status_code}")
        logging.info(f"API Response: {response}")

        # Step 5: Logging & watermark
        if status_code == 200:

            log_batch(batch_size, "SUCCESS","",1,json.dumps(final_payload))
            update_last_timestamp(max_ts)

        else:
            log_batch(batch_size, "FAILED",response,0,json.dumps(final_payload))
            raise Exception(f"API failed with status {status_code}")

    except Exception as e:
        logging.error(f"Queue processing error: {str(e)}")
        raise

    logging.info("========== ONBOARDING QUEUE FINISHED ==========")
