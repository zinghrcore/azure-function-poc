import azure.functions as func
import logging
import json
import uuid
import time
from datetime import datetime

from shared.db_helper import (
    get_last_timestamp,
    get_updated_records,
    create_payload,
    update_last_timestamp,
    log_batch
)

from shared.queue_helper import send_to_queue
from shared.api_helper import call_attendance_api

app = func.FunctionApp()

# ---------------- TIMER TRIGGER ----------------

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer",run_on_startup=False)
def attendance_timer(myTimer: func.TimerRequest):

    logging.info("========== TIMER TRIGGER STARTED ==========")
    
    # wait for queue worker to start
    time.sleep(5)

    try:

        last_ts = get_last_timestamp()
        logging.info(f"Last timestamp from watermark: {last_ts}")

        records = get_updated_records(last_ts)

        if not records:
            logging.info("No new attendance updates found")
            return

        logging.info(f"Total records fetched: {len(records)}")

        batch_size = 50

        for i in range(0, len(records), batch_size):

            batch = records[i:i + batch_size]

            payload = create_payload(batch)

            logging.info(f"Pushing batch of {len(batch)} records to queue")

            send_to_queue(payload)

        logging.info(f"{len(records)} records pushed to queue successfully")

    except Exception as e:

        logging.error(f"Timer trigger failed: {str(e)}")

    logging.info("========== TIMER TRIGGER FINISHED ==========")

# ---------------- QUEUE TRIGGER ----------------

@app.queue_trigger(
    arg_name="azqueue",
    queue_name="attendance-queue",
    connection="AzureWebJobsStorage"
)

def process_attendance_batch(azqueue: func.QueueMessage):

    logging.info("========== QUEUE TRIGGER STARTED ==========")

    batch_id = str(uuid.uuid4())

    try:

        # Read queue message
        message_body = azqueue.get_body().decode("utf-8")

        logging.info(f"Queue Message: {message_body}")

        payload = json.loads(message_body)

        records = payload.get("records", [])

        batch_size = len(records)

        logging.info(f"Processing batch with {batch_size} records")
        logging.info(f"Batch ID: {batch_id}")

        # ---------------- CAPTURE TIMESTAMP FOR WATERMARK ----------------

        max_ts = max(
        datetime.fromisoformat(r["updateDate_timestamp"])
        for r in records
        )

        # ---------------- REMOVE INTERNAL FIELD BEFORE API ----------------

        for r in records:
            r.pop("updateDate_timestamp", None)

        # ---------------- CALL ATTENDANCE API ----------------

        logging.info("Calling Attendance API...")

        logging.info(f"Final Payload Sent To API: {json.dumps(payload)}")

        status_code, response = call_attendance_api(payload)

        logging.info(f"API Status Code: {status_code}")
        logging.info(f"API Response: {response}")

        if status_code == 200:

            logging.info("API call successful")

            # Log success
            log_batch(
                batch_id,
                batch_size,
                "SUCCESS",
                str(response)
            )

            # Update watermark
            update_last_timestamp(max_ts)

            logging.info("Watermark updated")

        else:

            logging.error("API returned non-200 response")

            log_batch(
                batch_id,
                batch_size,
                "FAILED",
                str(response)
            )

            raise Exception(f"API returned status {status_code}")

    except Exception as e:

        logging.error(f"Queue processing error: {str(e)}")

        try:
            log_batch(
                batch_id,
                batch_size,
                "ERROR",
                str(e)
            )
        except:
            logging.error("Failed to log batch error")

        raise

    logging.info("========== QUEUE TRIGGER FINISHED ==========")