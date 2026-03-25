import azure.functions as func
import logging
import json
import uuid
import time
from datetime import datetime
from collections import defaultdict

from shared.db_helper import (
    get_last_timestamp,
    get_onboarding_data,
    update_last_timestamp,
    log_batch
)

from shared.queue_helper import send_to_queue
from shared.api_helper import call_attendance_api
from datetime import datetime, date
from decimal import Decimal

app = func.FunctionApp()

# ---------------- TIMER TRIGGER ----------------

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer")
def onboarding_timer(myTimer: func.TimerRequest):

    logging.info("========== ONBOARDING TIMER STARTED ==========")

    try:
        last_ts = get_last_timestamp()
        logging.info(f"Last watermark timestamp: {last_ts}")

        records = get_onboarding_data(last_ts)

        if not records:
            logging.info("No new onboarding records found")
            return

        logging.info(f"Total records fetched: {len(records)}")

        emp_groups = defaultdict(list)

        for r in records:
            emp_groups[r["EmployeeCode"]].append(r)

        grouped_records = list(emp_groups.values())

        batch_size = 1

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


# ---------------- QUEUE TRIGGER ----------------

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
            return

        batch_size = len(records)

        max_ts = max(
            datetime.fromisoformat(str(r["updateDate_timestamp"]))
            for r in records
        )

        emp_data = []

        for row in records:

            emp_data.append({
                "DOJ": str(row.get("DOJ", ""))[:10],
                "from_date": str(row.get("FromDate", ""))[:10],

                "absent_deduction_applicable": int(row.get("Absentdeductionapplicable", 0)),
                "country_id": int(row.get("CountryId", 0)),
                "employee_code": str(row.get("EmployeeCode", "")),
                "employee_id": int(row.get("EmployeeID", 0)),
                "exemption": int(row.get("Exemption", 0)),
                "gender": int(row.get("Gender", 0)),

                "income_tax_applicable": int(row.get("Incometaxapplicable", 0)),
                "leave_encash_applicable": int(row.get("LeenPayApp", 0)),
                "ot_applicable": int(row.get("Otapplicable", 1)),

                "monthly_rate": float(row.get("MonthlyRate", 0)),
                "yearly_rate": float(row.get("YearlyRate", 0)),

                "payhead_category_id": int(row.get("PayHeadCategoryID", 0)),
                "payhead_code": str(row.get("PayheadCode", "")).lower(),
                "payhead_id": int(row.get("PayheadId", 0)),

                "pag_ibig_hdmf_employee": int(row.get("Pagibighdmfemployee", 0)),
                "pag_ibig_hdmf_employer": int(row.get("Pagibighdmfemployer", 0)),

                "philhealth_employee": int(row.get("Philhealthemployee", 0)),
                "philhealth_employer": int(row.get("Philhealthemployer", 0)),

                "sss_ecr_contri": int(row.get("Sssecercontri", 0)),
                "sss_employee": int(row.get("Sssemployee", 0)),
                "sss_employer": int(row.get("Sssemployer", 0)),

                "voluntary_employee_contribution_app": int(row.get("Voluntaryemployeecontributionapp", 0)),
                "workers_investment_and_savings_program_app": int(row.get("Workersinvestmentandsavingsprogramapp", 0)),

                "w_tax": int(row.get("Wtax", 0))
            })

        final_payload = {
            "emp_data": emp_data,
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

    logging.info("========== ONBOARDING QUEUE FINISHED ==========")