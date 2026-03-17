import logging
import json
import azure.functions as func

def main(msg: func.QueueMessage):
    try:
        # Parse the incoming message
        message_content = msg.get_body().decode('utf-8')
        data = json.loads(message_content)

        batch_id = data.get("batch_id")
        emp_codes = data.get("emp_codes", [])

        logging.info(f"Processing batch {batch_id} with {len(emp_codes)} employees")
        
        # TODO: Add your attendance API call here
        # Example:
        # for emp_code in emp_codes:
        #     call_attendance_api(emp_code)

        logging.info(f"Batch {batch_id} processed successfully.")

    except Exception as e:
        logging.error(f"Error processing queue message: {e}")
        raise e  # Re-raise so Azure Functions can handle retries