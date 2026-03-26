import requests
import logging

API_URL = "https://mservices-dev.zinghr.com/ztp/phil/overtime_calculation/"
API_TIMEOUT = 180

HEADERS = {
    "Content-Type": "application/json"
}

def call_attendance_api(payload):

    try:
        logging.info("Calling API...")

        response = requests.post(
            API_URL,
            json=payload,
            headers=HEADERS,
            timeout=API_TIMEOUT
        )

        return response.status_code, response.text

    except Exception as e:
        logging.error(f"API call failed: {e}")
        return 0, str(e)