import requests
import logging
from shared.config import API_URL, API_TIMEOUT

HEADERS = {
    "Content-Type": "application/json"
}

def call_variable_api(payload):

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