import os
import requests

HEADERS = {
    "Content-Type": "application/json"
}

def call_attendance_api(payload):

    API_URL = os.getenv("API_URL")
    API_TIMEOUT = int(os.getenv("API_TIMEOUT", "180"))

    try:

        if not API_URL:
            raise Exception("API_URL is not configured")

        response = requests.post(
            API_URL,
            json=payload,
            headers=HEADERS,
            timeout=API_TIMEOUT
        )

        return response.status_code, response.text

    except Exception as e:

        return 0, str(e)