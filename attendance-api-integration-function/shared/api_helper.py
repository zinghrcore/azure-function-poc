import requests
import logging

API_URL = "https://mservices-qa.zinghr.com/ztp/phil/attendance-change/"
API_TIMEOUT = 180

HEADERS = {
    "Content-Type": "application/json"
}

def call_attendance_api(payload):
    try:

        response = requests.post(
            API_URL,
            json=payload,
            headers=HEADERS,
            timeout=API_TIMEOUT
        )

        return response.status_code, response.text

    except Exception as e:

        return 0, str(e)