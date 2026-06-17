import os
import json

# ---------------- DATABASE ----------------
#SQL_CONFIG = {
#    "server": os.getenv("DB_SERVER"),
#    "database": os.getenv("DB_NAME"),
#    "username": os.getenv("DB_USER"),
#    "password": os.getenv("DB_PASSWORD"),
#    "driver": os.getenv("DB_DRIVER", "{ODBC Driver 18 for SQL Server}")
#}

DATABASES = json.loads(
    os.getenv("DATABASES", "[]")
)

# ---------------- API ----------------
API_URL = os.getenv("API_URL")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "180"))

# ---------------- QUEUE ----------------
QUEUE_NAME = os.getenv("QUEUE_NAME", "salary-revision-queue")

# ---------------- FUNCTION ----------------
SCHEDULE = os.getenv("TIMER_SCHEDULE", "0 */5 * * * *")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))

# ---------------- APP ----------------
#SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME")
#COUNTRY = os.getenv("COUNTRY")