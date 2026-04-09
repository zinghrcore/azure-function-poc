import json
import os
from azure.storage.queue import QueueClient

QUEUE_NAME = "attendance-queue"

def send_to_queue(message: dict):
    conn_str = os.getenv("AzureWebJobsStorage")
    queue = QueueClient.from_connection_string(conn_str,QUEUE_NAME)
    queue.send_message(json.dumps(message))
