import json
import os
from azure.storage.queue import QueueClient

QUEUE_NAME = "overtime-queue"

def send_to_queue(message):

    conn_str = os.getenv("AzureWebJobsStorage")
    queue = QueueClient.from_connection_string(conn_str, QUEUE_NAME)

    try:
        queue.create_queue()
    except:
        pass

    queue.send_message(json.dumps(message, default=str))
