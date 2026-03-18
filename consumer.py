from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob import BlobServiceClient
import requests
import os
import time
import threading
import queue

EVENT_HUB_CONNECTION = os.getenv("EVENT_HUB_CONNECTION")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")

DATADOG_API_KEY = os.getenv("DATADOG_API_KEY")
DATADOG_URL = f"https://http-intake.logs.datadoghq.com/api/v2/logs"

DATADOG_SOURCE = "azure.apimanagement"
DATADOG_SERVICE = "tst.apim.service"

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

BLOB_CONTAINER = "eventhub-checkpoints"

blob_account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

checkpoint_store = BlobCheckpointStore(
    blob_account_url,
    BLOB_CONTAINER,
    STORAGE_ACCOUNT_KEY
)

def send_to_datadog(events):

    logs = []

    for event in events:

        body = event.body_as_str()

        logs.append({
            "message": body,
            "ddsource": DATADOG_SOURCE,
            "service": DATADOG_SERVICE
        })

    try:

        response = requests.post(
            DATADOG_URL,
            headers={
                "Content-Type": "application/json",
                "DD-API-KEY": DATADOG_API_KEY
            },
            json=logs,
            timeout=10
        )

        print("Sent", len(logs), "logs to Datadog")

    except Exception as e:
        print("Datadog error:", e)


def on_event_batch(partition_context, events):

    if not events:
        return

    print(f"Received {len(events)} events")

    send_to_datadog(events)

    partition_context.update_checkpoint()


print("Starting EventHub consumer")

client = EventHubConsumerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION,
    consumer_group=CONSUMER_GROUP,
    eventhub_name=EVENT_HUB_NAME,
    checkpoint_store=checkpoint_store
)

with client:

    client.receive_batch(
        on_event_batch=on_event_batch,
        max_batch_size=500,
        max_wait_time=5,
        starting_position="@latest"
    )
