from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob import BlobServiceClient
import requests
import os
import time
import json

# EventHub configuration
EVENT_HUB_CONNECTION = os.getenv("EVENT_HUB_CONNECTION")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")

# Datadog configuration
DATADOG_API_KEY = os.getenv("DATADOG_API_KEY")
DATADOG_URL = f"https://http-intake.logs.us3.datadoghq.com/v1/input/{DATADOG_API_KEY}"

# Blob storage
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

BLOB_CONTAINER = "eventhub-checkpoints"

blob_account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

checkpoint_store = BlobCheckpointStore(
    blob_account_url,
    BLOB_CONTAINER,
    STORAGE_ACCOUNT_KEY
)

# Retry send to Datadog
def send_batch_to_datadog(logs):

    retries = 5
    backoff = 2

    for attempt in range(retries):

        try:
            response = requests.post(
                DATADOG_URL,
                headers={"Content-Type": "application/json"},
                data="\n".join(logs),
                timeout=10
            )

            if response.status_code == 200:
                return True

        except Exception as e:
            print("Datadog error:", e)

        time.sleep(backoff)
        backoff *= 2

    return False


def on_event_batch(partition_context, events):

    logs = []

    for event in events:
        logs.append(event.body_as_str())

    if not logs:
        return

    if send_batch_to_datadog(logs):

        print(f"Sent batch of {len(logs)} logs")

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
        max_batch_size=100,
        max_wait_time=5,
        starting_position="-1"
    )
