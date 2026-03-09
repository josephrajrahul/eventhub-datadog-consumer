from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob import BlobServiceClient
import requests
import os
import time
import json

# -----------------------------
# Environment Variables
# -----------------------------
EVENT_HUB_CONNECTION = os.getenv("EVENT_HUB_CONNECTION")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")

DATADOG_API_KEY = os.getenv("DATADOG_API_KEY")

BLOB_CONNECTION = os.getenv("BLOB_CONNECTION")
BLOB_CONTAINER = "eventhub-checkpoints"

DATADOG_URL = f"https://http-intake.logs.us3.datadoghq.com/v1/input/{DATADOG_API_KEY}"

# -----------------------------
# Blob Checkpoint Store
# -----------------------------
blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION)
container_client = blob_service_client.get_container_client(BLOB_CONTAINER)

checkpoint_store = BlobCheckpointStore(container_client)

# -----------------------------
# Datadog Sender (Batch)
# -----------------------------
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

            print(f"Datadog returned {response.status_code}")

        except Exception as e:

            print(f"Datadog request failed: {e}")

        time.sleep(backoff)
        backoff *= 2

    return False


# -----------------------------
# EventHub Batch Processor
# -----------------------------
def on_event_batch(partition_context, events):

    logs = []

    for event in events:
        try:
            logs.append(event.body_as_str())
        except Exception as e:
            print("Error parsing event:", e)

    if not logs:
        return

    success = send_batch_to_datadog(logs)

    if success:

        print(f"Sent batch of {len(logs)} logs")

        partition_context.update_checkpoint()

    else:

        print("Batch failed after retries")


# -----------------------------
# Consumer Client
# -----------------------------
print("Starting EventHub consumer")

client = EventHubConsumerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION,
    consumer_group=CONSUMER_GROUP,
    eventhub_name=EVENT_HUB_NAME,
    checkpoint_store=checkpoint_store
)

# -----------------------------
# Receive events
# -----------------------------
with client:

    client.receive_batch(
        on_event_batch=on_event_batch,
        max_batch_size=100,
        max_wait_time=5,
        starting_position="-1"
    )
