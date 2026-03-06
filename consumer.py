from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob import BlobServiceClient
import requests
import os
import time

# Event Hub configuration
connection_str = os.getenv("EVENT_HUB_CONNECTION")
eventhub_name = os.getenv("EVENT_HUB_NAME")
consumer_group = os.getenv("CONSUMER_GROUP")

# Datadog configuration
datadog_api_key = os.getenv("DATADOG_API_KEY")
datadog_url = f"https://http-intake.logs.us3.datadoghq.com/v1/input/{datadog_api_key}"

# Blob checkpoint configuration
blob_connection = os.getenv("BLOB_CONNECTION")
blob_container = "eventhub-checkpoints"

blob_service_client = BlobServiceClient.from_connection_string(blob_connection)

checkpoint_store = BlobCheckpointStore(
    blob_service_client,
    container_name=blob_container
)

# Retry function for Datadog
def send_to_datadog(log):

    retries = 3

    for attempt in range(retries):
        try:
            response = requests.post(
                datadog_url,
                headers={"Content-Type": "application/json"},
                data=log,
                timeout=5
            )

            if response.status_code == 200:
                return True

            print("Datadog error:", response.status_code)

        except Exception as e:
            print("Datadog request failed:", e)

        time.sleep(2)

    return False


def on_event(partition_context, event):

    log = event.body_as_str()

    if send_to_datadog(log):

        print("Log sent")

        # checkpoint only after successful send
        partition_context.update_checkpoint(event)

    else:

        print("Failed to send log after retries")


client = EventHubConsumerClient.from_connection_string(
    conn_str=connection_str,
    consumer_group=consumer_group,
    eventhub_name=eventhub_name,
    checkpoint_store=checkpoint_store
)

with client:
    client.receive(
        on_event=on_event,
        starting_position="-1"
    )
