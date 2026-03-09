from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob import BlobServiceClient
import requests
import os
import time

# Validate environment variables
required_env = [
    "EVENT_HUB_CONNECTION",
    "EVENT_HUB_NAME",
    "CONSUMER_GROUP",
    "DATADOG_API_KEY",
    "BLOB_CONNECTION"
]

for var in required_env:
    if not os.getenv(var):
        raise ValueError(f"Missing environment variable: {var}")

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

container_client = blob_service_client.get_container_client(blob_container)

checkpoint_store = BlobCheckpointStore(
    container_client,
    blob_container
)
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

            print(f"Datadog error {response.status_code}, retry {attempt+1}")

        except Exception as e:
            print("Datadog request failed:", e)

        time.sleep(2)

    return False


def on_event(partition_context, event):

    log = event.body_as_str()

    if send_to_datadog(log):

        print("Log sent")

        partition_context.update_checkpoint(event)

    else:

        print("Failed to send log after retries")


print("Starting EventHub consumer...")
print("EventHub:", eventhub_name)
print("Consumer group:", consumer_group)

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
