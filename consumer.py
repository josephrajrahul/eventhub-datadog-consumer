from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob import BlobServiceClient
import requests
import os
import time
import json
import threading
import queue
import hashlib

# ================================
# ENV VARIABLES
# ================================
EVENT_HUB_CONNECTION = os.getenv("EVENT_HUB_CONNECTION")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")

DATADOG_API_KEY = os.getenv("DATADOG_API_KEY")
DATADOG_URL = "https://http-intake.logs.datadoghq.com/api/v2/logs"

DATADOG_SOURCE = "azure.apimanagement"
DATADOG_SERVICE = "tst.apim.service"

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

BLOB_CONTAINER = "eventhub-checkpoints"

# ================================
# CHECKPOINT STORE
# ================================
blob_account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

checkpoint_store = BlobCheckpointStore(
    blob_account_url,
    BLOB_CONTAINER,
    STORAGE_ACCOUNT_KEY
)

# ================================
# HELPER FUNCTION - HASH
# ================================
def generate_hash(body):
    return hashlib.md5(body.encode()).hexdigest()

# ================================
# SEND LOGS TO DATADOG
# ================================
def send_to_datadog(events, partition_context):

    logs = []

    for event in events:
        try:
            body = event.body_as_str()

            # 🔥 Parse APIM log (IMPORTANT)
            log_json = json.loads(body)

            # 🔥 Add Datadog + duplicate detection fields
            log_json.update({
                "ddsource": DATADOG_SOURCE,
                "service": DATADOG_SERVICE,

                # ✅ Duplicate detection
                "event_id": event.sequence_number,
                "partition_id": partition_context.partition_id,
                "log_hash": generate_hash(body),

                # ✅ Timestamp
                "timestamp": int(time.time() * 1000)
            })

            logs.append(log_json)

        except Exception as e:
            print("❌ Error processing event:", e)

    if not logs:
        return

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

        if 200 <= response.status_code < 300:
            print(f"✅ Sent {len(logs)} logs to Datadog")
        else:
            print(f"❌ Datadog API error: {response.status_code} - {response.text}")

    except Exception as e:
        print("❌ Datadog request failed:", e)

# ================================
# EVENT HUB CALLBACK
# ================================
def on_event_batch(partition_context, events):

    if not events:
        return

    print(f"📦 Received {len(events)} events from partition {partition_context.partition_id}")

    # Send logs
    send_to_datadog(events, partition_context)

    # ✅ Update checkpoint AFTER successful send
    try:
        partition_context.update_checkpoint()
        print(f"✅ Checkpoint updated for partition {partition_context.partition_id}")
    except Exception as e:
        print("❌ Checkpoint update failed:", e)


# ================================
# MAIN CONSUMER
# ================================
print("🚀 Starting EventHub consumer...")

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
