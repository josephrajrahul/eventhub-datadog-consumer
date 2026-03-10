from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob import BlobServiceClient
import requests
import os
import time
import threading
import queue

# ========================
# CONFIG
# ========================

EVENT_HUB_CONNECTION = os.getenv("EVENT_HUB_CONNECTION")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")

DATADOG_API_KEY = os.getenv("DATADOG_API_KEY")
DATADOG_URL = f"https://http-intake.logs.us3.datadoghq.com/v1/input/{DATADOG_API_KEY}"

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

BLOB_CONTAINER = "eventhub-checkpoints"

blob_account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

checkpoint_store = BlobCheckpointStore(
    blob_account_url,
    BLOB_CONTAINER,
    STORAGE_ACCOUNT_KEY
)

# ========================
# PERFORMANCE SETTINGS
# ========================

BATCH_SIZE = 200
FLUSH_INTERVAL = 2
WORKER_THREADS = 4
SEND_TIMEOUT = 10

log_queue = queue.Queue()

# ========================
# DATADOG SEND FUNCTION
# ========================

def send_to_datadog(batch):

    retries = 5
    backoff = 2

    payload = "\n".join(batch)

    for attempt in range(retries):

        try:

            response = requests.post(
                DATADOG_URL,
                headers={"Content-Type": "application/json"},
                data=payload,
                timeout=SEND_TIMEOUT
            )

            if response.status_code == 200:

                print(f"Sent {len(batch)} logs to Datadog")

                return True

            print("Datadog error:", response.status_code)

        except Exception as e:

            print("Datadog exception:", e)

        time.sleep(backoff)
        backoff *= 2

    return False


# ========================
# WORKER THREAD
# ========================

def worker():

    batch = []
    last_flush = time.time()

    while True:

        try:
            log = log_queue.get(timeout=1)
            batch.append(log)
            log_queue.task_done()

        except queue.Empty:
            pass

        now = time.time()

        if len(batch) >= BATCH_SIZE or (batch and now - last_flush >= FLUSH_INTERVAL):

            send_to_datadog(batch)

            batch = []
            last_flush = now


# start workers
for _ in range(WORKER_THREADS):
    threading.Thread(target=worker, daemon=True).start()


# ========================
# EVENT HUB HANDLER
# ========================

event_counter = 0

def on_event_batch(partition_context, events):

    global event_counter

    if events:
        print(f"Received {len(events)} events")

    for event in events:

        log = event.body_as_str()

        log_queue.put(log)

        event_counter += 1

        # checkpoint every 100 events
        if event_counter % 100 == 0:
            partition_context.update_checkpoint()


# ========================
# START CONSUMER
# ========================

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
