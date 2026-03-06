from azure.eventhub import EventHubConsumerClient
import requests
import os

connection_str = os.getenv("EVENT_HUB_CONNECTION")
eventhub_name = os.getenv("EVENT_HUB_NAME")
consumer_group = os.getenv("CONSUMER_GROUP")

datadog_api_key = os.getenv("DATADOG_API_KEY")
datadog_url = f"https://http-intake.logs.datadoghq.com/v1/input/{datadog_api_key}"

def on_event(partition_context, event):

    log = event.body_as_str()

    requests.post(
        datadog_url,
        headers={"Content-Type": "application/json"},
        data=log
    )

    print("Log sent")

    partition_context.update_checkpoint(event)


client = EventHubConsumerClient.from_connection_string(
    conn_str=connection_str,
    consumer_group=consumer_group,
    eventhub_name=eventhub_name,
)

with client:
    client.receive(on_event=on_event)
