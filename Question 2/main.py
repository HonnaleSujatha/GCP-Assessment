import base64
import json
import logging
from google.cloud import bigquery
from google.cloud import pubsub_v1

# Initialize clients
bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()

PROJECT_ID = "pavanproject-481304"
DATASET = "realtime_sujatha"
TABLE = "user_events_sujatha"
DLQ_TOPIC = f"projects/{PROJECT_ID}/topics/user-activity-dlq-sujatha"


def ingest_user_activity(event, context):
    try:
        # Decode Pub/Sub message
        message = base64.b64decode(event["data"]).decode("utf-8")
        data = json.loads(message)

        # Explicit validation
        if not all(k in data for k in ("event", "user", "ts")):
            raise ValueError("Missing required fields")

        row = {
            "event": data["event"],
            "user": data["user"],
            "ts": data["ts"]
        }

        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
        errors = bq_client.insert_rows_json(table_id, [row])

        if errors:
            raise RuntimeError(errors)

        logging.info("Inserted row successfully")

    except Exception as e:
        logging.error(f"Invalid message: {e}")

        # Send original message to DLQ
        publisher.publish(
            DLQ_TOPIC,
            message.encode("utf-8")
        )
