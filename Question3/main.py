import base64
import json
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# BigQuery table info
dataset_id = "honnalesujatha_analytics_ds"
table_id = "honnalesujatha_user_activity"

def pubsub_to_bigquery(event, context):
    """Triggered from a message on a Pub/Sub topic."""
    if 'data' in event:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)

        # Insert row into BigQuery
        table_ref = f"{client.project}.{dataset_id}.{table_id}"
        rows_to_insert = [{
            "event": data.get("event"),
            "user": data.get("user"),
            "ts": data.get("ts")
        }]
        errors = client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            print(f"Encountered errors: {errors}")
        else:
            print(f"Inserted: {rows_to_insert}")
    else:
        print("No data found in the Pub/Sub message.")
