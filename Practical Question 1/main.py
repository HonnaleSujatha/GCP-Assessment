import functions_framework
from google.cloud import firestore
import logging

db = firestore.Client()

@functions_framework.cloud_event
def file_upload_trigger(cloud_event):
    data = cloud_event.data

    file_name = data["name"]
    file_size = data["size"]
    bucket_name = data["bucket"]

    doc_ref = db.collection("file_metadata").add({
        "file_name": file_name,
        "file_size": int(file_size),
        "bucket_name": bucket_name
    })

    logging.info(
        f"Processed file: {file_name}, Size: {file_size}, Bucket: {bucket_name}"
    )
