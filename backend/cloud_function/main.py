import functions_framework
from google.cloud import storage

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    if "part" in name and ".csv" in name:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.blob(name)
        splitted_name = name.split('/')
        new_name = f'results/{splitted_name[1]}/{splitted_name[2]}/result.csv'
        new_blob = bucket.rename_blob(blob, new_name)

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
