import os
import requests
from google.cloud import storage

# Constants
BUCKET_NAME = "de_zoomcamp_yellow_taxi_us"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
DATA_TYPES = {
    "yellow": {"years": [2019, 2020], "prefix": "yellow_tripdata"},
    "green": {"years": [2019, 2020], "prefix": "green_tripdata"},
    "fhv": {"years": [2019], "prefix": "fhv_tripdata"},
}

# Initialize GCS client
CREDENTIALS_FILE = "week_3/gcs.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the GCS bucket."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def download_and_upload_data():
    for data_type, details in DATA_TYPES.items():
        prefix = details["prefix"]
        years = details["years"]

        for year in years:
            for month in range(1, 13):
                month_str = f"{month:02d}"  # Format month as two digits (e.g., 01, 02, ..., 12)
                file_name = f"{prefix}_{year}-{month_str}.csv.gz"
                url = f"{BASE_URL}/{data_type}/{file_name}"
                local_file_path = f"/tmp/{file_name}"  # Temporary local storage

                print(f"Downloading {url}...")
                try:
                    # Download the file
                    response = requests.get(url)
                    response.raise_for_status()

                    # Save the file locally
                    with open(local_file_path, "wb") as file:
                        file.write(response.content)

                    # Upload the file to GCS
                    gcs_blob_name = f"{data_type}/{file_name}"
                    upload_to_gcs(BUCKET_NAME, local_file_path, gcs_blob_name)

                    # Clean up the local file
                    os.remove(local_file_path)
                except requests.exceptions.HTTPError as err:
                    print(f"Failed to download {url}: {err}")
                except Exception as e:
                    print(f"An error occurred: {e}")

if __name__ == "__main__":
    download_and_upload_data()