import json
import pandas as pd
from google.cloud import storage, bigquery
import os
from datetime import datetime
import tempfile

# Define GCS bucket, folder path, and project name
project_name = 'prj-p-dmo-dwh-restricted-4hj8'
bucket_name = 'mat-prod-me-central2-rac-azure'
folder_path = 'flight/'  # Folder containing JSON files
sa_key_path = 'SA_KEY/prj-p-dmo-dwh-restricted-4hj8-e1fe81a1e3b6.json'  # Path to the service account key in GCS

# Initialize GCS client with default credentials
initial_client = storage.Client()

# Download the service account key from GCS
bucket = initial_client.bucket(bucket_name)
sa_key_blob = bucket.blob(sa_key_path)

# Create a temporary file to store the service account key
with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
    sa_key_content = sa_key_blob.download_as_text()
    temp_file.write(sa_key_content)
    temp_file_path = temp_file.name

# Set Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file_path

# Initialize GCS client with the downloaded service account key
client = storage.Client(project=project_name)
bucket = client.bucket(bucket_name)

# List all JSON files in the flight folder
blobs = list(bucket.list_blobs(prefix=folder_path))
all_data = []

for blob in blobs:
    if blob.name.endswith('.json') and blob.name.startswith('flight/Matarat_'):
        print(f"Processing file: {blob.name}")  # Print file name being processed
        try:
            json_data = blob.download_as_text()
            data = json.loads(json_data)
            for record in data:
                record["FILE"] = blob.name  # Add file name
                record["CREATED_DT"] = datetime.now().isoformat()  # Add created timestamp
                record["UPDATED_DT"] = datetime.now().isoformat()  # Add updated timestamp
            all_data.extend(data)  # Append data from each file

            # Move the processed file to the new location
            new_blob_name = blob.name.replace('flight/', 'flight/Processed/')
            new_blob = bucket.blob(new_blob_name)
            new_blob.rewrite(blob)
            blob.delete()
            print(f"Moved {blob.name} to {new_blob_name}")

        except Exception as e:
            print(f"Error processing {blob.name}: {str(e)}")
            continue

# Convert JSON to DataFrame
df = pd.DataFrame(all_data)

# Ensure column names match case of schema
df.columns = [col.upper() for col in df.columns]

# Convert timestamp columns to datetime format (ensure proper type)
df['SCHEDULE_DATE_TIME'] = pd.to_datetime(df['SCHEDULE_DATE_TIME'], errors='coerce')
df['ACTUAL_DATE_TIME'] = pd.to_datetime(df['ACTUAL_DATE_TIME'], errors='coerce')
df['CREATED_DT'] = pd.to_datetime(df['CREATED_DT'], errors='coerce')
df['UPDATED_DT'] = pd.to_datetime(df['UPDATED_DT'], errors='coerce')

# Initialize BigQuery client
bq_client = bigquery.Client(project=project_name)

table_id = 'staging.stg_rac_actual_flights'  # Use dataset.table format

# Define schema explicitly, moving FILE to third-last position
schema = [
    bigquery.SchemaField("SCHEDULE_DATE_TIME", "TIMESTAMP"),
    bigquery.SchemaField("ACTUAL_DATE_TIME", "TIMESTAMP"),
    bigquery.SchemaField("DIRECTION", "STRING"),
    bigquery.SchemaField("FLIGHT_NUMBER", "STRING"),
    bigquery.SchemaField("ROUTE_AIRPORT_CODE", "STRING"),
    bigquery.SchemaField("ROUTE_AIRPORT_NAME", "STRING"),
    bigquery.SchemaField("ROUTE_CITY_CODE", "STRING"),
    bigquery.SchemaField("ROUTE_CITY_NAME", "STRING"),
    bigquery.SchemaField("ROUTE_COUNTRY_CODE", "STRING"),
    bigquery.SchemaField("ROUTE_COUNTRY_NAME", "STRING"),
    bigquery.SchemaField("FLIGHT_TYPE_CODE", "STRING"),
    bigquery.SchemaField("FLIGHT_TYPE_DESCRIPTION", "STRING"),
    bigquery.SchemaField("FLIGHT_CATEGORY_CODE", "STRING"),
    bigquery.SchemaField("FLIGHT_CATEGORY_DESCRIPTION", "STRING"),
    bigquery.SchemaField("TOTAL_REVENUE_PAX", "FLOAT64"),
    bigquery.SchemaField("HAJ_PAX", "FLOAT64"),
    bigquery.SchemaField("UMRAH_PAX", "FLOAT64"),
    bigquery.SchemaField("ACTUAL_CARGO", "FLOAT64"),
    bigquery.SchemaField("ACTUAL_MAIL", "FLOAT64"),
    bigquery.SchemaField("LOAD_FACTOR", "FLOAT64"),
    bigquery.SchemaField("AIRCRAFT_SEATS_CAPACITY", "FLOAT64"),
    bigquery.SchemaField("AIRLINE_CODE", "STRING"),
    bigquery.SchemaField("TRANSFER_PAX", "FLOAT64"),
    bigquery.SchemaField("AIRLINE_NAME", "STRING"),
    bigquery.SchemaField("OTP", "STRING"),
    bigquery.SchemaField("CREATED_DT", "DATETIME"),
    bigquery.SchemaField("UPDATED_DT", "DATETIME"),
    bigquery.SchemaField("FILE", "STRING")  # Moved FILE to the third-last position
]

# Load DataFrame into BigQuery using BigQuery client
job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # Wait for the job to complete

print("Data loaded successfully into BigQuery table.")

# Clean up the temporary file
os.unlink(temp_file_path)