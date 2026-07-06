import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))
datasets = list(client.list_datasets())
print(f"Connected to project: {client.project}")
print(f"Datasets: {[d.dataset_id for d in datasets]}")