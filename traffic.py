#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy
# pip install google-cloud-storage
# pip install google-cloud-bigquery
# pip install pandas-gbq

import pandas as pd
import os
from google.cloud import bigquery
import google.cloud.storage
from sodapy import Socrata
from pandas.io import gbq
from datetime import datetime

def _getToday():
        return datetime.now().strftime('%Y%m%d%H%M%S')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./service_account.json"

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.cityofchicago.org", None)

# Google Cloud Storage client creation and information
#storage_client = google.cloud.storage.Client()
storage_client = google.cloud.storage.Client.from_service_account_json(
        'service_account.json')
bucket_name = 'chi-traffic'
bucket = storage_client.get_bucket(bucket_name)

# Create storage bucket if it does not exist
if not bucket.exists():
    bucket.create()

# Create a BigQuery client and information
bigquery_client = bigquery.Client()
dataset_name = 'chitraffic_dataset'
table_name = 'chitraffic_table'

#dataset = bigquery_client.dataset(dataset_name)
#table = bigquery_client.Table(table_name)

# Create BigQuery dataset
#if not dataset.exists():
#    dataset.create()

# Example authenticated client (needed for non-public datasets):
# client = Socrata(data.cityofchicago.org,
#                  MyAppToken,
#                  userame="user@example.com",
#                  password="AFakePassword")

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("8v9j-bter", limit=2000)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)


# Create or overwrite the existing table if it exists
#table_schema = bigquery_client.Schema.from_dataframe(results_df)
#table.create(schema = table_schema, overwrite = True)

# Write the DataFrame to GCS (Google Cloud Storage)
results_df.to_csv('traffic' + _getToday() + '.csv')
source_file_name = 'traffic' + _getToday() + '.csv'
blob = bucket.blob(os.path.basename(source_file_name))
blob.upload_from_filename(source_file_name)

print('File {} uploaded to {}.'.format(
        source_file_name,
        bucket))

# Write the DataFrame to a BigQuery table
#table.insert_data(results_df)
#gbq.to_gbq(results_df, 'chicago_traffic.chitraffic', 'certain-region-147416', if_exists='append')

