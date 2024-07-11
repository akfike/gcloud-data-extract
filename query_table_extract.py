from google.cloud import bigquery
import pandas as pd
import os

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "innate-summit-406402-4aeabd48c04d.json"

# Initialize the BigQuery client
client = bigquery.Client()

def download_table_in_chunks(project, dataset, table, rows_per_query=7000):
    table_id = f"{project}.{dataset}.{table}"
    offset = 0
    # Get the total number of rows in the table
    table_ref = client.get_table(table_id)
    total_rows = table_ref.num_rows
    chunk_number = 1

    while offset < total_rows:
        query = f"""
        SELECT * FROM `{table_id}`
        LIMIT {rows_per_query} OFFSET {offset}
        """
        query_job = client.query(query)

        # Convert the query result to a DataFrame
        df = query_job.to_dataframe()

        # Save the DataFrame to a CSV file
        file_name = f"{table}_part_{chunk_number}.csv"
        df.to_csv(file_name, index=False)
        print(f"Downloaded chunk {chunk_number}: {offset} to {offset + rows_per_query} rows, saved as {file_name}")

        # Increment the offset and chunk number
        offset += rows_per_query
        chunk_number += 1

# Specify the project, dataset, and table name
project = 'innate-summit-406402'
dataset = 'blockgroup_2010_5yr'
table = 'blockgroup_2011_5yr'

# Call the function to download the table in chunks
download_table_in_chunks(project, dataset, table)
