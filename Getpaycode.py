from google.cloud import bigquery, storage
import os
import pandas as pd
import datetime
import logging
import tempfile
import json
from flask import jsonify

# Constants
PROJECT_ID = 'st-npr-ukg-pro-data-hub-8100'
BUCKET_NAME = 'bqgcstest2212'
CHUNK_SIZE = 41943040  # 20 MB for resumable upload
MAX_ROWS_PER_BATCH = 100000  # Rows fetched per BigQuery page

# Set up logging
logging.basicConfig(level=logging.INFO)

def execute_query_with_dataframe(request):
    try:
        # Initialize BigQuery and Cloud Storage clients
        bq_client = bigquery.Client(project=PROJECT_ID)
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        
        #fetching the data from delta
        def fetch_query_and_metadata(bq_client):
            delta_table_query = """
            SELECT 
                query,
                records_per_chunk,
                file_prefix,
                JSON_EXTRACT_SCALAR(date_summary, '$[0].DATE_RUN') AS date_run,
            FROM `st-npr-ukg-pro-data-hub-8100.UKG.delta`
            WHERE query_id = 2
            """
            query_job = bq_client.query(delta_table_query)
            results = query_job.result()

            for row in results:
                return (
                    row.query, 
                    row.records_per_chunk, 
                    row.file_prefix, 
                    row.date_run,  
                )

            raise ValueError("No query, records_per_chunk, file prefix, or date_summary JSON found in the delta table.")
        
        #Generating a file prefix
        def generate_dynamic_file_prefix(base_prefix, file_count):
            current_time = datetime.datetime.now()
            incremented_time = current_time + datetime.timedelta(minutes=file_count)
            incremented_timestamp = incremented_time.strftime('%Y%m%d-%H%M')
            return f"{base_prefix}_{incremented_timestamp}"
        
        #Uploder to GCS
        def upload_to_gcs(file_count, data):
            try:
                file_name = generate_dynamic_file_prefix(base_prefix, file_count) + ".csv"
                blob = bucket.blob(file_name)
                blob.chunk_size = CHUNK_SIZE

                with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv') as temp_file:
                    pd.DataFrame(data).to_csv(temp_file.name, index=False)
                    temp_file_path = temp_file.name

                with open(temp_file_path, 'rb') as file_data:
                    blob.upload_from_file(file_data)
                logging.info(f"Uploaded file {file_name}")
            finally:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
        
        #-------------Process start-----------------#
        
        #everything is now stored in var
        base_query, records_per_chunk, base_prefix, date_run = fetch_query_and_metadata(bq_client)
        
        #Logic
        #Fullquery where the updateDtm is greater than the previous run
        final_query=f"""
        select * from {base_query}
        where CAST(updateDtm AS DATETIME) > CAST({date_run} AS DATETIME)
        """
        #executing the query
        query_job = bq_client.query(final_query)
        rows_iter = query_job.result(page_size=MAX_ROWS_PER_BATCH)
        accumulated_rows = []
        
        #results processing
        total_rows = 0
        file_count = 0
        for page in rows_iter.pages:
            rows = [dict(row) for row in page]
            accumulated_rows.extend(rows)
            total_rows += len(rows)

        #Fetches results are stored in a list and uploaded to GCS
        if accumulated_rows:
            upload_to_gcs(file_count, accumulated_rows)
            file_count += 1
        
        


        
        

        
            
        
            
            
            
            
            
            
            
            
            
            
            
        
        
        
        
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500