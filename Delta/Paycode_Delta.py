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
        
        def fetch_query_and_metadata(bq_client):
            delta_table_query = """
            SELECT 
                query,
                records_per_chunk,
                file_prefix,
                delta_condition AS last_run
            FROM `st-npr-ukg-pro-data-hub-8100.UKG.delta`
            WHERE query_id = 3
            """
            query_job = bq_client.query(delta_table_query)
            results = query_job.result()

            for row in results:
                return (
                    row.query, 
                    row.records_per_chunk, 
                    row.file_prefix, 
                    row.last_run,  
                )

            raise ValueError("No query, records_per_chunk, file prefix, or date_summary JSON found in the delta table.")
        
        #Generating a file prefix
        def generate_dynamic_file_prefix(base_prefix, file_count):
            current_time = datetime.datetime.now()
            incremented_time = current_time + datetime.timedelta(hours=file_count)
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
                    
        #updates the datetime for the next run
        def update_delta_condition(next_run):
             # Ensure next_run is a string
            
            next_run = next_run.isoformat()
                
                
            update_query = """
            UPDATE `st-npr-ukg-pro-data-hub-8100.UKG.delta`
            SET delta_condition = @next_run 
            WHERE query_id = 3
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("next_run", "STRING", next_run)
                ]
            )
            
            query_job = bq_client.query(update_query, job_config=job_config)
            query_job.result()  # Wait for the query to finish
            logging.info("Update successful")
            
                    
        #-------------Process start-----------------#
        
        #everything is now stored in var
        base_query, records_per_chunk, base_prefix, last_run = fetch_query_and_metadata(bq_client)
        
        if last_run:
            if isinstance(last_run, str):
                    last_run = datetime.datetime.strptime(last_run, "%Y-%m-%dT%H:%M:%S.%f")
            next_run = last_run + datetime.timedelta(hours=1)
        
        
        #Logic
        #Fullquery where the updateDtm is greater than the previous run
        final_query = f"""
        SELECT * FROM ({base_query})
        WHERE CAST(updateDtm as DATETIME) > CAST(@last_run AS DATETIME)
        """
        
        job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("last_run", "DATETIME", last_run),
            bigquery.ScalarQueryParameter("next_run", "DATETIME", next_run)
        ]
        )  
        
        #executing the query
        query_job = bq_client.query(final_query,job_config=job_config)
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
            
        #finaly update the date for the next run
        update_delta_condition(next_run)
        
        return jsonify({
                "message": "CSV files created successfully",
                "total_rows": total_rows,
                "file_count": file_count
            }), 200
        
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500