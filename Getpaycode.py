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
        
        
        # Fetches and ammends the query
        def FetchandAppendQuery(bq_client):
            #fetching the query from delta table
            fetch_query = """
            SELECT query
            FROM `st-npr-ukg-pro-data-hub-8100.UKG.delta`
            WHERE query_id = 2
            """
            #executing the query
            query_job = bq_client.query(fetch_query)
            results = query_job.result()
            
            #Storing the query in a var
            query_from_table = None
            for row in results:
                query_from_table = row["query"]
                break
            
            if not query_from_table:
                raise ValueError("No query found for query_id = 2")
            
            #query to get the lates updateDtm
            datequery = """
            SELECT date_summary
            FROM `st-npr-ukg-pro-data-hub-8100.UKG.delta`
            WHERE query_id = 5
            """
            
            #executing the getupdateDtm query
            query_job = bq_client.query(datequery)
            results = query_job.result()
            
            # Fetch the first row
            first_row = next(results)

            # Convert DATE_RUN to a date object
            date_run_str = first_row["DATE_RUN"]
            #recent_date_run = datetime.datetime.strptime(date_run_str, "%Y-%m-%d").date()
            
            finalquery = """
            SELECT * FROM 
            {query_from_table}
            WHERE updateDtm > {date_run_str}
            """
            
            
            
            
            
            
            
            
            
            
            
        
        
        
        
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500