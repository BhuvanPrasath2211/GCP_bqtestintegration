# Code for openshift #
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
MAX_WORKERS = 10  # Number of parallel upload workers

# Set up logging
logging.basicConfig(level=logging.INFO)

def execute_query_with_dataframe(request):
    try:
        # Initialize BigQuery and Cloud Storage clients
        bq_client = bigquery.Client(project=PROJECT_ID)
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)

        # Update the date_summary field
        def update_date_summary(bq_client):
            fetch_query = """
            SELECT query
            FROM `st-npr-ukg-pro-data-hub-8100.UKG.delta`
            WHERE query_id = 5
            """
            query_job = bq_client.query(fetch_query)
            results = query_job.result()

            query_from_table = None
            for row in results:
                query_from_table = row["query"]
                break

            if not query_from_table:
                raise ValueError("No query found for query_id = 5")

            amended_query = f"""
            SELECT DISTINCT(CAST(cdc_updateDtm AS DATE)) AS DATE_RUN,
                   MIN(cdc_srcPartitionDate) AS min_date,
                   MAX(cdc_srcPartitionDate) AS max_date
            FROM (
                {query_from_table}
            ) z
            GROUP BY CAST(cdc_updateDtm AS DATE)
            ORDER BY DATE_RUN
            """
            query_job = bq_client.query(amended_query)
            results = query_job.result()

            date_summary = [
                {
                    "DATE_RUN": row["DATE_RUN"].isoformat() if row["DATE_RUN"] else None,
                    "min_date": row["min_date"].isoformat() if row["min_date"] else None,
                    "max_date": row["max_date"].isoformat() if row["max_date"] else None,
                }
                for row in results
            ]
            date_summary_json = json.dumps(date_summary)

            update_query = """
            UPDATE `st-npr-ukg-pro-data-hub-8100.UKG.delta`
            SET date_summary = @date_summary
            WHERE query_id = 5
            """
            query_parameters = [
                bigquery.ScalarQueryParameter("date_summary", "STRING", date_summary_json)
            ]
            update_job = bq_client.query(
                update_query,
                job_config=bigquery.QueryJobConfig(query_parameters=query_parameters)
            )
            update_job.result()
            logging.info("Updated date_summary column in the delta table.")

        # Fetch metadata from the delta table
        def fetch_query_and_metadata(bq_client):
            delta_table_query = """
            SELECT 
                query,
                records_per_chunk,
                file_prefix,
                JSON_EXTRACT_SCALAR(date_summary, '$[0].DATE_RUN') AS date_run,
                JSON_EXTRACT_SCALAR(date_summary, '$[0].min_date') AS min_date
            FROM `st-npr-ukg-pro-data-hub-8100.UKG.delta`
            WHERE query_id = 5
            """
            query_job = bq_client.query(delta_table_query)
            results = query_job.result()

            for row in results:
                return (
                    row.query, 
                    row.records_per_chunk, 
                    row.file_prefix, 
                    row.date_run,  
                    row.min_date   
                )

            raise ValueError("No query, records_per_chunk, file prefix, or date_summary JSON found in the delta table.")

        # Determine the next processing start and end dates
        def determine_start_date(bq_client, min_date_from_json):
            processed_query = """
            SELECT 
                date_run AS recent_date_run,
                end_date AS recent_end_date
            FROM `st-npr-ukg-pro-data-hub-8100.UKG.delta_processed`
            WHERE CAST(end_process AS DATE) = (
                SELECT MAX(CAST(end_process AS DATE)) 
                FROM `st-npr-ukg-pro-data-hub-8100.UKG.delta_processed`
            )
            """
            query_job = bq_client.query(processed_query)
            results = query_job.result()

            recent_date_run = None
            recent_end_date = None
            for row in results:
                recent_date_run = row.recent_date_run
                recent_end_date = row.recent_end_date

            if recent_end_date:
                if isinstance(recent_end_date, str):
                    recent_end_date = datetime.datetime.strptime(recent_end_date, "%Y-%m-%d").date()
                next_start_date = recent_end_date + datetime.timedelta(days=1)
            else:
                if isinstance(min_date_from_json, str):
                    next_start_date = datetime.datetime.strptime(min_date_from_json, "%Y-%m-%d").date()
                else:
                    next_start_date = min_date_from_json

            delta_query = """
            SELECT 
                JSON_EXTRACT_SCALAR(date_summary, '$[0].DATE_RUN') AS delta_date_run,
                JSON_EXTRACT_SCALAR(date_summary, '$[0].min_date') AS delta_min_date,
                records_per_chunk
            FROM `st-npr-ukg-pro-data-hub-8100.UKG.delta`
            WHERE query_id = 5
            """
            query_job = bq_client.query(delta_query)
            delta_results = query_job.result()

            delta_date_run = None
            delta_min_date = None
            records_per_chunk = None
            for row in delta_results:
                delta_date_run = row.delta_date_run
                delta_min_date = row.delta_min_date
                records_per_chunk = row.records_per_chunk

            if delta_date_run and recent_date_run:
                if isinstance(delta_date_run, str):
                    delta_date_run = datetime.datetime.strptime(delta_date_run, "%Y-%m-%d").date()
                if isinstance(recent_date_run, str):
                    recent_date_run = datetime.datetime.strptime(recent_date_run, "%Y-%m-%d").date()
                if recent_date_run == delta_date_run and delta_min_date:
                    if isinstance(delta_min_date, str):
                        delta_min_date = datetime.datetime.strptime(delta_min_date, "%Y-%m-%d").date()
                    if delta_min_date >= next_start_date:
                        next_start_date = delta_min_date

            if delta_date_run and recent_date_run and recent_date_run < delta_date_run:
                next_start_date = delta_date_run

            end_date = next_start_date + datetime.timedelta(days=records_per_chunk) if records_per_chunk else next_start_date + datetime.timedelta(days=1)

            logging.info(f"Next start date determined: {next_start_date}")
            logging.info(f"End date determined: {end_date}")
            return next_start_date, end_date, recent_date_run

        # Core logic for processing
        update_date_summary(bq_client)

        base_query, records_per_chunk, base_prefix, date_run, min_date_from_json = fetch_query_and_metadata(bq_client)

        next_start_date, end_date, recent_date_run = determine_start_date(bq_client, min_date_from_json)

        filtered_query = f"""
        {base_query}
        WHERE cdc_srcPartitionDate >= '{next_start_date.strftime("%Y-%m-%d")}'
          AND cdc_srcPartitionDate < '{end_date.strftime("%Y-%m-%d")}'
        """

        query_job = bq_client.query(filtered_query)
        rows_iter = query_job.result(page_size=MAX_ROWS_PER_BATCH)
        accumulated_rows = []

        def generate_dynamic_file_prefix(base_prefix, file_count):
            current_time = datetime.datetime.now()
            incremented_time = current_time + datetime.timedelta(minutes=file_count)
            incremented_timestamp = incremented_time.strftime('%Y%m%d-%H%M')
            return f"{base_prefix}_{incremented_timestamp}"

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

        total_rows = 0
        file_count = 0
        for page in rows_iter.pages:
            rows = [dict(row) for row in page]
            accumulated_rows.extend(rows)
            total_rows += len(rows)

        if accumulated_rows:
            upload_to_gcs(file_count, accumulated_rows)
            file_count += 1

        update_query = """
        INSERT INTO `st-npr-ukg-pro-data-hub-8100.UKG.delta_processed`
        (pipeline, start_date, end_date, start_process, end_process, rows_processed, date_run)
        VALUES
        (@pipeline, @start_date, @end_date, @start_process, @end_process, @rows_processed, @date_run)
        """
        query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("pipeline", "STRING", base_prefix),
                bigquery.ScalarQueryParameter("start_date", "DATE", next_start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                bigquery.ScalarQueryParameter("start_process", "TIMESTAMP", datetime.datetime.now()),
                bigquery.ScalarQueryParameter("end_process", "TIMESTAMP", datetime.datetime.now()),
                bigquery.ScalarQueryParameter("rows_processed", "INT64", total_rows),
                bigquery.ScalarQueryParameter("date_run", "DATE", date_run)
            ]
        )
        bq_client.query(update_query, job_config=query_config).result()
        logging.info(f"Metadata updated in delta_processed with date_run: {date_run}")

        return jsonify({
            "message": "CSV files created successfully",
            "total_rows": total_rows,
            "file_count": file_count
        }), 200

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500
