import functions_framework
import json
from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# BigQuery Configuration
PROJECT_ID = 'laeswdm-agbgagenticaihackat'
DATASET_ID = 'agriculture'
TABLE_ID = 'farm_profiles'
BIGQUERY_TABLE = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}'

# Initialize BigQuery client (will use default credentials from Cloud Functions env)
client = bigquery.Client()

@functions_framework.http
def lookup_farm_by_name(request):
    """
    Cloud Function to look up farm details by name or location query, or list all farms.
    Expects a POST request with an optional JSON body containing 'farm_name'.
    If 'farm_name' is missing or empty, returns a list of initial farms.
    Returns a list of potential matches.

    Args:
        request (flask.Request): The request object.
            <https://flask.palletsprojects.org/en/1.1.x/api/#incoming-request-data>
    Returns:
        JSON response with a list of matches or error, and HTTP status code.
    """
    request_json = request.get_json(silent=True)
    logging.info(f"Received request: {request_json}")

    farm_name_input = None
    # Check if request body is present and contains a non-empty 'farm_name'
    if request_json and 'farm_name' in request_json and request_json['farm_name']:
        farm_name_input = request_json['farm_name']

    query_params = [] # Initialize empty parameters list

    if not farm_name_input: # Check if input is None or empty string
        logging.info("No farm_name provided or it was empty, fetching an initial list of farms.")
        query = f"""
            SELECT farm_id, farm_name, farm_location, supplier_name
            FROM `{BIGQUERY_TABLE}`
            LIMIT 100 -- Limit results when fetching all to avoid large responses
        """
    else:
        logging.info(f"Looking up farm query: '{farm_name_input}'")
        # Look up by name or location using LIKE
        query = f"""
            SELECT farm_id, farm_name, farm_location, supplier_name
            FROM `{BIGQUERY_TABLE}`
            WHERE LOWER(farm_name) LIKE LOWER(@query_pattern) OR LOWER(farm_location) LIKE LOWER(@query_pattern)
            LIMIT 100 -- Limit results for search too
        """
        query_pattern = f'%{farm_name_input}%'
        query_params.append(bigquery.ScalarQueryParameter("query_pattern", "STRING", query_pattern))


    # Use QueryJobConfig with potentially empty parameters
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        query_job = client.query(query, job_config=job_config)  # API request
        results = query_job.result()  # Waits for job to complete.

        matches = []
        for row in results:
            matches.append({
                'farm_id': row['farm_id'],
                'farm_name': row['farm_name'],
                'farm_location': row['farm_location'],
                'supplier_name': row['supplier_name']
            })

        logging.info(f"Returning {len(matches)} matches.")

        # Always return 200 OK if the lookup process itself didn't fail.
        return json.dumps({
            'matches': matches
        }), 200

    except Exception as e:
        logging.error(f"An error occurred during BigQuery lookup: {e}", exc_info=True)
        return json.dumps({
            'error': 'Internal server error during farm lookup',
            'details': str(e)
        }), 500 # Internal Server Error