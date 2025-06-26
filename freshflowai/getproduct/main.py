import functions_framework
import json
from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# BigQuery Configuration
PROJECT_ID = 'laeswdm-agbgagenticaihackat'
DATASET_ID = 'agriculture'
TABLE_ID = 'product_catalog'
BIGQUERY_TABLE = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}'

# Initialize BigQuery client (will use default credentials from Cloud Functions env)
client = bigquery.Client()

@functions_framework.http
def lookup_product_by_name(request):
    """
    Cloud Function to look up product details by product name from BigQuery.
    Expects a POST request with an optional JSON body containing 'product_name'.
    If 'product_name' is missing or empty, returns a list of initial products.
    Returns a list of potential matches.

    Args:
        request (flask.Request): The request object.
            <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        JSON response with a list of matches or error, and HTTP status code.
    """
    request_json = request.get_json(silent=True)
    logging.info(f"Received request: {request_json}")

    product_name_input = None
    # Check if the request body is not empty and contains 'product_name'
    if request_json and 'product_name' in request_json and request_json['product_name']:
        product_name_input = request_json['product_name']

    query_params = [] # Initialize empty parameters list

    if not product_name_input: # Check if input is None or empty string
        logging.info("No product_name provided or it was empty, fetching an initial list of products.")
        query = f"""
            SELECT product_id, product_name, category, storage_requirements
            FROM `{BIGQUERY_TABLE}`
            LIMIT 100 -- Limit results when fetching all to avoid large responses
        """
    else:
        logging.info(f"Looking up product name: '{product_name_input}'")
        # Use LOWER() and LIKE with wildcards for flexible matching
        query = f"""
            SELECT product_id, product_name, category, storage_requirements
            FROM `{BIGQUERY_TABLE}`
            WHERE LOWER(product_name) LIKE LOWER(@product_name_pattern)
        """
        product_name_pattern = f'%{product_name_input}%'
        query_params.append(bigquery.ScalarQueryParameter("product_name_pattern", "STRING", product_name_pattern))


    # Use QueryJobConfig with potentially empty parameters
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        query_job = client.query(query, job_config=job_config)  # API request
        results = query_job.result()  # Waits for job to complete.

        matches = []
        for row in results:
            matches.append({
                'product_id': row['product_id'],
                'product_name': row['product_name'],
                'category': row['category'],
                'storage_requirements': row['storage_requirements']
            })

        logging.info(f"Returning {len(matches)} matches.")

        # Always return 200 OK if the lookup process itself didn't fail,
        # even if zero matches were found. The 'matches' list will be empty.
        return json.dumps({
            'matches': matches
        }), 200

    except Exception as e:
        logging.error(f"An error occurred during BigQuery lookup: {e}", exc_info=True)
        return json.dumps({
            'error': 'Internal server error during product lookup',
            'details': str(e)
        }), 500 # Internal Server Error