import functions_framework
import json
from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# BigQuery Configuration
PROJECT_ID = 'laeswdm-agbgagenticaihackat'
DATASET_ID = 'agriculture'
TABLE_ID = 'customers'
BIGQUERY_TABLE = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}'

# Initialize BigQuery client (will use default credentials from Cloud Functions env)
client = bigquery.Client()

@functions_framework.http
def lookup_customer_by_name(request):
    """
    Cloud Function to look up customer details by name query, or list all customers.
    Expects a POST request with an optional JSON body containing 'customer_name_query'.
    If 'customer_name_query' is missing or empty, returns a list of initial customers.
    Returns a list of potential matches.

    Args:
        request (flask.Request): The request object.
            <https://flask.palletsprojects.org/en/1.1.x/api/#incoming-request-data>
    Returns:
        JSON response with a list of matches or error, and HTTP status code.
    """
    request_json = request.get_json(silent=True)
    logging.info(f"Received request: {request_json}")

    customer_name_query_input = None
    # Check if request body is present and contains a non-empty 'customer_name_query'
    if request_json and 'customer_name_query' in request_json and request_json['customer_name_query']:
        customer_name_query_input = request_json['customer_name_query']

    query_params = [] # Initialize empty parameters list

    if not customer_name_query_input: # Check if input is None or empty string
        logging.info("No customer_name_query provided or it was empty, fetching an initial list of customers.")
        query = f"""
            SELECT customer_id, customer_name, customer_type, shipping_address
            FROM `{BIGQUERY_TABLE}`
            LIMIT 100 -- Limit results when fetching all to avoid large responses
        """
    else:
        logging.info(f"Looking up customer name query: '{customer_name_query_input}'")
        # Look up by name using LIKE
        query = f"""
            SELECT customer_id, customer_name, customer_type, shipping_address
            FROM `{BIGQUERY_TABLE}`
            WHERE LOWER(customer_name) LIKE LOWER(@query_pattern)
            LIMIT 100 -- Limit results for search too
        """
        query_pattern = f'%{customer_name_query_input}%'
        query_params.append(bigquery.ScalarQueryParameter("query_pattern", "STRING", query_pattern))


    # Use QueryJobConfig with potentially empty parameters
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        query_job = client.query(query, job_config=job_config)  # API request
        results = query_job.result()  # Waits for job to complete.

        matches = []
        for row in results:
            matches.append({
                'customer_id': row['customer_id'],
                'customer_name': row['customer_name'],
                'customer_type': row['customer_type'],
                'shipping_address': row['shipping_address']
            })

        logging.info(f"Found {len(matches)} matches for '{customer_name_query_input}'")

        # Always return 200 OK if the lookup process itself didn't fail.
        return json.dumps({
            'matches': matches
        }), 200

    except Exception as e:
        logging.error(f"An error occurred during BigQuery lookup: {e}", exc_info=True)
        return json.dumps({
            'error': 'Internal server error during customer lookup',
            'details': str(e)
        }), 500 # Internal Server Error