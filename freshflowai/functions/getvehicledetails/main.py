import functions_framework
import json
from google.cloud import bigquery
import logging
import os # Import os to read secrets

# Configure logging
logging.basicConfig(level=logging.INFO)

# BigQuery Configuration
PROJECT_ID = 'laeswdm-agbgagenticaihackat'
DATASET_ID = 'agriculture'
TABLE_ID = 'vehicles'
BIGQUERY_TABLE = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}'

# Initialize BigQuery client (will use default credentials from Cloud Functions env)
client = bigquery.Client()

@functions_framework.http
def lookup_vehicle_by_license(request):
    """
    Cloud Function to look up vehicle details by license number from BigQuery.
    Expects a POST request with a JSON body containing 'vehicle_license_no'.
    Returns details of the matching vehicle.

    Args:
        request (flask.Request): The request object.
            <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
            Expected JSON body:
            {
                "vehicle_license_no": "ABC-123"
            }
    Returns:
        JSON response with a list containing the matching vehicle or error,
        and HTTP status code.
    """
    request_json = request.get_json(silent=True)
    logging.info(f"Received request: {request_json}")

    # --- 1. Validate Input ---
    if not request_json or 'vehicle_license_no' not in request_json or not request_json['vehicle_license_no']:
        logging.error("Missing or empty 'vehicle_license_no' in request body")
        return json.dumps({
            'error': 'Missing or empty vehicle_license_no in request body'
        }), 400 # Bad Request

    vehicle_license_no_input = request_json['vehicle_license_no']
    logging.info(f"Looking up vehicle license: '{vehicle_license_no_input}'")

    # --- 2. Query BigQuery ---
    # Query to select details based on vehicle_license_no
    # Assumes vehicle_license_no column exists and is unique or we only care about the first match
    query = f"""
        SELECT vehicle_id, carrier_name, vehicle_type, capacity_kg, has_temperature_monitoring, vehicle_license_no # Include license_no in select
        FROM `{BIGQUERY_TABLE}`
        WHERE vehicle_license_no = @license_no
        LIMIT 1
    """

    # Use QueryJobConfig to pass parameters safely
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("license_no", "STRING", vehicle_license_no_input),
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)  # API request
        results = query_job.result()  # Waits for job to complete.

        matches = []
        if results.total_rows > 0:
            row = next(iter(results)) # Get the first (and likely only) row
            matches.append({
                'vehicle_id': row['vehicle_id'],
                'carrier_name': row['carrier_name'],
                'vehicle_type': row['vehicle_type'],
                'capacity_kg': row['capacity_kg'],
                'has_temperature_monitoring': row['has_temperature_monitoring'],
                'vehicle_license_no': row['vehicle_license_no']
            })
            logging.info(f"Found vehicle: {row['vehicle_id']} for license {vehicle_license_no_input}")
            # Return 200 OK with the match(es) list
            return json.dumps({'matches': matches}), 200
        else:
            logging.warning(f"Vehicle not found for license: {vehicle_license_no_input}")
            # Return 404 Not Found if no vehicle matches the license
            return json.dumps({
                'error': f"Vehicle with license '{vehicle_license_no_input}' not found",
                'matches': [] # Still include an empty matches list for consistency
            }), 404

    except Exception as e:
        logging.error(f"An error occurred during BigQuery lookup: {e}", exc_info=True)
        return json.dumps({
            'error': 'Internal server error during vehicle lookup',
            'details': str(e)
        }), 500 # Internal Server Error