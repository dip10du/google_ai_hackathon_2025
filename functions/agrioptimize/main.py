import json
import os
import sys
import uuid
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.api_core import exceptions
import random
from flask import Request

# --- Configuration ---
PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = os.environ.get('BQ_DATASET')

if not PROJECT_ID:
    print("FATAL ERROR: GCP_PROJECT environment variable not set. Exiting.", file=sys.stderr)
    sys.exit(1)
if not DATASET_ID:
    print("FATAL ERROR: BQ_DATASET environment variable not set. Exiting.", file=sys.stderr)
    sys.exit(1)

print(f"Initializing AgriOptimize AI Backend for project {PROJECT_ID}, dataset {DATASET_ID}", file=sys.stdout)

# Construct table IDs using the obtained variables
FARM_PROFILES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.farm_profiles"
HARVEST_RECORDS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.harvest_records"
FARM_QC_ISSUES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.farm_qc_issues"
PLANTING_SCHEDULES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.planting_schedules"

# Initialize BigQuery client
bigquery_client = None
try:
    bigquery_client = bigquery.Client(project=PROJECT_ID)
    print("BigQuery client initialized successfully.", file=sys.stdout)
except exceptions.DefaultCredentialsError as e:
    print(f"FATAL ERROR: Could not initialize BigQuery client. DefaultCredentialsError: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
     print(f"FATAL ERROR: Unexpected error initializing BigQuery client: {e}", file=sys.stderr)
     sys.exit(1)

# --- Helper Functions ---

def get_request_data(request):
    """Extract JSON data from request safely."""
    try:
        if request.is_json:
            return request.get_json(silent=True)
        return None
    except Exception as e:
        print(f"Error parsing request JSON: {e}", file=sys.stderr)
        return None

def insert_rows(table_id, rows_json):
    try:
        errors = bigquery_client.insert_rows_json(table_id, rows_json)
        if errors:
            print(f"BigQuery insert errors for {table_id}: {errors}", file=sys.stderr)
            return False, errors[0].get('errors', [{'message': 'Unknown insert error'}])[0].get('message', 'Unknown error details')
        return True, None
    except exceptions.NotFound as e:
         print(f"BigQuery table not found during insert: {table_id}. Error: {e}", file=sys.stderr)
         return False, f"BigQuery table not found: {table_id}"
    except exceptions.GoogleAPIError as e:
        print(f"BigQuery API error during insert into {table_id}: {e}", file=sys.stderr)
        return False, str(e)
    except Exception as e:
        print(f"Unexpected error during BigQuery insert into {table_id}: {e}", file=sys.stderr)
        return False, str(e)

def fetch_rows(query, query_params=None):
    try:
        job_config = bigquery.QueryJobConfig(query_parameters=query_params) if query_params else None
        query_job = bigquery_client.query(query, job_config=job_config)
        results = [dict(row) for row in query_job.result()]
        return True, results
    except exceptions.NotFound as e:
         print(f"BigQuery table not found during query. Error: {e}", file=sys.stderr)
         return False, "BigQuery table not found needed for query."
    except exceptions.GoogleAPIError as e:
        print(f"BigQuery API error during query: {e}", file=sys.stderr)
        return False, str(e)
    except Exception as e:
        print(f"Unexpected error during BigQuery query: {e}", file=sys.stderr)
        return False, str(e)

# --- Request Handler Functions ---

def handle_log_harvest(data):
    required_fields = ['farm_id', 'product_id', 'harvested_quantity_kg', 'harvest_date']
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    try:
        try:
            harvest_date_str = data['harvest_date']
            harvest_date_obj = datetime.strptime(harvest_date_str, '%Y-%m-%d').date()
            harvest_date_iso = harvest_date_obj.isoformat()
        except (ValueError, TypeError):
             return 400, {"status": "error", "message": "Invalid harvest_date format. Expected YYYY-MM-DD."}

        row_to_insert = {
            "harvest_id": str(uuid.uuid4()),
            "farm_id": data['farm_id'],
            "product_id": data['product_id'],
            "product_name": data.get('product_name'),
            "category": data.get('category'),
            "harvest_date": harvest_date_iso,
            "harvested_quantity_kg": data.get('harvested_quantity_kg'),
            "estimated_yield_kg": data.get('estimated_yield_kg'),
            "quality_score": data.get('quality_score'),
            "quality_notes": data.get('quality_notes', ''),
            "photos_gcs_path": data.get('photos_gcs_path'),
            "planting_date": data.get('planting_date'),
            "field_id": data.get('field_id')
        }

        try:
            row_to_insert['harvested_quantity_kg'] = int(row_to_insert['harvested_quantity_kg'])
        except (ValueError, TypeError):
            return 400, {"status": "error", "message": "Invalid format for harvested_quantity_kg. Expected integer."}

        if 'estimated_yield_kg' in row_to_insert and row_to_insert['estimated_yield_kg'] is not None:
             try: row_to_insert['estimated_yield_kg'] = int(row_to_insert['estimated_yield_kg'])
             except (ValueError, TypeError): return 400, {"status": "error", "message": "Invalid format for estimated_yield_kg. Expected integer."}
        if 'quality_score' in row_to_insert and row_to_insert['quality_score'] is not None:
             try: row_to_insert['quality_score'] = float(row_to_insert['quality_score'])
             except (ValueError, TypeError): return 400, {"status": "error", "message": "Invalid format for quality_score. Expected number."}
        if 'planting_date' in row_to_insert and row_to_insert['planting_date'] is not None:
             try: datetime.strptime(row_to_insert['planting_date'], '%Y-%m-%d').date().isoformat()
             except (ValueError, TypeError): return 400, {"status": "error", "message": "Invalid planting_date format. Expected YYYY-MM-DD."}

        success, errors = insert_rows(HARVEST_RECORDS_TABLE, [row_to_insert])

        if success:
            return 200, {"status": "success", "harvest_id": row_to_insert['harvest_id'], "message": "Harvest record logged successfully."}
        else:
            return 500, {"status": "error", "message": "Failed to log harvest record.", "details": errors}

    except Exception as e:
        print(f"Error in handle_log_harvest: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred."}

def handle_get_harvest_advice(data):
    farm_id = data.get('farm_id')
    product_id = data.get('product_id')
    start_date_str = data.get('start_date')
    end_date_str = data.get('end_date')

    try:
        harvest_params = []
        harvest_where_clauses = []
        qc_params = []
        qc_where_clauses = []
        schedule_params = []
        schedule_where_clauses = ["planned_harvest_date_estimate >= CURRENT_DATE()"]

        if farm_id:
             harvest_where_clauses.append("farm_id = @farm_id_h")
             harvest_params.append(bigquery.ScalarQueryParameter("farm_id_h", "STRING", farm_id))
             qc_where_clauses.append("farm_id = @farm_id_qc")
             qc_params.append(bigquery.ScalarQueryParameter("farm_id_qc", "STRING", farm_id))
             schedule_where_clauses.append("farm_id = @farm_id_sched")
             schedule_params.append(bigquery.ScalarQueryParameter("farm_id_sched", "STRING", farm_id))

        if product_id:
             harvest_where_clauses.append("product_id = @product_id_h")
             harvest_params.append(bigquery.ScalarQueryParameter("product_id_h", "STRING", product_id))
             qc_where_clauses.append("(product_id IS NULL OR product_id = @product_id_qc)")
             qc_params.append(bigquery.ScalarQueryParameter("product_id_qc", "STRING", product_id))
             schedule_where_clauses.append("product_id = @product_id_sched")
             schedule_params.append(bigquery.ScalarQueryParameter("product_id_sched", "STRING", product_id))

        date_params = []
        if start_date_str and end_date_str:
             date_params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date_str))
             date_params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date_str))
             harvest_where_clauses.append("harvest_date BETWEEN @start_date AND @end_date")
             qc_where_clauses.append("issue_date BETWEEN @start_date AND @end_date")
        elif start_date_str:
             date_params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date_str))
             harvest_where_clauses.append("harvest_date >= @start_date")
             qc_where_clauses.append("issue_date >= @start_date")
        elif end_date_str:
             date_params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date_str))
             harvest_where_clauses.append("harvest_date <= @end_date")
             qc_where_clauses.append("issue_date <= @end_date")
        else:
             harvest_where_clauses.append("harvest_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)")
             qc_where_clauses.append("issue_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)")

        harvest_params.extend(date_params)
        qc_params.extend(date_params)

        harvest_where_sql = "WHERE " + " AND ".join(harvest_where_clauses) if harvest_where_clauses else ""
        qc_where_sql = "WHERE " + " AND ".join(qc_where_clauses) if qc_where_clauses else ""
        schedule_where_sql = "WHERE " + " AND ".join(schedule_where_clauses)

        # Get recent Harvest Records
        harvest_query = f"""
            SELECT harvest_date, harvested_quantity_kg, quality_score, quality_notes
            FROM `{HARVEST_RECORDS_TABLE}`
            {harvest_where_sql}
            ORDER BY harvest_date DESC
            LIMIT 5
        """
        success_h, harvests = fetch_rows(harvest_query, harvest_params)

        # Get recent QC Issues
        qc_query = f"""
            SELECT issue_date, issue_type, severity, notes
            FROM `{FARM_QC_ISSUES_TABLE}`
            {qc_where_sql}
            ORDER BY issue_date DESC
            LIMIT 5
        """
        success_q, qc_issues = fetch_rows(qc_query, qc_params)

        # Get upcoming Planting Schedules
        schedule_query = f"""
            SELECT planned_planting_date, planned_harvest_date_estimate, expected_yield_estimate_kg, status
            FROM `{PLANTING_SCHEDULES_TABLE}`
            {schedule_where_sql}
            ORDER BY planned_planting_date ASC
            LIMIT 5
        """
        success_s, schedules = fetch_rows(schedule_query, schedule_params)

        # Get Farm Profile details
        farm_details = {}
        success_f = True
        if farm_id:
            farm_query = f"""
                SELECT farm_name, farm_location, primary_crops_grown
                FROM `{FARM_PROFILES_TABLE}`
                WHERE farm_id = @farm_id_f
                LIMIT 1
            """
            farm_params = [bigquery.ScalarQueryParameter("farm_id_f", "STRING", farm_id)]
            success_f, farm_info = fetch_rows(farm_query, farm_params)
            farm_details = farm_info[0] if (success_f and farm_info) else {}

        overall_success = success_h and success_q and success_s and success_f

        if overall_success:
             return 200, {
                 "status": "success",
                 "farm_details": farm_details,
                 "recent_harvests": harvests,
                 "recent_qc_issues": qc_issues,
                 "upcoming_schedules": schedules
             }
        else:
            error_messages = []
            if not success_h: error_messages.append(f"Harvest query failed: {harvests}")
            if not success_q: error_messages.append(f"QC query failed: {qc_issues}")
            if not success_s: error_messages.append(f"Schedule query failed: {schedules}")
            if not success_f and farm_id: error_messages.append(f"Farm query failed: {farm_info}")
            
            status_code = 200 if (harvests or qc_issues or schedules or farm_details) else 500
            return status_code, {"status": "warning" if status_code == 200 else "error",
                                 "message": "Data retrieval issues encountered." if status_code == 200 else "Failed to retrieve data for advice.",
                                 "details": " | ".join(error_messages),
                                 "farm_details": farm_details,
                                 "recent_harvests": harvests,
                                 "recent_qc_issues": qc_issues,
                                 "upcoming_schedules": schedules}

    except Exception as e:
        print(f"Error in handle_get_harvest_advice: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred while getting advice."}

def handle_report_farm_qc_issue(data):
    required_fields = ['farm_id', 'issue_type']
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    try:
        issue_date_str = data.get('issue_date', date.today().isoformat())
        try:
            issue_date_obj = datetime.strptime(issue_date_str, '%Y-%m-%d').date()
            issue_date_iso = issue_date_obj.isoformat()
        except (ValueError, TypeError):
             return 400, {"status": "error", "message": "Invalid issue_date format. Expected YYYY-MM-DD."}

        row_to_insert = {
            "issue_id": str(uuid.uuid4()),
            "farm_id": data['farm_id'],
            "product_id": data.get('product_id'),
            "issue_date": issue_date_iso,
            "issue_type": data['issue_type'],
            "affected_quantity_kg": data.get('affected_quantity_kg'),
            "severity": data.get('severity', 'Medium'),
            "reported_by": data.get('reported_by', 'AI Agent'),
            "notes": data.get('notes', ''),
            "photos_gcs_path": data.get('photos_gcs_path')
        }

        if 'affected_quantity_kg' in row_to_insert and row_to_insert['affected_quantity_kg'] is not None:
            try: row_to_insert['affected_quantity_kg'] = int(row_to_insert['affected_quantity_kg'])
            except (ValueError, TypeError): return 400, {"status": "error", "message": "Invalid format for affected_quantity_kg. Expected integer."}

        success, errors = insert_rows(FARM_QC_ISSUES_TABLE, [row_to_insert])

        if success:
            return 200, {"status": "success", "issue_id": row_to_insert['issue_id'], "message": "Farm quality issue reported successfully."}
        else:
            return 500, {"status": "error", "message": "Failed to report quality issue.", "details": errors}

    except Exception as e:
        print(f"Error in handle_report_farm_qc_issue: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred."}

def handle_schedule_farm_pickup(data):
    required_fields = ['farm_id', 'product_id', 'quantity_kg', 'requested_date']
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    try:
        requested_date_str = data['requested_date']
        try:
            requested_date_obj = datetime.strptime(requested_date_str, '%Y-%m-%d').date()
            requested_date_iso = requested_date_obj.isoformat()
        except (ValueError, TypeError):
             return 400, {"status": "error", "message": "Invalid requested_date format. Expected YYYY-MM-DD."}

        quantity_kg = data['quantity_kg']
        try:
             quantity_kg_int = int(quantity_kg)
        except (ValueError, TypeError):
             return 400, {"status": "error", "message": "Invalid format for quantity_kg. Expected integer."}

        farm_id = data['farm_id']
        product_id = data['product_id']

        # Check availability
        availability_query = f"""
            SELECT SUM(harvested_quantity_kg) as total_harvested
            FROM `{HARVEST_RECORDS_TABLE}`
            WHERE farm_id = @farm_id AND product_id = @product_id AND harvest_date >= DATE_SUB(@requested_date, INTERVAL 7 DAY)
        """
        availability_params = [
            bigquery.ScalarQueryParameter("farm_id", "STRING", farm_id),
            bigquery.ScalarQueryParameter("product_id", "STRING", product_id),
            bigquery.ScalarQueryParameter("requested_date", "DATE", requested_date_iso),
        ]
        success_avail, availability_result = fetch_rows(availability_query, availability_params)

        available_kg = availability_result[0]['total_harvested'] if success_avail and availability_result and availability_result[0]['total_harvested'] is not None else 0

        if not success_avail:
             return 500, {"status": "error", "message": "Failed to check farm availability.", "details": availability_result}

        is_available_for_pickup = available_kg >= quantity_kg_int

        if is_available_for_pickup:
             estimated_pickup_datetime = datetime.combine(requested_date_obj, datetime.min.time()) + timedelta(days=random.randint(0,2), hours=random.randint(8,16), minutes=random.randint(0,59))

             pickup_confirmation = {
                "request_status": "Acknowledged",
                "estimated_pickup_datetime": estimated_pickup_datetime.isoformat(),
                "confirmed_quantity_kg": quantity_kg_int,
                "message": "Pickup request received and appears feasible. Logistics is being notified and will confirm exact details.",
                "requires_cold_chain": (random.random() < 0.9)
             }
             return 200, {"status": "success", "details": pickup_confirmation}
        else:
             return 200, {"status": "unavailable", "message": f"Unable to schedule pickup for {quantity_kg_int} kg. Only found approximately {available_kg} kg recently harvested or on hand for this product at this farm. Please adjust quantity or date."}

    except Exception as e:
        print(f"Error in handle_schedule_farm_pickup: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred while processing pickup request."}

# --- Main Cloud Function Entry Point ---

def agrioptimize_backend_function(request):
    """HTTP Cloud Function for AgriOptimize AI backend.
    Routes requests based on action parameter in JSON body.
    """
    print(f"Received request: Method={request.method}, URL={request.url}", file=sys.stdout)

    # --- Health Check ---
    if request.method == 'GET':
        try:
            return {"status": "success", "message": "AgriOptimize AI Backend is running!"}, 200
        except Exception as e:
             print(f"Health check failed: {e}", file=sys.stderr)
             return {"status": "error", "message": f"AgriOptimize AI Backend health check failed: {e}"}, 500

    # --- Handle POST requests ---
    if request.method == 'POST':
        request_data = get_request_data(request)

        if request_data is None:
             return {"status": "error", "message": "Invalid JSON body received."}, 400

        # Route based on action parameter in request data
        action = request_data.get('action')
        
        if action == 'log_harvest':
            status_code, response_body = handle_log_harvest(request_data)
        elif action == 'get_harvest_advice':
            status_code, response_body = handle_get_harvest_advice(request_data)
        elif action == 'report_farm_qc_issue':
            status_code, response_body = handle_report_farm_qc_issue(request_data)
        elif action == 'schedule_farm_pickup':
            status_code, response_body = handle_schedule_farm_pickup(request_data)
        else:
            return {"status": "error", "message": f"Unknown action: {action}. Supported actions: log_harvest, get_harvest_advice, report_farm_qc_issue, schedule_farm_pickup"}, 400

        return response_body, status_code

    # --- Handle other HTTP methods ---
    else:
        return {"status": "error", "message": f"Method {request.method} not allowed."}, 405