import json
import os
import sys
import uuid
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.api_core import exceptions
import random

# --- Configuration ---
# Get PROJECT_ID and DATASET_ID from environment variables
# In Cloud Functions, these MUST be set via --set-env-vars
PROJECT_ID = "laeswdm-agbgagenticaihackat"
DATASET_ID = "agriculture"

# Exit immediately if essential environment variables are NOT set
# Cloud Functions will log this and the function instance will fail to start
if not PROJECT_ID:
    print("FATAL ERROR: GCP_PROJECT environment variable not set. Exiting.", file=sys.stderr)
    sys.exit(1)
if not DATASET_ID:
    print("FATAL ERROR: BQ_DATASET environment variable not set. Exiting.", file=sys.stderr)
    sys.exit(1)

print(f"Initializing LogiFresh AI Backend for project {PROJECT_ID}, dataset {DATASET_ID}", file=sys.stdout)

# --- BigQuery Table IDs (LogiFresh Focus + Cross-Domain Reads) ---
WAREHOUSES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.warehouses"
VEHICLES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.vehicles"
SHIPMENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.shipments"
INVENTORY_STOCK_TABLE = f"{PROJECT_ID}.{DATASET_ID}.inventory_stock"
INVENTORY_MOVEMENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.inventory_movements"
COLD_CHAIN_READINGS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.cold_chain_readings"

# Cross-domain reads (from MarketFlow and AgriOptimize)
ORDERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.orders"             # MarketFlow
ORDER_ITEMS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.order_items" # MarketFlow
FARM_PROFILES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.farm_profiles" # AgriOptimize
CUSTOMERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.customers" # Added for route optimization


# Initialize BigQuery client - done outside the main function handler
# Runs during cold start
bigquery_client = None

try:
    bigquery_client = bigquery.Client(project=PROJECT_ID)
except exceptions.DefaultCredentialsError as e:
    print(f"FATAL ERROR: Could not initialize BigQuery client. DefaultCredentialsError: {e}", file=sys.stderr)
    sys.exit(1) # Exit if cannot connect to BQ (credentials issue)
except exceptions.GoogleAPIError as e:
     print(f"FATAL ERROR: BigQuery API error during initialization query: {e}", file=sys.stderr)
     sys.exit(1) # Exit if BQ API fails on init (permissions, network, etc.)
except Exception as e:
     print(f"FATAL ERROR: Unexpected error initializing BigQuery client: {e}", file=sys.stderr)
     sys.exit(1)

# --- Helper Functions (Reused) ---

def get_request_data(req):
    try:
        return req.get_json(silent=True)
    except Exception as e:
        print(f"Error parsing request JSON: {e}", file=sys.stderr)
        return None

# --- BigQuery Helper Functions ---

def insert_rows(table_id, rows_json):
    # No need for client/env var checks here, done at startup
    try:
        errors = bigquery_client.insert_rows_json(table_id, rows_json)
        if errors:
            print(f"BigQuery insert errors for {table_id}: {errors}", file=sys.stderr)
            # Return details of the first error for simplicity
            # errors is a list of dicts, e.g., [{'index': 0, 'errors': [{'reason': 'invalid', 'message': '...'}]}]
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
    # No need for client/env var checks here, done at startup
    try:
        job_config = bigquery.QueryJobConfig(query_parameters=query_params) if query_params else None
        query_job = bigquery_client.query(query, job_config=job_config)
        results = [dict(row) for row in query_job.result()] # Convert results to list of dicts
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

# --- Request Handler Functions (Business Logic) ---

def handle_track_shipment_location(data):
    # Track Shipment Location logic
    required_fields = ['shipment_id']
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    try:
        shipment_id = data['shipment_id']

        # 1. Get Shipment details
        shipment_query = f"""
            SELECT
                shipment_id, order_id, origin_location_id, destination_location_id,
                departure_timestamp, arrival_timestamp_estimate, arrival_timestamp_actual,
                carrier_name, vehicle_id, status, total_quantity_kg, requires_cold_chain
            FROM `{SHIPMENTS_TABLE}`
            WHERE shipment_id = @shipment_id
            LIMIT 1
        """
        shipment_params = [bigquery.ScalarQueryParameter("shipment_id", "STRING", shipment_id)]
        success_s, shipment_details = fetch_rows(shipment_query, shipment_params)

        if not success_s:
             return 500, {"status": "error", "message": "Failed to retrieve shipment details.", "details": shipment_details}

        if not shipment_details:
             return 200, {"status": "not_found", "message": f"Shipment ID {shipment_id} not found."}

        shipment_info = shipment_details[0]

        # 2. Get recent Cold Chain Readings (if applicable)
        cold_chain_readings = []
        success_cc = True # Assume success if no readings attempted

        if shipment_info.get('requires_cold_chain', False):
             cold_chain_query = f"""
                SELECT timestamp, temperature_celsius, location, sensor_id
                FROM `{COLD_CHAIN_READINGS_TABLE}`
                WHERE shipment_id = @shipment_id_cc
                ORDER BY timestamp DESC
                LIMIT 10 -- Get recent readings
             """
             cold_chain_params = [bigquery.ScalarQueryParameter("shipment_id_cc", "STRING", shipment_id)]
             success_cc, cold_chain_readings = fetch_rows(cold_chain_query, cold_chain_params)

             if not success_cc:
                  print(f"Warning: Failed to retrieve cold chain readings for {shipment_id}: {cold_chain_readings}", file=sys.stderr)
                  cold_chain_readings = [] # Return empty list but still mark overall as potentially successful

        # Determine overall success status
        overall_success = success_s and success_cc

        if overall_success:
             response_body = {
                 "status": "success",
                 "shipment_details": shipment_info,
                 "recent_cold_chain_readings": cold_chain_readings
             }
             return 200, response_body
        else:
            error_messages = []
            if not success_s: error_messages.append(f"Shipment query failed: {shipment_details}")
            if not success_cc: error_messages.append(f"Cold chain query failed: {cold_chain_readings}")
            # Return 500 only if the main shipment details failed
            status_code = 500 if not success_s else 200
            return status_code, {"status": "warning" if status_code == 200 else "error",
                                 "message": "Data retrieval issues encountered." if status_code == 200 else "Failed to retrieve shipment data.",
                                 "details": " | ".join(error_messages),
                                 "shipment_details": shipment_info,
                                 "recent_cold_chain_readings": cold_chain_readings}


    except Exception as e:
        print(f"Error in handle_track_shipment_location: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred."}


def handle_report_cold_chain_issue(data):
    # Report Cold Chain Issue logic
    required_fields = ['shipment_id', 'temperature_celsius', 'timestamp'] # Simplify required fields
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    try:
        shipment_id = data['shipment_id']
        temperature_celsius = data['temperature_celsius']
        timestamp_str = data['timestamp']

        try:
            temperature_celsius_float = float(temperature_celsius)
        except (ValueError, TypeError):
             return 400, {"status": "error", "message": "Invalid format for temperature_celsius. Expected number."}

        try:
            # Attempt to parse timestamp (accept various common formats)
            timestamp_obj = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')) # Handle 'Z' for UTC
            timestamp_iso = timestamp_obj.isoformat() # Ensure standard ISO format with timezone info
        except (ValueError, TypeError):
             return 400, {"status": "error", "message": "Invalid timestamp format. Expected ISO 8601 format."}

        row_to_insert = {
            "reading_id": str(uuid.uuid4()), # Generate UUID
            "shipment_id": shipment_id,
            "vehicle_id": data.get('vehicle_id'), # Optional
            "timestamp": timestamp_iso,
            "temperature_celsius": temperature_celsius_float,
            "location": data.get('location'), # Optional location string (e.g., Lat/Lon)
            "sensor_id": data.get('sensor_id', 'Unknown') # Optional, default Unknown
        }

        success, errors = insert_rows(COLD_CHAIN_READINGS_TABLE, [row_to_insert])

        if success:
            # Based on temperature, you might add a warning message
            message = "Cold chain reading reported successfully."
            if temperature_celsius_float > 8.0: # Example threshold for warning
                 message += " Warning: Temperature is elevated."
            if temperature_celsius_float > 15.0: # Example threshold for critical
                 message = "Critical: High temperature excursion reported."

            return 200, {"status": "success", "reading_id": row_to_insert['reading_id'], "message": message}
        else:
            return 500, {"status": "error", "message": "Failed to report cold chain reading.", "details": errors}

    except Exception as e:
        print(f"Error in handle_report_cold_chain_issue: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred."}


def handle_check_warehouse_stock(data):
    # Check Warehouse Stock logic
    required_fields = ['location_id'] # Warehouse ID or similar location ID
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    try:
        location_id = data['location_id'] # This should be a Warehouse ID from your warehouses table
        product_id = data.get('product_id') # Optional product filter
        status_filter = data.get('status', 'Available') # Optional status filter, defaults to Available
        # Optional date range for best before/expiry

        query_params = [
            bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
            bigquery.ScalarQueryParameter("status_filter", "STRING", status_filter),
        ]
        where_clauses = [
            "location_id = @location_id",
            "status = @status_filter"
        ]

        if product_id:
            where_clauses.append("product_id = @product_id")
            query_params.append(bigquery.ScalarQueryParameter("product_id", "STRING", product_id))

        # Add date filters if provided (e.g., 'nearing_expiry': True)
        if data.get('nearing_expiry'):
             # Example: Nearing expiry means expiry within the next 7 days
             where_clauses.append("expiry_date BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)")
             # No extra params needed for this condition

        query = f"""
            SELECT
                stock_id, product_id, current_quantity_kg,
                entry_date, best_before_date, expiry_date,
                storage_conditions, status
            FROM `{INVENTORY_STOCK_TABLE}`
            WHERE {' AND '.join(where_clauses)}
            ORDER BY expiry_date ASC, best_before_date ASC -- Order by freshness
            LIMIT 20 -- Limit results
        """
        success, stock_items = fetch_rows(query, query_params)

        if success:
             return 200, {"status": "success", "location_id": location_id, "stock_items": stock_items}
        else:
             return 500, {"status": "error", "message": "Failed to retrieve warehouse stock.", "details": stock_items}

    except Exception as e:
        print(f"Error in handle_check_warehouse_stock: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred."}


def handle_optimize_delivery_route(data):
    # Optimize Delivery Route logic (Cross-domain read + Simulation)
    required_fields = ['destination_locations'] # List of destinations (could be addresses, order_ids, customer_ids)
    # Required: needs a vehicle or capacity info
    required_fields.append('vehicle_id') # Simplify by requiring a vehicle ID

    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    if not isinstance(data['destination_locations'], list) or not data['destination_locations']:
        return 400, {"status": "error", "message": "Field 'destination_locations' must be a non-empty list."}

    try:
        vehicle_id = data['vehicle_id']
        destinations = data['destination_locations'] # List of destination identifiers

        # 1. Get Vehicle details
        vehicle_query = f"""
            SELECT vehicle_id, vehicle_type, capacity_kg, has_temperature_monitoring, carrier_name
            FROM `{VEHICLES_TABLE}`
            WHERE vehicle_id = @vehicle_id
            LIMIT 1
        """
        vehicle_params = [bigquery.ScalarQueryParameter("vehicle_id", "STRING", vehicle_id)]
        success_v, vehicle_details = fetch_rows(vehicle_query, vehicle_params)

        if not success_v:
             return 500, {"status": "error", "message": "Failed to retrieve vehicle details.", "details": vehicle_details}
        if not vehicle_details:
             return 200, {"status": "not_found", "message": f"Vehicle ID {vehicle_id} not found."}
        vehicle_info = vehicle_details[0]

        # 2. Get Destination details (Cross-domain read to Orders/Customers if destinations are order_ids/customer_ids)
        # This is highly complex. For simplicity, let's assume destinations are just string addresses
        # or IDs that we can look up in Customers or Warehouses and get an address/location string.
        # We will simulate getting addresses for a list of assumed customer IDs.

        customer_ids_to_lookup = destinations # Assume the input list is customer IDs for simplicity
        customer_params = [bigquery.ScalarQueryParameter(f"customer_id_{i}", "STRING", cid) for i, cid in enumerate(customer_ids_to_lookup)]
        customer_query = f"""
            SELECT customer_id, shipping_address
            FROM `{CUSTOMERS_TABLE}`
            WHERE customer_id IN UNNEST(@customer_ids)
        """
        # Create array parameter for IN clause
        customer_params_array = [bigquery.ArrayQueryParameter("customer_ids", "STRING", customer_ids_to_lookup)]

        success_c, customer_locations = fetch_rows(customer_query, customer_params_array)

        if not success_c:
             print(f"Warning: Failed to lookup customer locations: {customer_locations}", file=sys.stderr)
             customer_locations = [] # Continue with potentially incomplete data

        # 3. Simulate Route Optimization
        # In a real system, you'd use a routing engine (like Google Maps Platform Distance Matrix + OR-Tools or a dedicated service)
        # For simulation, just generate a plausible list of stops and estimated times

        if not customer_locations:
             return 200, {"status": "unavailable", "message": "Could not find valid destinations based on provided IDs."}

        simulated_route = []
        current_time = datetime.now()
        estimated_duration_per_stop_minutes = random.randint(30, 90) # Simulate travel + service time

        # Randomly shuffle destinations for a 'route'
        random.shuffle(customer_locations)

        for i, customer in enumerate(customer_locations):
            estimated_arrival = current_time + timedelta(minutes=(i + 1) * estimated_duration_per_stop_minutes)
            simulated_route.append({
                "stop_number": i + 1,
                "destination_id": customer['customer_id'],
                "destination_address": customer['shipping_address'],
                "estimated_arrival_time": estimated_arrival.isoformat(),
                "estimated_service_duration_minutes": random.randint(15, 45)
            })
            current_time = estimated_arrival # Update current time for next stop simulation

        total_estimated_duration_minutes = len(customer_locations) * estimated_duration_per_stop_minutes
        # Simple capacity check simulation - This logic is incorrect, should sum required quantity per stop
        # and compare to vehicle capacity
        # if total_estimated_duration_minutes * 60 > vehicle_info['capacity_kg']: # Incorrect simulation logic
        #      return 200, {"status": "unavailable", "message": f"Requested route exceeds vehicle capacity ({vehicle_info['capacity_kg']} kg estimated required, vehicle capacity {vehicle_info['capacity_kg']} kg)."}

        # Acknowledge success with simulated route
        return 200, {
            "status": "success",
            "vehicle_id": vehicle_id,
            "num_stops": len(simulated_route),
            "estimated_total_duration_minutes": total_estimated_duration_minutes,
            "route_stops": simulated_route,
            "message": "Simulated route generated."
        }

    except Exception as e:
        print(f"Error in handle_optimize_delivery_route: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred during route optimization."}


# This function would receive pickup requests, likely triggered by Pub/Sub from AgriOptimize
# Or potentially called directly by the AgriOptimize Cloud Function
# It would then interact with LogiFresh tables (shipments, inventory_movements)
# For simplicity, we'll just acknowledge.
def handle_schedule_farm_pickup_logistics(data):
    # This is the receiving end of the AgriOptimize pickup request
    # In a real system, this would:
    # 1. Validate the request details (farm_id, product_id, quantity, date)
    # 2. Assign a vehicle and driver (query vehicles, potentially drivers table)
    # 3. Create a new shipment record in the shipments table
    # 4. Create inventory_movement records (e.g., 'Picked' from farm, 'Shipped' in transit)
    # 5. Return a confirmation to the AgriOptimize system (if direct call) or log confirmation.

    required_fields = ['farm_id', 'product_id', 'quantity_kg', 'requested_date']
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        # Return 200 as this might be an async trigger, log error
        print(f"Logistics Pickup Request Error: Missing required fields in body: {', '.join(missing)}", file=sys.stderr)
        return 200, {"status": "error", "message": f"Logistics Pickup Request Error: Missing required fields: {', '.join(missing)}"}

    try:
         print(f"Logistics received pickup request: Farm ID {data.get('farm_id')}, Product ID {data.get('product_id')}, Quantity {data.get('quantity_kg')} kg, Requested Date {data.get('requested_date')}", file=sys.stdout)

         # Simulate successful processing - In reality, this is where the BQ inserts/updates happen
         # Example: Create a new shipment
         # shipment_id = str(uuid.uuid4())
         # shipment_to_insert = {... build shipment data ...}
         # success, errors = insert_rows(SHIPMENTS_TABLE, [shipment_to_insert])
         # Log movement from Farm to Transit etc.

         # Simple Acknowledgment
         return 200, {"status": "acknowledged", "message": "Pickup request received by Logistics."}

    except Exception as e:
         print(f"Error processing logistics pickup request: {e}", file=sys.stderr)
         return 200, {"status": "error", "message": f"Error processing logistics pickup request: {e}"}


# --- Main Cloud Function Entry Point ---

def logifresh_backend_function(request):
    """HTTP Cloud Function for LogiFresh AI backend.
    Routes requests based on path and method to specific handlers.
    """
    print(f"Received request: Method={request.method}, Path={request.path}", file=sys.stdout)

    # --- Health Check ---
    if request.method == 'GET' and request.path == '/':
        try:
            bigquery_client.query("SELECT 1").result(timeout=5)
            return ("LogiFresh AI Backend is running and connected to BigQuery!", 200)
        except Exception as e:
             print(f"Health check failed BigQuery query: {e}", file=sys.stderr)
             return (f"LogiFresh AI Backend health check failed: {e}", 500)

    # --- Handle POST requests ---
    if request.method == 'POST':
        request_data = get_request_data(request)

        if request_data is None:
             if request.headers.get('Content-Type', '').lower() != 'application/json':
                 print(f"Bad Request: Expected 'application/json', got {request.headers.get('Content-Type')}", file=sys.stderr)
                 return ({"status": "error", "message": "Unsupported Media Type. Expected 'application/json'."}, 415)
             else:
                 print("Bad Request: Invalid JSON body received.", file=sys.stderr)
                 return ({"status": "error", "message": "Invalid JSON body received."}, 400)

        # Route POST requests based on path
        if request.path == '/shipment/status':
            status_code, response_body = handle_track_shipment_location(request_data)
        elif request.path == '/issue/coldchain':
            status_code, response_body = handle_report_cold_chain_issue(request_data)
        elif request.path == '/inventory/stock':
            status_code, response_body = handle_check_warehouse_stock(request_data)
        elif request.path == '/route/optimize':
            status_code, response_body = handle_optimize_delivery_route(request_data)
        elif request.path == '/logistics/schedule-pickup-request': # Endpoint potentially called by AgriOptimize
             status_code, response_body = handle_schedule_farm_pickup_logistics(request_data)
        else:
            print(f"Not Found: Unrecognized POST path: {request.path}", file=sys.stderr)
            return ({"status": "error", "message": f"Endpoint {request.path} not found."}, 404)

        return (response_body, status_code)

    # --- Handle other HTTP methods ---
    else:
        print(f"Method Not Allowed: Method={request.method}, Path={request.path}", file=sys.stderr)
        return ({"status": "error", "message": f"Method {request.method} not allowed for path {request.path}."}, 405)