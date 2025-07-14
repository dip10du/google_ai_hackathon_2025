import json
import os
import sys
import uuid
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.api_core import exceptions
import random

# --- Configuration ---
PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = os.environ.get('BQ_DATASET')

if not PROJECT_ID:
    print("FATAL ERROR: GCP_PROJECT environment variable not set. Exiting.", file=sys.stderr)
    sys.exit(1)
if not DATASET_ID:
    print("FATAL ERROR: BQ_DATASET environment variable not set. Exiting.", file=sys.stderr)
    sys.exit(1)

print(f"Initializing MarketFlow AI Backend for project {PROJECT_ID}, dataset {DATASET_ID}", file=sys.stdout)

# --- BigQuery Table IDs (MarketFlow Focus + Cross-Domain Reads) ---
PRODUCT_CATALOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.product_catalog"
CUSTOMERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.customers"
ORDERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.orders"
ORDER_ITEMS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.order_items"
DEMAND_FORECASTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.demand_forecasts"
MARKET_PRICES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.market_prices"
PROMOTIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.promotions"

# Cross-domain reads (from LogiFresh and AgriOptimize)
INVENTORY_STOCK_TABLE = f"{PROJECT_ID}.{DATASET_ID}.inventory_stock" # LogiFresh
SHIPMENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.shipments"           # LogiFresh
PLANTING_SCHEDULES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.planting_schedules" # AgriOptimize
FARM_PROFILES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.farm_profiles" # AgriOptimize

# Initialize BigQuery client
bigquery_client = None
try:
    bigquery_client = bigquery.Client(project=PROJECT_ID)
    bigquery_client.query("SELECT 1").result(timeout=5)
    print("BigQuery client initialized and connection verified successfully.", file=sys.stdout)
except exceptions.DefaultCredentialsError as e:
    print(f"FATAL ERROR: Could not initialize BigQuery client. DefaultCredentialsError: {e}", file=sys.stderr)
    sys.exit(1)
except exceptions.GoogleAPIError as e:
     print(f"FATAL ERROR: BigQuery API error during initialization query: {e}", file=sys.stderr)
     sys.exit(1)
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

# --- Request Handler Functions (Business Logic) ---

def handle_get_demand_forecast(data):
    # Get Demand Forecast logic
    required_fields = ['product_id', 'target_date_start', 'target_date_end']
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    try:
        product_id = data['product_id']
        region = data.get('region') # Region is optional
        target_date_start_str = data['target_date_start']
        target_date_end_str = data['target_date_end']

        query_params = [
            bigquery.ScalarQueryParameter("product_id", "STRING", product_id),
            bigquery.ScalarQueryParameter("start_date", "DATE", target_date_start_str),
            bigquery.ScalarQueryParameter("end_date", "DATE", target_date_end_str),
        ]
        where_clauses = ["product_id = @product_id", "target_date_start = @start_date", "target_date_end = @end_date"]

        if region:
            where_clauses.append("region = @region")
            query_params.append(bigquery.ScalarQueryParameter("region", "STRING", region))

        query = f"""
            SELECT
                forecast_id, region, forecast_date,
                target_date_start, target_date_end,
                forecasted_demand_kg, confidence_level
            FROM `{DEMAND_FORECASTS_TABLE}`
            WHERE {' AND '.join(where_clauses)}
            ORDER BY forecast_date DESC
            LIMIT 5 -- Get the latest few forecasts for this period/region
        """
        success, forecasts = fetch_rows(query, query_params)

        if success:
            return 200, {"status": "success", "forecasts": forecasts}
        else:
            return 500, {"status": "error", "message": "Failed to retrieve demand forecasts.", "details": forecasts}

    except Exception as e:
        print(f"Error in handle_get_demand_forecast: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred."}


def handle_get_market_prices(data):
    # Get Current Market Prices logic
    required_fields = ['product_id']
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    try:
        product_id = data['product_id']
        region = data.get('region') # Region is optional
        date_str = data.get('date', date.today().isoformat()) # Date defaults to today

        query_params = [
            bigquery.ScalarQueryParameter("product_id", "STRING", product_id),
            bigquery.ScalarQueryParameter("market_date", "DATE", date_str),
        ]
        where_clauses = ["product_id = @product_id", "market_date = @market_date"]

        if region:
            where_clauses.append("region = @region")
            query_params.append(bigquery.ScalarQueryParameter("region", "STRING", region))

        # Prioritize exact date match, then maybe a range
        query = f"""
            SELECT
                price_record_id, region, market_date,
                average_market_price_per_kg, source
            FROM `{MARKET_PRICES_TABLE}`
            WHERE {' AND '.join(where_clauses)}
            LIMIT 1 -- Get price for exact date/region
        """
        success, prices = fetch_rows(query, query_params)

        if success and prices:
            return 200, {"status": "success", "prices": prices}
        elif success and not prices:
             # If no exact match, maybe look for nearest date? Or just return no data
             return 200, {"status": "success", "prices": [], "message": f"No market price found for {product_id} on {date_str}{f' in {region}' if region else ''}."}
        else:
            return 500, {"status": "error", "message": "Failed to retrieve market prices.", "details": prices}

    except Exception as e:
        print(f"Error in handle_get_market_prices: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred."}


def handle_check_product_availability(data):
    # Check Product Availability logic (Cross-domain read)
    required_fields = ['product_id']
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    try:
        product_id = data['product_id']
        location_id = data.get('location_id') # Optional location filter

        query_params = [bigquery.ScalarQueryParameter("product_id", "STRING", product_id)]
        stock_where_clauses = ["product_id = @product_id", "status = 'Available'"] # Only look for available stock
        shipment_where_clauses = ["t2.product_id = @product_id_shipment", "t1.status = 'In Transit'", "t1.arrival_timestamp_estimate >= CURRENT_TIMESTAMP()"] # Incoming shipments
        schedule_where_clauses = ["product_id = @product_id_sched", "planned_harvest_date_estimate >= CURRENT_DATE()", "status IN ('Planned', 'Planted', 'Growing', 'Harvesting')"] # Future harvests

        if location_id:
             stock_where_clauses.append("location_id = @location_id_stock")
             query_params.append(bigquery.ScalarQueryParameter("location_id_stock", "STRING", location_id))
             # Shipments/Schedules might also filter by location, but the schema might need joins (e.g. shipment destination = location_id, schedule farm_id = location_id)
             # For simplicity, location filter is only applied to inventory_stock here.

        stock_where_sql = "WHERE " + " AND ".join(stock_where_clauses)

        # 1. Check Inventory Stock (LogiFresh Table)
        stock_query = f"""
            SELECT SUM(current_quantity_kg) as total_on_hand, COUNT(DISTINCT location_id) as num_locations
            FROM `{INVENTORY_STOCK_TABLE}`
            {stock_where_sql}
        """
        success_stock, stock_result = fetch_rows(stock_query, query_params)
        on_hand_kg = stock_result[0]['total_on_hand'] if success_stock and stock_result and stock_result[0]['total_on_hand'] is not None else 0
        num_stock_locations = stock_result[0]['num_locations'] if success_stock and stock_result and stock_result[0]['num_locations'] is not None else 0


        # 2. Check Incoming Shipments (LogiFresh Table - requires join to order_items for product_id)
        # This is complex in pure SQL, simplified by assuming shipments are linked to order items correctly
        # A more robust approach might require a view or a more complex query here
        shipment_params = [bigquery.ScalarQueryParameter("product_id_shipment", "STRING", product_id)] # Separate param to avoid conflict

        shipment_query = f"""
            SELECT SUM(t2.ordered_quantity_kg) as total_incoming_shipments
            FROM `{SHIPMENTS_TABLE}` t1
            JOIN `{ORDER_ITEMS_TABLE}` t2 ON t1.order_id = t2.order_id -- Simplified join
            WHERE {' AND '.join(shipment_where_clauses)}
        """
        success_shipment, shipment_result = fetch_rows(shipment_query, shipment_params)
        incoming_shipments_kg = shipment_result[0]['total_incoming_shipments'] if success_shipment and shipment_result and shipment_result[0]['total_incoming_shipments'] is not None else 0


        # 3. Check Upcoming Harvests (AgriOptimize Table)
        schedule_params = [bigquery.ScalarQueryParameter("product_id_sched", "STRING", product_id)] # Separate param

        schedule_where_sql = "WHERE " + " AND ".join(schedule_where_clauses)

        schedule_query = f"""
            SELECT SUM(expected_yield_estimate_kg) as total_upcoming_harvest
            FROM `{PLANTING_SCHEDULES_TABLE}`
            {schedule_where_sql}
        """
        success_schedule, schedule_result = fetch_rows(schedule_query, schedule_params)
        upcoming_harvest_kg = schedule_result[0]['total_upcoming_harvest'] if success_schedule and schedule_result and schedule_result[0]['total_upcoming_harvest'] is not None else 0


        # Combine results and determine overall success status
        overall_success = success_stock and success_shipment and success_schedule

        if overall_success:
             response_body = {
                 "status": "success",
                 "product_id": product_id,
                 "location_id": location_id, # Report back filter used
                 "total_on_hand_kg": on_hand_kg,
                 "num_stock_locations": num_stock_locations,
                 "total_incoming_shipments_kg": incoming_shipments_kg,
                 "total_upcoming_harvest_kg": upcoming_harvest_kg
             }
             return 200, response_body
        else:
            error_messages = []
            if not success_stock: error_messages.append(f"Stock query failed: {stock_result}")
            if not success_shipment: error_messages.append(f"Shipment query failed: {shipment_result}")
            if not success_schedule: error_messages.append(f"Schedule query failed: {schedule_result}")
            return 500, {"status": "error", "message": "Failed to retrieve availability data.", "details": " | ".join(error_messages)}

    except Exception as e:
        print(f"Error in handle_check_product_availability: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred."}


def handle_place_purchase_order(data):
    # Place Purchase Order logic
    required_fields = ['customer_id', 'items'] # items should be a list of {product_id, quantity_kg}
    if not data or not all(field in data for field in required_fields):
        missing = [field for field in required_fields if not data or field not in data]
        return 400, {"status": "error", "message": f"Missing required fields in body: {', '.join(missing)}"}

    if not isinstance(data['items'], list) or not data['items']:
        return 400, {"status": "error", "message": "Field 'items' must be a non-empty list."}

    order_id = str(uuid.uuid4())
    customer_id = data['customer_id']
    order_date = date.today().isoformat()
    # delivery_date_requested, delivery_address could be optional or retrieved from customer table

    order_items_to_insert = []
    total_amount = 0.0

    # In a real scenario, you would validate product_ids, check stock, maybe apply pricing rules
    # For this synthetic data insertion, we just create records

    try:
        for item in data['items']:
            if 'product_id' not in item or 'quantity_kg' not in item:
                return 400, {"status": "error", "message": "Each item in 'items' must have 'product_id' and 'quantity_kg'."}

            product_id = item['product_id']
            quantity_kg = item['quantity_kg']

            try:
                 quantity_kg_int = int(quantity_kg)
                 if quantity_kg_int <= 0:
                     return 400, {"status": "error", "message": f"Invalid quantity for product {product_id}. Must be positive integer."}
            except (ValueError, TypeError):
                 return 400, {"status": "error", "message": f"Invalid quantity_kg format for product {product_id}. Expected integer."}

            # Simulate price lookup (e.g., fetch from product catalog or market prices)
            # For simplicity, let's use a random price or a default
            simulated_price_per_kg = round(random.uniform(1.0, 5.0), 2) # Simulate price
            line_item_total = round(quantity_kg_int * simulated_price_per_kg, 2)
            total_amount += line_item_total

            order_items_to_insert.append({
                "order_item_id": str(uuid.uuid4()),
                "order_id": order_id,
                "product_id": product_id,
                "ordered_quantity_kg": quantity_kg_int,
                "price_per_kg_at_order": simulated_price_per_kg,
                "line_item_total": line_item_total
            })

        # Get delivery address from customer table (Cross-domain read to Customers)
        customer_query = f"SELECT shipping_address FROM `{CUSTOMERS_TABLE}` WHERE customer_id = @customer_id LIMIT 1"
        customer_params = [bigquery.ScalarQueryParameter("customer_id", "STRING", customer_id)]
        success_cust, customer_info = fetch_rows(customer_query, customer_params)

        delivery_address = customer_info[0]['shipping_address'] if success_cust and customer_info else "Unknown Address"
        if not success_cust:
             print(f"Warning: Could not fetch customer shipping address for {customer_id}: {customer_info}", file=sys.stderr)
             # Continue, but use default address

        # Insert into Orders table
        order_to_insert = {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date,
            "delivery_date_requested": data.get('delivery_date_requested', order_date), # Use requested date or order date
            "delivery_address": data.get('delivery_address', delivery_address), # Use provided address or fetched one
            "status": data.get('status', 'Pending'), # Default status
            "total_amount": round(total_amount, 2)
        }

        success_order, errors_order = insert_rows(ORDERS_TABLE, [order_to_insert])

        # Insert into Order Items table
        success_items, errors_items = insert_rows(ORDER_ITEMS_TABLE, order_items_to_insert)

        if success_order and success_items:
            return 200, {"status": "success", "order_id": order_id, "message": "Purchase order placed successfully.", "total_amount": total_amount}
        else:
            # In case of failure, you might need to clean up already inserted rows
            # For this example, we just report failure
            error_details = []
            if not success_order: error_details.append(f"Order insert failed: {errors_order}")
            if not success_items: error_details.append(f"Order items insert failed: {errors_items}")
            return 500, {"status": "error", "message": "Failed to place purchase order.", "details": " | ".join(error_details)}

    except Exception as e:
        print(f"Error in handle_place_purchase_order: {e}", file=sys.stderr)
        return 500, {"status": "error", "message": "An internal error occurred."}


# --- Main Cloud Function Entry Point ---

def marketflow_backend_function(request):
    """HTTP Cloud Function for MarketFlow AI backend.
    Routes requests based on path and method to specific handlers.
    """
    print(f"Received request: Method={request.method}, Path={request.path}", file=sys.stdout)

    # --- Health Check ---
    if request.method == 'GET' and request.path == '/':
        try:
            bigquery_client.query("SELECT 1").result(timeout=5)
            return ("MarketFlow AI Backend is running and connected to BigQuery!", 200)
        except Exception as e:
             print(f"Health check failed BigQuery query: {e}", file=sys.stderr)
             return (f"MarketFlow AI Backend health check failed: {e}", 500)

    # --- Handle POST requests (most API calls) ---
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
        if request.path == '/forecast':
            status_code, response_body = handle_get_demand_forecast(request_data)
        elif request.path == '/prices/current':
            status_code, response_body = handle_get_market_prices(request_data)
        elif request.path == '/inventory/available':
            status_code, response_body = handle_check_product_availability(request_data)
        elif request.path == '/order':
            status_code, response_body = handle_place_purchase_order(request_data)
        else:
            print(f"Not Found: Unrecognized POST path: {request.path}", file=sys.stderr)
            return ({"status": "error", "message": f"Endpoint {request.path} not found."}, 404)

        return (response_body, status_code)

    # --- Handle other HTTP methods ---
    else:
        print(f"Method Not Allowed: Method={request.method}, Path={request.path}", file=sys.stderr)
        return ({"status": "error", "message": f"Method {request.method} not allowed for path {request.path}."}, 405)