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
    print("BigQuery client initialized successfully.", file=sys.stdout)
except exceptions.DefaultCredentialsError as e:
    print(f"FATAL ERROR: Could not initialize BigQuery client. DefaultCredentialsError: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
     print(f"FATAL ERROR: Unexpected error initializing BigQuery client: {e}", file=sys.stderr)
     sys.exit(1)

# --- Helper Functions (Reused) ---

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

        stock_where_sql = "WHERE " + " AND ".join(stock_where_clauses)

        # 1. Check Inventory Stock (LogiFresh Table)
        stock_query = f"""
            SELECT IFNULL(SUM(current_quantity_kg), 0) as total_on_hand, COUNT(DISTINCT location_id) as num_locations
            FROM `{INVENTORY_STOCK_TABLE}`
            {stock_where_sql}
        """
        success_stock, stock_result = fetch_rows(stock_query, query_params)
        on_hand_kg = stock_result[0]['total_on_hand'] if success_stock and stock_result else 0
        num_stock_locations = stock_result[0]['num_locations'] if success_stock and stock_result else 0

        if success_stock:
            response_body = {
                "status": "success",
                "product_id": product_id,
                "location_id": location_id,
                "total_on_hand_kg": on_hand_kg,
                "num_stock_locations": num_stock_locations,
                "total_incoming_shipments_kg": 0,  # Simplified for now
                "total_upcoming_harvest_kg": 0     # Simplified for now
            }
            return 200, response_body
        else:
            return 500, {"status": "error", "message": "Failed to retrieve availability data.", "details": stock_result}

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

    order_items_to_insert = []
    total_amount = 0.0

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

            # Simulate price lookup
            simulated_price_per_kg = round(random.uniform(1.0, 5.0), 2)
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

        # Insert into Orders table
        order_to_insert = {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date,
            "delivery_date_requested": data.get('delivery_date_requested', order_date),
            "delivery_address": data.get('delivery_address', "Default Address"),
            "status": data.get('status', 'Pending'),
            "total_amount": round(total_amount, 2)
        }

        success_order, errors_order = insert_rows(ORDERS_TABLE, [order_to_insert])
        success_items, errors_items = insert_rows(ORDER_ITEMS_TABLE, order_items_to_insert)

        if success_order and success_items:
            return 200, {"status": "success", "order_id": order_id, "message": "Purchase order placed successfully.", "total_amount": total_amount}
        else:
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
    Routes requests based on query parameters and request data to specific handlers.
    """
    print(f"Received request: Method={request.method}, URL={request.url}", file=sys.stdout)

    # --- Health Check ---
    if request.method == 'GET':
        try:
            # Simple health check without heavy query
            return {"status": "success", "message": "MarketFlow AI Backend is running!"}, 200
        except Exception as e:
             print(f"Health check failed: {e}", file=sys.stderr)
             return {"status": "error", "message": f"MarketFlow AI Backend health check failed: {e}"}, 500

    # --- Handle POST requests (most API calls) ---
    if request.method == 'POST':
        request_data = get_request_data(request)

        if request_data is None:
             return {"status": "error", "message": "Invalid JSON body received."}, 400

        # Route based on action parameter in request data
        action = request_data.get('action')
        
        if action == 'get_demand_forecast':
            status_code, response_body = handle_get_demand_forecast(request_data)
        elif action == 'get_market_prices':
            status_code, response_body = handle_get_market_prices(request_data)
        elif action == 'check_product_availability':
            status_code, response_body = handle_check_product_availability(request_data)
        elif action == 'place_purchase_order':
            status_code, response_body = handle_place_purchase_order(request_data)
        else:
            return {"status": "error", "message": f"Unknown action: {action}. Supported actions: get_demand_forecast, get_market_prices, check_product_availability, place_purchase_order"}, 400

        return response_body, status_code

    # --- Handle other HTTP methods ---
    else:
        return {"status": "error", "message": f"Method {request.method} not allowed."}, 405