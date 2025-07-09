import functions_framework
import json
import logging
import os
import requests
from google.cloud import bigquery
from datetime import datetime, date, timedelta
import random
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)

# --- BigQuery Configuration ---
PROJECT_ID = os.environ.get('GCP_PROJECT', 'laeswdm-agbgagenticaihackat') # Use environment variable
DATASET_ID = 'agriculture'
FARM_PROFILES_TABLE = f'{PROJECT_ID}.{DATASET_ID}.farm_profiles'
WAREHOUSES_TABLE = f'{PROJECT_ID}.{DATASET_ID}.warehouses'
ORDERS_TABLE = f'{PROJECT_ID}.{DATASET_ID}.orders'
ORDER_ITEMS_TABLE = f'{PROJECT_ID}.{DATASET_ID}.order_items'
CUSTOMERS_TABLE = f'{PROJECT_ID}.{DATASET_ID}.customers'
VEHICLES_TABLE = f'{PROJECT_ID}.{DATASET_ID}.vehicles'

# --- Google Maps Platform Configuration ---
# Get API Key from Secret Manager environment variable
GOOGLE_MAPS_API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY') # This env var name must match the --set-secrets flag
if not GOOGLE_MAPS_API_KEY:
     logging.error("GOOGLE_MAPS_API_KEY environment variable not set. Cannot call Maps APIs.")
     # In a real production function, you might want to raise an exception or handle this more robustly

ROUTES_API_URL = 'https://routes.googleapis.com/directions/v2:computeRoutes'
GEOCODING_API_URL = 'https://maps.googleapis.com/maps/api/geocode/json' # Geocoding API Endpoint

# Initialize BigQuery client
client = bigquery.Client()

# Helper function to execute BigQuery queries (same as before)
def fetch_rows(query, parameters=None):
    """Executes a BigQuery query and returns rows as a list of dictionaries."""
    job_config = bigquery.QueryJobConfig(query_parameters=parameters)
    try:
        query_job = client.query(query, job_config=job_config)
        results = [dict(row) for row in query_job.result()]
        return True, results
    except Exception as e:
        logging.error(f"BigQuery query failed: {e}", exc_info=True)
        return False, str(e)

# --- NEW Helper function for Geocoding API ---
def geocode_address(address_string, api_key):
    """Calls Google Geocoding API to convert address string to lat/lon."""
    if not api_key:
        logging.error("Geocoding API Key is not set.")
        return None

    if not address_string:
        logging.warning("Attempted to geocode an empty address string.")
        return None

    params = {
        'address': address_string,
        'key': api_key
    }

    try:
        logging.info(f"Calling Geocoding API for address: {address_string}")
        response = requests.get(GEOCODING_API_URL, params=params)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        geocode_result = response.json()
        logging.info(f"Received Geocoding API response (status: {geocode_result.get('status')})")

        if geocode_result.get('status') == 'OK' and geocode_result.get('results'):
            # Get location from the geometry of the first result
            location = geocode_result['results'][0]['geometry']['location']
            return {'latitude': location['lat'], 'longitude': location['lng']}
        elif geocode_result.get('status') == 'ZERO_RESULTS':
             logging.warning(f"Geocoding API found no results for address: {address_string}")
             return None
        else:
             logging.error(f"Geocoding API returned status: {geocode_result.get('status')} for address: {address_string}. Error: {geocode_result.get('error_message')}")
             return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Error calling Geocoding API: {e}", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"Unexpected error processing Geocoding API response for address {address_string}: {e}", exc_info=True)
        return None


# Helper function to get coordinates (MODIFIED to use geocoding)
def get_location_coordinates(bq_client, location_id, api_key):
    """Queries BQ for location string and attempts to geocode it."""
    logging.info(f"Fetching location string for ID: {location_id}")
    # Query to get location string from either farm_profiles or warehouses
    query = f"""
        SELECT farm_location FROM `{FARM_PROFILES_TABLE}` WHERE farm_id = @loc_id
        UNION ALL
        SELECT location_address FROM `{WAREHOUSES_TABLE}` WHERE warehouse_id = @loc_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("loc_id", "STRING", location_id)])
    result = bq_client.query(query, job_config=job_config).result()

    if result.total_rows > 0:
        location_string = next(iter(result))['farm_location'] # Assuming UNION result uses first column name
        # Try to parse dummy coords first (useful for your synthetic data)
        if isinstance(location_string, str) and "Lat:" in location_string and "Lon:" in location_string:
             try:
                 parts = location_string.replace("Lat:", "").replace("Lon:", "").split(", Lon:")
                 lat = float(parts[0].strip())
                 lon = float(parts[1].strip())
                 logging.info(f"Parsed dummy coordinates for {location_id}: Lat={lat}, Lon={lon}")
                 return {'latitude': lat, 'longitude': lon}
             except Exception as e:
                 logging.warning(f"Failed to parse dummy coordinates for {location_id}: {e}. Attempting geocoding.", exc_info=True)
                 # Fall through to geocoding if parsing fails

        # If not dummy format or parsing failed, call Geocoding API
        logging.info(f"Attempting to geocode location string for {location_id}: '{location_string}'")
        return geocode_address(location_string, api_key)


    logging.warning(f"Location ID {location_id} not found in farm_profiles or warehouses.")
    return None

# Helper function to get destination details (MODIFIED to use geocoding)
def get_destination_details_for_orders(bq_client, order_ids, api_key):
    """Queries BQ for destination addresses/locations and quantities for a list of order IDs, and geocodes."""
    logging.info(f"Fetching destination details for order IDs: {order_ids}")
    # Query to join orders, order_items, customers/warehouses to get total quantity and shipping address/location
    query = f"""
        SELECT
            o.order_id,
            o.delivery_address, -- Get the delivery address from the order
            CAST(RAND() * 4000 + 1000 AS INT64) AS total_kg -- Total quantity per order
        FROM `{ORDERS_TABLE}` o
        --JOIN `{ORDER_ITEMS_TABLE}` oi ON o.order_id = oi.order_id
        JOIN `{CUSTOMERS_TABLE}` c ON o.customer_id = c.customer_id -- Join to customers to ensure address is from customer
        WHERE o.order_id IN UNNEST(@order_ids)
        GROUP BY o.order_id, o.delivery_address -- Group by order and address
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("order_ids", "STRING", order_ids)])
    success, results = fetch_rows(query, job_config.query_parameters)

    destination_details = []
    if success:
        for row in results:
            address_string = row['delivery_address']
            coords = None
            # Try to parse dummy coords first (useful for your synthetic data)
            if isinstance(address_string, str) and "Lat:" in address_string and "Lon:" in address_string:
                 try:
                     parts = address_string.replace("Lat:", "").replace("Lon:", "").split(", Lon:")
                     lat = float(parts[0].strip())
                     lon = float(parts[1].strip())
                     coords = {'latitude': lat, 'longitude': lon}
                     logging.info(f"Parsed dummy coords for order {row['order_id']}: {coords}")
                 except Exception as e:
                     logging.warning(f"Failed to parse dummy coords for order {row['order_id']}: {e}. Attempting geocoding.", exc_info=True)
                     # coords remains None, will fall through to geocoding

            # If not dummy format or parsing failed, call Geocoding API
            if not coords:
                 logging.info(f"Attempting to geocode address string for order {row['order_id']}: '{address_string}'")
                 coords = geocode_address(address_string, api_key)


            if coords:
                 destination_details.append({
                     'order_id': row['order_id'], # Use order_id as the destination identifier
                     'address': address_string, # Keep original address string
                     'latitude': coords['latitude'],
                     'longitude': coords['longitude'],
                     'total_kg': row['total_kg']
                     # Add product specific details if needed for reefer planning etc.
                 })
            else:
                 logging.warning(f"Could not get coordinates for order {row['order_id']} from address '{address_string}'. Skipping destination.")


    return destination_details # List of dictionaries for successfully geocoded destinations

# Helper function to get vehicle details (same as before, added for completeness)
def get_vehicle_details(bq_client, vehicle_id):
     """Queries BQ for vehicle capacity, reefer status, etc."""
     logging.info(f"Fetching vehicle details for ID: {vehicle_id}")
     query = f"""
         SELECT vehicle_id, vehicle_type, capacity_kg, has_temperature_monitoring, carrier_name
         FROM `{VEHICLES_TABLE}`
         WHERE vehicle_id = @vehicle_id
         LIMIT 1
     """
     job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("vehicle_id", "STRING", vehicle_id)])
     success, results = fetch_rows(query, job_config.query_parameters)
     if success and results:
         return results[0]
     elif success:
         logging.warning(f"Vehicle ID {vehicle_id} not found.")
         return None
     else:
         logging.error(f"Failed to retrieve vehicle details: {results}")
         raise Exception("Failed to retrieve vehicle details from BQ.")


# Helper function to call Google Routes API (computeRoutes) (same as before, using the new helper names)
def call_routes_api_compute_routes(origin_coords, destination_details, api_key):
    """Calls Google Routes API (computeRoutes) to get route details."""
    if not api_key:
        logging.error("Google Maps API Key is not set.")
        return False, "API Key not configured."

    # Prepare waypoints: origin, intermediates (destinations except the last one), destination (the last one)
    if not destination_details:
        logging.warning("No valid destinations with coordinates provided for routing API call.")
        return False, "No valid destinations with coordinates provided."

    origin_waypoint = {"location": {"latLng": origin_coords}}
    # Assume destination_details is already in the desired route order for computeRoutes
    destination_waypoints = [{"location": {"latLng": {'latitude': dest['latitude'], 'longitude': dest['longitude']}}} for dest in destination_details]

    # The last destination is the final destination, others are intermediates
    final_destination_waypoint = destination_waypoints[-1]
    intermediate_waypoints = destination_waypoints[:-1] # All except the last one

    routes_api_payload = {
        "origin": origin_waypoint,
        "destination": final_destination_waypoint,
        "intermediates": intermediate_waypoints,
        "travelMode": "DRIVE", # Or other modes like TWO_WHEELER, WALKING etc.
        "routingPreference": "TRAFFIC_AWARE_OPTIMAL", # Get real-time traffic estimates
        #"departureTime": datetime.now().isoformat() + 'Z', # Use current time for real-time traffic
        "computeAlternativeRoutes": False,
        "routeModifiers": {
            "avoidTolls": False, # Configure based on requirements
            "avoidHighways": False # Configure based on requirements
        },
        # Add vehicle type/emission data here if needed and available
    }

    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': api_key,
        # Request specific fields to minimize response size
        #'X-Goog-FieldMask': 'routes.duration,routes.distanceMeters,routes.legs.duration,routes.legs.distanceMeters,routes.legs.steps,routes.legs.trafficData'
        'X-Goog-FieldMask': 'routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline'
    }

    try:
        logging.info(f"Calling Google Routes API computeRoutes: {ROUTES_API_URL}")

        logging.info("--- Routes API Request Payload ---")
        logging.info(json.dumps(routes_api_payload)) # Use indent for readability
        logging.info("--- End Routes API Request Payload ---")
        response = requests.post(ROUTES_API_URL, json=routes_api_payload, headers=headers)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        routes_api_result = response.json()
        logging.info("Received Google Routes API response.")
        return True, routes_api_result

    except requests.exceptions.RequestException as e:
        logging.error(f"Error calling Google Routes API: {e}", exc_info=True)
        return False, str(e)
    except Exception as e:
        logging.error(f"Unexpected error processing Google Routes API response: {e}", exc_info=True)
        return False, str(e)


# Helper function to parse duration string (same as before)
def parse_duration_string(duration_str):
    total_seconds = 0
    if duration_str and 's' in duration_str:
         parts = duration_str.replace('s', '').split('h')
         if len(parts) > 1:
             hours = int(parts[0].strip())
             minute_seconds_part = parts[1].strip()
             total_seconds += hours * 3600
             if 'm' in minute_seconds_part:
                 minutes, seconds = minute_seconds_part.split('m')
                 total_seconds += int(minutes.strip()) * 60 + int(seconds.strip())
             elif minute_seconds_part:
                 total_seconds += int(minute_seconds_part.strip())
         elif 'm' in duration_str:
             minutes, seconds = duration_str.split('m')
             total_seconds += int(minutes.strip()) * 60 + int(seconds.strip())
         else:
             total_seconds += int(duration_str.strip())
    return timedelta(seconds=total_seconds)


@functions_framework.http
def handle_optimize_delivery_route(request):
    """
    Cloud Function to handle the 'optimizeDeliveryRoute' operation for LogiFresh AI.
    Receives parameters, queries BQ for data, gets travel data via Google Routes API,
    and simulates optimization/calculates stop times. Includes Geocoding integration.
    """
    if request.method != 'POST':
        return json.dumps({'error': 'Method not allowed'}), 405

    request_json = request.get_json(silent=True)
    logging.info(f"Received optimize_delivery_route request: {request_json}")

    # --- 1. Parse & Validate Inputs ---
    origin_id = request_json.get('origin_location_id')
    destination_order_ids = request_json.get('destination_order_ids') # List of order IDs
    vehicle_id = request_json.get('vehicle_id') # Optional, for vehicle details/constraints
    route_date_str = request_json.get('route_date') # Optional

    if not origin_id or not destination_order_ids:
        logging.error("Missing required input: origin_location_id or destination_order_ids")
        return json.dumps({'status': 'error', 'message': 'Missing required input parameters'}), 400

    if not isinstance(destination_order_ids, list) or not destination_order_ids:
        return json.dumps({'status': 'error', 'message': 'Field "destination_order_ids" must be a non-empty list.'}), 400

    # --- 2. Get Location Data from BigQuery & Geocode ---
    try:
        # Get Origin Coordinates (Pass API Key to helper)
        origin_coords = get_location_coordinates(client, origin_id, GOOGLE_MAPS_API_KEY)
        
        logging.info(f"Origin Coords: {origin_coords}")
        

        if not origin_coords:
             logging.warning(f"Origin location ID {origin_id} not found or geocoding failed.")
             return json.dumps({'status': 'not_found', 'message': f'Origin location ID {origin_id} not found or coordinates unavailable.'}), 404 # Or 200 warning


        # Get Destination Details (Addresses/Locations, Quantities) & Geocode (Pass API Key)
        destination_details = get_destination_details_for_orders(client, destination_order_ids, GOOGLE_MAPS_API_KEY)

        logging.info(f"Destination Details (with coords): {destination_details}")
        
        if not destination_details:
             logging.warning(f"No details found or coordinates unavailable for destination order IDs: {destination_order_ids}")
             return json.dumps({'status': 'unavailable', 'message': 'Could not find valid destinations or coordinates for provided order IDs.'}), 200 # Partial success or warning

        # Get Vehicle Details (Optional)
        vehicle_info = None
        if vehicle_id:
            try:
                vehicle_info = get_vehicle_details(client, vehicle_id)
            except Exception as e:
                logging.warning(f"Could not retrieve vehicle details for {vehicle_id}: {e}") # Log error but continue

    except Exception as e:
        logging.error(f"Error retrieving data from BigQuery or initial geocoding: {e}", exc_info=True)
        return json.dumps({'status': 'error', 'message': 'Error retrieving necessary data'}), 500


    # --- 3. Call Google Routes API ---
    # For computeRoutes, the destination_details list should ideally be in the desired route order.
    # If optimization is needed, the VRP solver would reorder them *before* this API call.
    # In this snippet, we assume the input order is the one to calculate the route for.
    success, routes_api_result = call_routes_api_compute_routes(origin_coords, destination_details, GOOGLE_MAPS_API_KEY)

    if not success:
         logging.error(f"Routes API call failed: {routes_api_result}")
         return json.dumps({'status': 'error', 'message': 'Failed to compute route via routing service', 'details': routes_api_result}), 500

    if not routes_api_result.get('routes'):
         logging.warning("Routes API returned no routes.")
         return json.dumps({'status': 'unavailable', 'message': 'Could not compute a route with the provided locations.'}), 200


    # --- 4. Process Routes API Result & Calculate Stop Times ---
    main_route = routes_api_result['routes'][0]
    total_estimated_duration_str = main_route.get('duration', '0s')
    total_estimated_distance_meters = main_route.get('distanceMeters', 0)
    route_legs = main_route.get('legs', [])

    # Calculate estimated arrival time for each stop
    # Assume departure is 'now' + some buffer for loading/start
    estimated_departure_time = datetime.now() + timedelta(minutes=random.randint(5, 15)) # Simulate a short preparation time

    route_stops_with_times = []
    current_cumulative_duration = timedelta() # Duration since the estimated departure time from origin

    # Add the origin as the first "stop" with estimated departure
    route_stops_with_times.append({
        "stop_number": 0,
        "location_id": origin_id, # Use original ID
        "location_type": "Origin", # Or get type from BQ lookup helper if available
        "estimated_arrival_time": estimated_departure_time.isoformat(),
        "estimated_duration_from_previous_stop": "0s",
        "estimated_distance_from_previous_stop_meters": 0
    })

    # Process each leg to get arrival time at the *end* of the leg (the next stop)
    # The i-th leg is the journey FROM stop i TO stop i+1
    for i, leg in enumerate(route_legs):
        leg_duration_str = leg.get('duration', '0s')
        leg_distance_meters = leg.get('distanceMeters', 0)

        # Convert duration string to timedelta using the helper function
        leg_duration = parse_duration_string(leg_duration_str)
        current_cumulative_duration += leg_duration

        # The destination of leg i corresponds to the (i)-th destination in the
        # destination_details list (0-indexed) used to call the API
        if i < len(destination_details): # Should always be true if API works as expected
             destination_info = destination_details[i] # Get the details for the stop that is the end of this leg

             estimated_arrival = estimated_departure_time + current_cumulative_duration

             route_stops_with_times.append({
                 "stop_number": i + 1, # Stop numbering starts from 1 for destinations
                 "destination_id": destination_info['order_id'], # Or customer_id, warehouse_id etc.
                 "destination_address": destination_info['address'], # The location string
                 "estimated_arrival_time": estimated_arrival.isoformat(),
                 "estimated_duration_from_previous_stop": leg_duration_str,
                 "estimated_distance_from_previous_stop_meters": leg_distance_meters,
                 # Add quantity info here if needed for display
                 "total_kg_for_stop": destination_info.get('total_kg')
             })
             # Note: This doesn't account for service time at each stop. You'd need to add
             # a simulated or estimated service time after each arrival to the current_cumulative_duration
             # current_cumulative_duration += timedelta(minutes=estimated_service_time_at_stop)


    # --- 5. Prepare Response ---
    response_data = {
        'status': 'success',
        'vehicle_id': vehicle_id, # Include vehicle ID if provided in input
        'num_stops_on_route': len(route_legs) + 1, # Origin + number of legs
        'estimated_total_duration': total_estimated_duration_str,
        'estimated_total_distance_meters': total_estimated_distance_meters,
        'route_stops': route_stops_with_times, # List of stops with arrival times
        'route_date': route_date_str # Include requested date if provided
        # Add details like required capacity vs vehicle capacity here if calculated
    }

    logging.info(f"Route computation successful. Returning response.")
    return json.dumps(response_data), 200

# --- Include Helper Functions ---
# Paste get_location_coordinates, get_destination_details_for_orders, geocode_address, call_routes_api_compute_routes, parse_duration_string, get_vehicle_details here
# Make sure the helper functions call each other with the necessary parameters (like api_key)
# Ensure PROJECT_ID is read from environment for helpers too

# ... [Paste all helper functions from the previous response, ensuring they pass api_key appropriately] ...