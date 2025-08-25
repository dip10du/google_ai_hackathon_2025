# Deploying and Understanding Cloud Functions in the `functions` Directory

This guide provides step-by-step instructions to deploy Python-based cloud functio## Additional Notes

- **Automated Deployment**: Use `deploy_functions.sh` for quickest setup of all functions
- **Manual Commands**: Detailed individual deployment commands available in `../DEPLOYMENT_COMMANDS.md`
- **Security**: Functions deployed with public access for demo purposes; implement authentication for production
- **Monitoring**: All functions automatically integrate with Google Cloud Logging and Monitoring
- **Entry Points**: Each function has a specific entry point - ensure correct mapping during deployment
- **Dependencies**: All functions use Python 3.11 runtime with BigQuery and Flask dependencies
- **Troubleshooting**: Check Cloud Logging for deployment errors and function execution logs

## Function Dependencies

Each function requires:
```txt
google-cloud-bigquery>=3.11.4
flask>=2.3.0
functions-framework>=3.4.0
```

For cold chain alerts:
```txt
google-cloud-pubsub>=2.18.0
```
```ated in each subfolder of the `functions` directory, along with a brief description of each function's purpose. The repository includes an automated deployment script for deploying all functions at once, as well as manual deployment options.

## Cloud Functions Overview

Below is a list of cloud functions, each implemented in its own subfolder:

- **agrioptimize**: AgriOptimize AI agent for farm operations, harvest logging, quality control, and pickup scheduling
- **getcustomerdetails**: Retrieves detailed customer information from the database
- **getfirmdetails**: Fetches farm profile details and information
- **getoptimizedeliveryroute**: Calculates optimal delivery routes for logistics operations
- **getproduct**: Retrieves product catalog information and details
- **getvehicledetails**: Obtains vehicle fleet information for logistics planning
- **logifresh**: LogiFresh AI agent for logistics, inventory management, and cold chain monitoring
- **marketflow**: MarketFlow AI agent for market intelligence, demand forecasting, and order processing
- **reportcoldchainissue**: Event-driven function for processing cold chain alerts via Pub/Sub

> _Note: Each function uses action-based routing where operations are specified via JSON request bodies with an 'action' parameter._

## Prerequisites

- **Python 3.9+** and **pip** installed on your machine
- **Google Cloud SDK** installed and configured ([Installation Guide](https://cloud.google.com/sdk/docs/install))
- Access to your Google Cloud project with necessary permissions
- **BigQuery dataset** created in your project for data storage

## Quick Deployment (Recommended)

### Automated Deployment Script

The repository includes `deploy_functions.sh` which deploys all 9 Cloud Functions automatically:

```bash
# Navigate to project root
cd google_ai_hackathon_2025

# Make script executable
chmod +x deploy_functions.sh

# Configure your settings in deploy_functions.sh:
# - PROJECT_ID: Your Google Cloud project ID
# - DATASET_ID: Your BigQuery dataset name
# - REGION: Deployment region (default: us-central1)

# Deploy all functions
./deploy_functions.sh
```

### What Gets Deployed

The script deploys all functions with:
- **Entry Points**: Specific function entry points for each service
- **HTTP Triggers**: For API functions (8 functions)
- **Pub/Sub Trigger**: For cold chain alerts (1 function)
- **Environment Variables**: GCP_PROJECT and BQ_DATASET automatically set
- **Resource Limits**: 256MB memory, 540s timeout, max 10 instances

## Manual Deployment Steps

For individual function deployment or troubleshooting, follow these steps:

### 1. Install Dependencies

Navigate to the specific function's subfolder and install required packages:

```bash
cd functions/<function_subfolder>
pip install -r requirements.txt
```

### 2. Configure Environment

Set up required environment variables:
- `GCP_PROJECT`: Your Google Cloud project ID
- `BQ_DATASET`: Your BigQuery dataset name

### 3. Deploy Individual Function

#### Using gcloud CLI:

```bash
gcloud functions deploy <function_name> \
    --runtime python311 \
    --trigger-http \
    --allow-unauthenticated \
    --entry-point <entry_point_function> \
    --region us-central1 \
    --set-env-vars GCP_PROJECT=your-project-id,BQ_DATASET=your-dataset \
    --source . \
    --memory 256MB \
    --timeout 540s
```

**Function Entry Points:**
- `agrioptimize` → `agrioptimize_backend_function`
- `getcustomerdetails` → `lookup_customer_by_name`
- `getfirmdetails` → `lookup_farm_by_name`
- `getoptimizedeliveryroute` → `handle_optimize_delivery_route`
- `getproduct` → `lookup_product_by_name`
- `getvehicledetails` → `lookup_vehicle_by_license`
- `logifresh` → `logifresh_backend_function`
- `marketflow` → `marketflow_backend_function`
- `reportcoldchainissue` → `cold_chain_alerter_function` (Pub/Sub trigger)


### 4. Verify Deployment

After deployment, test the functions:

```bash
# List all deployed functions
gcloud functions list --filter="name:freshflow-*"

# Test a function (health check)
curl https://REGION-PROJECT_ID.cloudfunctions.net/freshflow-agrioptimize

# Test with action-based request
curl -X POST https://REGION-PROJECT_ID.cloudfunctions.net/freshflow-agrioptimize \
  -H "Content-Type: application/json" \
  -d '{"action": "get_harvest_advice", "farm_id": "FARM123"}'
```

## API Usage

All functions use action-based routing. Send POST requests with JSON bodies containing an `action` parameter:

### Example API Calls:

```bash
# AgriOptimize - Log Harvest
curl -X POST https://your-function-url/freshflow-agrioptimize \
  -H "Content-Type: application/json" \
  -d '{
    "action": "log_harvest",
    "farm_id": "FARM123",
    "product_id": "TOMATO001",
    "harvested_quantity_kg": 500,
    "harvest_date": "2025-08-21"
  }'

# MarketFlow - Get Market Prices
curl -X POST https://your-function-url/freshflow-marketflow \
  -H "Content-Type: application/json" \
  -d '{
    "action": "get_market_prices",
    "product_id": "TOMATO001",
    "region": "California"
  }'

# LogiFresh - Check Inventory
curl -X POST https://your-function-url/freshflow-logifresh \
  -H "Content-Type: application/json" \
  -d '{
    "action": "check_inventory_status",
    "product_id": "TOMATO001",
    "location_id": "WH_CA_001"
  }'
```

## Additional Notes

- Update any placeholder values with your actual configuration.
- For advanced settings or troubleshooting, consult your cloud provider’s official documentation.
- Ensure you follow security best practices when exposing cloud functions.
```