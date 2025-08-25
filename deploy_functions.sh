#!/bin/bash

# FreshFlow AI - Cloud Functions Deployment Script
# This script deploys all Python Cloud Functions for the FreshFlow AI project

# Set your project configuration
PROJECT_ID="kmpjgbn-agbgagenticaihackat"  # Replace with your actual GCP project ID
DATASET_ID="freshflow"    # Replace with your BigQuery dataset ID
REGION="us-central1"              # Replace with your preferred region
RUNTIME="python313"               # Python runtime version
SERVICE_ACCOUNT_EMAIL="aaih-sa-01@kmpjgbn-agbgagenticaihackat.iam.gserviceaccount.com"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if gcloud is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    print_error "gcloud CLI is not installed. Please install it first."
    exit 1
fi

# Set the project
print_status "Setting GCP project to: $PROJECT_ID"
gcloud config set project $PROJECT_ID

# Array of function names and their entry points
declare -A FUNCTIONS
FUNCTIONS=(
    ["agrioptimize"]="agrioptimize_backend_function"
    ["getcustomerdetails"]="lookup_customer_by_name"
    ["getfirmdetails"]="lookup_farm_by_name"
    ["getoptimizedeliveryroute"]="handle_optimize_delivery_route"
    ["getproduct"]="lookup_product_by_name"
    ["getvehicledetails"]="lookup_vehicle_by_license"
    ["logifresh"]="logifresh_backend_function"
    ["marketflow"]="marketflow_backend_function"
    ["reportcoldchainissue"]="cold_chain_alerter_function"
)

# Base directory
BASE_DIR="functions"

print_status "Starting deployment of ${#FUNCTIONS[@]} Cloud Functions..."

# Deploy each function
for FUNCTION_NAME in "${!FUNCTIONS[@]}"; do
    ENTRY_POINT="${FUNCTIONS[$FUNCTION_NAME]}"
    print_status "Deploying function: $FUNCTION_NAME with entry point: $ENTRY_POINT"
    
    # Check if function directory exists
    if [ ! -d "$BASE_DIR/$FUNCTION_NAME" ]; then
        print_error "Directory $BASE_DIR/$FUNCTION_NAME does not exist. Skipping..."
        continue
    fi
    
    # Change to function directory
    cd "$BASE_DIR/$FUNCTION_NAME"
    
    # Deploy the function with appropriate trigger and entry point
    if [ "$FUNCTION_NAME" = "reportcoldchainissue" ]; then
        # This function uses cloud event trigger (Pub/Sub)
        print_status "Checking if Pub/Sub topic 'freshflow-cold-chain-alerts' exists..."
        if ! gcloud pubsub topics describe freshflow-cold-chain-alerts --project=$PROJECT_ID &> /dev/null; then
            print_status "Pub/Sub topic 'freshflow-cold-chain-alerts' does not exist. Creating..."
            gcloud pubsub topics create freshflow-cold-chain-alerts --project=$PROJECT_ID
        else
            print_status "Pub/Sub topic 'freshflow-cold-chain-alerts' already exists."
        fi

        print_status "Deploying with Pub/Sub trigger..."
        gcloud functions deploy freshflow-$FUNCTION_NAME \
            --runtime $RUNTIME \
            --trigger-topic freshflow-cold-chain-alerts \
            --entry-point $ENTRY_POINT \
            --region $REGION \
            --set-env-vars GCP_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID \
            --source . \
            --memory 256MB \
            --timeout 540s \
            --max-instances 10 \
            --service-account $SERVICE_ACCOUNT_EMAIL
    else
        # Standard HTTP trigger functions
        gcloud functions deploy freshflow-$FUNCTION_NAME \
            --runtime $RUNTIME \
            --trigger-http \
            --no-allow-unauthenticated \
            --entry-point $ENTRY_POINT \
            --region $REGION \
            --set-env-vars GCP_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID \
            --source . \
            --memory 256MB \
            --timeout 540s \
            --max-instances 10 \
            --service-account $SERVICE_ACCOUNT_EMAIL
    fi
    
    if [ $? -eq 0 ]; then
        print_success "Successfully deployed: $FUNCTION_NAME"
    else
        print_error "Failed to deploy: $FUNCTION_NAME"
    fi
    
    # Return to base directory
    cd ../..
    
    echo "----------------------------------------"
done

print_success "Deployment process completed!"
print_status "You can view your deployed functions at:"
print_status "https://console.cloud.google.com/functions/list?project=$PROJECT_ID"

# Optional: List all deployed functions
print_status "Listing all deployed functions:"
gcloud functions list --filter="name:*" --format="table(name,status,trigger.httpsTrigger.url)"
