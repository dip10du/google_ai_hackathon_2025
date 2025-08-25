# FreshFlow AI: Intelligent Perishable Supply Chain Assistant

Welcome to the FreshFlow AI GitHub repository! This project demonstrates an innovative conversational AI solution designed to tackle the unique challenges of managing perishable food supply chains using Google Cloud Platform.

![FreshFlow AI Logo](./FreshFlowAI.png)

## 📊 Project Overview

FreshFlow AI is a Google Cloud-based multimodal agentic solution that streamlines perishable supply chains. Its multilingual conversational intelligence integrates data, automates workflows across farm, market, and logistics, and delivers predictive insights with real-time alerts – enabling faster, smarter decisions to improve efficiency and reduce waste from farm to fork.

FreshFlow AI aims to reduce waste, improve efficiency, and provide real-time visibility across the perishable food supply chain (from farm to fork). It does this by providing a unified, natural language interface powered by an Agentic AI architecture built on Google Cloud services.

## 💡 The Problem

The supply chain for perishable goods (fruits, vegetables, etc.) is highly inefficient, leading to significant waste (over 13% lost post-harvest, FAO SOFA 2019), primarily due to:
*   Limited shelf life & strict cold chain needs.
*   Volatile supply and demand.
*   Fragmented networks and manual processes.
*   Disconnected data silos preventing real-time visibility.

## ✨ The Solution: FreshFlow AI

FreshFlow AI is a Google Cloud-based multimodal agentic solution that streamlines perishable supply chains. Its multilingual conversational intelligence integrates data, automates workflows across farm, market, and logistics, and delivers predictive insights with real-time alerts – enabling faster, smarter decisions to improve efficiency and reduce waste from farm to fork.

It leverages:
*   **Conversational AI (Vertex AI Conversation):** As the intuitive user interface and workflow orchestrator.
*   **Centralized Data (BigQuery):** A single source of truth for all supply chain data.
*   **Backend Logic (Cloud Functions):** Specialized APIs performing operations against the data.

## 🤖 Agent Interactions & Scopes

![Agent Interaction Diagram](./FreshFlowAI-Agents.jpeg)

*Figure: Illustration of how different AI agents interact within FreshFlow AI, each operating within their defined scopes (e.g., inventory management, logistics, demand forecasting), and collaborating via orchestrated workflows to optimize the perishable supply chain.*

## 🏗️ High-Level Architecture

FreshFlow AI follows an agentic architecture where a central AI agent (Vertex AI Conversation) understands user goals via Playbooks and calls specific backend Cloud Functions ("Tools") to access or update data in BigQuery.

![FreshFlow AI Architecture](./FreshFlowAI.gif)

*Figure: High-level architecture of FreshFlow AI, illustrating the integration of Vertex AI Conversation, Cloud Functions, and BigQuery on Google Cloud Platform.*

## 📁 Project Structure

Below is an overview of the key folders included in this repository:

```
freshflowai/
├── functions/                # Cloud Functions source code and APIs
│   ├── deploy_functions.sh   # Automated deployment script for all Cloud Functions
│   ├── agrioptimize/         # AgriOptimize AI agent
│   ├── logifresh/            # LogiFresh AI agent
│   ├── marketflow/           # MarketFlow AI agent
│   └── ...                   # Other function directories
├── bq_tables_schemas/        # BigQuery table schema definitions
├── conversational-agents/    # Additional conversational agent logic and configs
├── DEPLOYMENT_COMMANDS.md    # Manual deployment instructions and commands
└── ReadMe.md                 # Project documentation (this file)
```

Each folder contains a `README.md` with more details about its contents and usage.

## 🚀 Quick Start & Deployment

### Prerequisites
- Google Cloud Platform account with billing enabled
- Google Cloud SDK (`gcloud`) installed and configured
- Python 3.9+ installed locally
- BigQuery dataset created in your GCP project

### Automated Deployment

FreshFlow AI includes an automated deployment script that deploys all Cloud Functions with a single command:

```bash
# Navigate to the functions directory
cd functions

# Make the deployment script executable
chmod +x deploy_functions.sh

# Configure your project settings
# Edit deploy_functions.sh and update:
# - PROJECT_ID: Your Google Cloud project ID
# - REGION: Deployment region (default: us-central1)
# - DATASET_ID: BigQuery dataset name

# Deploy all functions
./deploy_functions.sh
```

### What Gets Deployed

The `deploy_functions.sh` script automatically deploys all 9 Cloud Functions that power the FreshFlow AI system:

**Core Agent Functions:**
- `freshflow-agrioptimize` - Farm operations and harvest management
- `freshflow-logifresh` - Logistics and cold chain management  
- `freshflow-marketflow` - Market intelligence and demand forecasting

**Lookup Services:**
- `freshflow-getcustomerdetails` - Customer data retrieval
- `freshflow-getfirmdetails` - Farm profile information
- `freshflow-getproduct` - Product catalog queries
- `freshflow-getvehicledetails` - Vehicle fleet information

**Specialized Operations:**
- `freshflow-getoptimizedeliveryroute` - Route optimization
- `freshflow-reportcoldchainissue` - Cold chain alert processing (Pub/Sub triggered)

### Verification

After deployment, verify your functions are working:

```bash
# List deployed functions
gcloud functions list --filter="name:freshflow-*"

# Test a function (health check)
curl https://REGION-PROJECT_ID.cloudfunctions.net/freshflow-agrioptimize

# Expected response: {"status": "success", "message": "AgriOptimize AI Backend is running!"}
```

### Manual Deployment

For individual function deployment or troubleshooting, refer to the [`DEPLOYMENT_COMMANDS.md`](DEPLOYMENT_COMMANDS.md) file for detailed manual deployment instructions.

## 🔧 Configuration

Before deployment, ensure you have:

1. **Google Cloud Project Setup:**
   - Project ID configured in `deploy_functions.sh`
   - Required APIs enabled (Cloud Functions, BigQuery, Pub/Sub)
   - Service accounts with appropriate permissions

2. **BigQuery Dataset:**
   - Dataset created in your project
   - Tables created using schemas from `bq_tables_schemas/`

3. **Environment Variables:**
   - `GCP_PROJECT`: Your Google Cloud project ID
   - `BQ_DATASET`: BigQuery dataset name

## 📖 API Documentation

Each deployed Cloud Function provides RESTful APIs with action-based routing. Operations are specified via JSON request bodies.

Example API call:
```bash
curl -X POST https://REGION-PROJECT.cloudfunctions.net/freshflow-agrioptimize \
  -H "Content-Type: application/json" \
  -d '{
    "action": "log_harvest",
    "farm_id": "FARM123",
    "product_id": "TOMATO001",
    "harvested_quantity_kg": 500,
    "harvest_date": "2025-08-21"
  }'
```

## Contributing

Feel free to open issues or submit pull requests for improvements or bug fixes.
