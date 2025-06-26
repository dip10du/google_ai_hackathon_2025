# FreshFlow AI: Intelligent Perishable Supply Chain Assistant

Welcome to the FreshFlow AI GitHub repository! This project demonstrates an innovative conversational AI solution designed to tackle the unique challenges of managing perishable food supply chains using Google Cloud Platform.

## 📊 Project Overview

FreshFlow AI aims to reduce waste, improve efficiency, and provide real-time visibility across the perishable food supply chain (from farm to fork). It does this by providing a unified, natural language interface powered by an Agentic AI architecture built on Google Cloud services.

## 💡 The Problem

The supply chain for perishable goods (fruits, vegetables, etc.) is highly inefficient, leading to significant waste (over 13% lost post-harvest, FAO SOFA 2019), primarily due to:
*   Limited shelf life & strict cold chain needs.
*   Volatile supply and demand.
*   Fragmented networks and manual processes.
*   Disconnected data silos preventing real-time visibility.

## ✨ The Solution: FreshFlow AI

FreshFlow AI acts as an intelligent hub, enabling users across the supply chain to interact with operational data and automate workflows conversationally.

It leverages:
*   **Conversational AI (Vertex AI Conversation):** As the intuitive user interface and workflow orchestrator.
*   **Centralized Data (BigQuery):** A single source of truth for all supply chain data.
*   **Backend Logic (Cloud Functions):** Specialized APIs performing operations against the data.

## 🏗️ High-Level Architecture

FreshFlow AI follows an agentic architecture where a central AI agent (Vertex AI Conversation) understands user goals via Playbooks and calls specific backend Cloud Functions ("Tools") to access or update data in BigQuery.

```mermaid
graph TD
    User[(Supply Chain User)] -- Converses with --> VTX_AI[("FreshFlow AI Agent\n(Vertex AI Conversation)")];

    subgraph Vertex AI Conversation
        direction LR
        VTX_AI --> Playbooks(Playbooks);
        Playbooks --> ParameterCollection[Parameter Collection\n($param_name)];
        Playbooks --> ToolCalls[Call Tool\n(${TOOL: ...})];
    end

    ToolCalls --> CloudFunctions[Backend Services\n(Cloud Functions)];

    subgraph Cloud Functions (Backend Tools)
        direction TB
        CF_Agri[AgriOptimize CF];
        CF_Market[MarketFlow CF];
        CF_Logi[LogiFresh CF];
        CF_Lookups[Lookup CFs];
        CF_Gateway[Gateway CF];
    end

    CloudFunctions --> BQ[("Centralized BigQuery\nDataset")];

    subgraph Data
        BQ --> DataStore(Operational Data Tables);
    end

    CF_Agri <--> BQ;
    CF_Market <--> BQ;
    CF_Logi <--> BQ;
    CF_Lookups --> BQ;

    ' Cross-Domain Interactions (via BQ & CF calls)
    CF_Market .-> BQ : Reads Inventory (Logi);
    CF_Market .-> BQ : Reads Schedules (Agri);
    CF_Logi .-> BQ : Reads Orders (Market);
    CF_Logi .-> BQ : Reads Farm/Warehouse (Agri/Logi);
    CF_Agri .-> CF_Logi : Triggers Pickup Request;


    User --> CF_Gateway[Gateway CF];
    CF_Gateway --> VTX_AI : Sends Audio/Text Query;
    VTX_AI --> CF_Gateway : Returns Text/Audio Response;

    style BQ fill:#aaddff
    style DataStore fill:#e0f0ff
    style VTX_AI fill:#ffffcc
    style Playbooks fill:#fffacd
    style ParameterCollection fill:#fff3cd
    style ToolCalls fill:#ffebcd
    style CF_Agri fill:#ccffcc
    style CF_Market fill:#ffcccc
    style CF_Logi fill:#ccccff
    style CF_Lookups fill:#e6e6fa
    style CF_Gateway fill:#f0f8ff