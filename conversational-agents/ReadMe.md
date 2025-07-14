# Google Vertex AI Conversational Agents

This repository contains all the files and resources required to define and deploy a Google Vertex AI Conversational Agent.

## Project Structure

Below is an overview of the main folders and files in this directory:

```
conversational-agents/
├── flows/                  # Dialog flow definitions  
├── generativeSettings/     # Generative AI settings and configurations  
├── intents/                # Definitions of agent intents  
├── playbooks/              # Playbook scripts for conversation management  
├── tools/                  # Custom tools and integrations  
├── webhooks/               # Webhook source code and deployment scripts  
└── agent.json              # agent configuration 
```


## Deployment

This folder contains a ZIP file, "exported_agent_freshflow.zip" with all the necessary components for a Conversational Agent, including agents, playbooks, tools, intents, and more.

The ZIP file is intended for import into Vertex AI Conversational Agent, allowing for easy setup and deployment of conversational experiences.

For detailed steps on how to import or export agents, please refer to the [official Vertex AI documentation](https://cloud.google.com/dialogflow/cx/docs/concept/agent#export).

## Contributing

Feel free to open issues or submit pull requests for improvements or bug fixes.