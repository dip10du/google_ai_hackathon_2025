import functions_framework
import base64
import json
import sys
import os # Added import for os for environment variables

# --- Configuration ---
# Add any necessary environment variables your alerter needs here
# (e.g., for sending emails, connecting to other services)
# Example: EMAIL_RECIPIENT = os.environ.get('ALERT_EMAIL_RECIPIENT')

# BigQuery dependency might not be needed in the alerter function itself,
# but if your logic requires looking up more details (e.g., farm name for shipment),
# keep BQ client init. Otherwise, remove it.

# --- No BQ Init needed if function only sends alerts ---
# # Initialize BigQuery client (done outside the function handler)
# client = None
# try:
#     # ... (BQ client init code) ...
# except Exception as e:
#     print(f"WARNING: Could not initialize BQ client in alerter: {e}", file=sys.stderr)
#     client = None # Allow function to run without BQ if it's not critical

@functions_framework.cloud_event
def cold_chain_alerter_function(cloud_event):
    """Cloud Function triggered by Pub/Sub (CloudEvent format).
    Args:
        cloud_event (CloudEvent): The Pub/Sub message wrapped in a CloudEvent.
            <https://github.com/cloudevents/spec/blob/v1.0.1/cloudevents/spec.md>
    """
    # Access event data and attributes correctly
    # The Pub/Sub message payload and attributes are typically nested
    # under the 'data' and 'attributes' keys of the CloudEvent's *data*.
    event_data = cloud_event.data

    # Check if event_data is not None and is a dictionary
    if not isinstance(event_data, dict):
        print(f"Received unexpected event data format (not dict): {event_data}", file=sys.stderr)
        return # Exit if data format is unexpected

    pubsub_message = event_data.get('message') # Get the nested Pub/Sub message payload
    attributes = event_data.get('attributes') # Get event attributes if needed

    if not pubsub_message:
        print("Received CloudEvent without expected 'message' field in data.", file=sys.stderr)
        # You might still find message_id in attributes, but the data is likely missing
        if attributes:
             print(f"Event attributes: {attributes}", file=sys.stdout)
        return # Exit gracefully if not a standard Pub/Sub message format


    print(f"Processing Pub/Sub message...", file=sys.stdout)
    if attributes:
        print(f"  Attributes: {attributes}", file=sys.stdout)
        # You can get message_id, publish_time, etc. from attributes if needed
        message_id = attributes.get('messageId')
        publish_time = attributes.get('publishTime')
        print(f"  Message ID: {message_id}, Published: {publish_time}", file=sys.stdout)


    if 'data' in pubsub_message:
        # Decode the base64 data from the Pub/Sub message payload
        message_data_bytes = base64.b64decode(pubsub_message['data'])

        try:
            message_data_string = message_data_bytes.decode('utf-8')
            # Attempt to parse data as JSON (assuming your publisher sends JSON alerts)
            alert_data = json.loads(message_data_string)
            print(f"Decoded Pub/Sub message data (JSON): {json.dumps(alert_data, indent=2)}", file=sys.stdout)

            # --- Your Alerting Logic Here ---
            # Example: Log a specific message based on severity
            severity = alert_data.get('severity', 'Unknown')
            shipment_id = alert_data.get('shipment_id', 'Unknown Shipment')
            temperature = alert_data.get('temperature', 'N/A')
            message = alert_data.get('message', 'No message')
            # Access timestamp if needed: alert_data.get('timestamp')

            if severity == 'Critical':
                print(f"--- !!! CRITICAL ALERT !!! ---", file=sys.stderr)
                print(f"Shipment {shipment_id}: HIGH TEMP {temperature}°C - {message}", file=sys.stderr)
                # TODO: Add logic to send email/SMS to ALERT_RECIPIENT, trigger workflow, etc.
                # Example: if EMAIL_RECIPIENT: send_email(EMAIL_RECIPIENT, f"CRITICAL COLD CHAIN ALERT: {shipment_id}", f"Temperature: {temperature}°C")

            elif severity == 'Warning':
                print(f"--- Warning Alert ---", file=sys.stdout)
                print(f"Shipment {shipment_id}: Elevated Temp {temperature}°C - {message}", file=sys.stdout)
                # TODO: Add logic for warning notification (e.g., lower priority email, log to specific channel)

            else:
                 print(f"Info Alert: Shipment {shipment_id}, Temp {temperature}°C - {message}", file=sys.stdout)


        except json.JSONDecodeError:
            # Handle cases where the message data is not JSON
            print(f"Decoded Pub/Sub message data (Plain Text): {message_data_string}", file=sys.stdout)
            # Process non-JSON data if needed
            pass # Or handle appropriately
        except Exception as e:
             print(f"Error processing message data: {e}", file=sys.stderr)


    else:
        print("Pub/Sub message has no 'data' field in payload.", file=sys.stdout)
        # This might happen if the publisher only sent attributes

# --- Helper function to send email (Example, requires configuration) ---
# import sendgrid # Requires sendgrid library
# from sendgrid.helpers.mail import Mail, Email, To, PlainTextContent
# def send_email(recipient, subject, body):
#     try:
#         sg = sendgrid.SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))
#         from_email = Email(os.environ.get('ALERT_SENDER_EMAIL', 'alerts@yourcompany.com')) # Use a verified sender email
#         to_email = To(recipient)
#         plain_text_content = PlainTextContent(body)
#         mail = Mail(from_email, to_email, subject, plain_text_content)
#
#         # Get a JSON-ready representation of the Mail object
#         mail_json = mail.get()
#
#         # Send the email
#         response = sg.client.mail.send.post(request_body=mail_json)
#         print(f"Email sent to {recipient}. Status Code: {response.status_code}", file=sys.stdout)
#         return True, response.status_code
#     except Exception as e:
#         print(f"ERROR: Failed to send email: {e}", file=sys.stderr)
#         return False, str(e)