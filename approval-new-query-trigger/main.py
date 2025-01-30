import functions_framework
from google.cloud import pubsub_v1
import json
from google.cloud import logging

# CloudEvent function to be triggered by an Eventarc Cloud Audit Logging trigger
# Note: this is NOT designed for second-party (Cloud Audit Logs -> Pub/Sub) triggers!
@functions_framework.cloud_event
def hello_auditlog(cloudevent):
    # Print out the CloudEvent's (required) `type` property
    # See https://github.com/cloudevents/spec/blob/v1.0.1/spec.md#type
    print(f"Event type: {cloudevent['type']}")

    # Print out the CloudEvent's (optional) `subject` property
    # See https://github.com/cloudevents/spec/blob/v1.0.1/spec.md#subject
    if 'subject' in cloudevent:
        # CloudEvent objects don't support `get` operations.
        # Use the `in` operator to verify `subject` is present.
        print(f"Subject: {cloudevent['subject']}")

    # Print out details from the `protoPayload`
    # This field encapsulates a Cloud Audit Logging entry
    # See https://cloud.google.com/logging/docs/audit#audit_log_entry_structure
    print("Function has been triggered.")


    payload = cloudevent.data.get("protoPayload")
    # Extracting meta data.
    metadata = payload.get('metadata', {})
    
    # If meta data found continue. 
    if metadata:
        
        # Checking meta data for table change action.
        table_data_change = metadata.get('tableDataChange')
        
        if table_data_change: #publish the message
                publisher=pubsub_v1.PublisherClient()
                message="This is a message to indicate that a new row has been added to BigQuery Data.".encode('utf-8')
                topic_name='projects/invoice-processor-420417/topics/bigquery_trigger'
                future=publisher.publish(topic_name,data=message)
                print("message published!")

        else: 
            print("table data was not actually changed, so no pub/sub published.")

    print("function execution completed")








