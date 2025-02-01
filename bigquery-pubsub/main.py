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

    payload = cloudevent.data.get("protoPayload")
    if payload:
        print(f"API method: {payload.get('methodName')}")
        print(f"Resource name: {payload.get('resourceName')}")
        print(f"Principal: {payload.get('authenticationInfo', dict()).get('principalEmail')}")
        
    print("function successfully triggered!")
    
    publisher = pubsub_v1.PublisherClient()
    #topic_name="bqfunc_output"
    #topic_path = publisher.topic_path('invoice-processor-420417', topic_name)
    message="This is the message to pub/sub to trigger my workflow!".encode('utf-8')
    topic_name = 'projects/invoice-processor-420417/topics/bqfunc_output'
    future = publisher.publish(topic_name, data=message)
    message_id = future.result()
    print("message published")

