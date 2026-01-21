from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os 
import time

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]; 


project_id="hidden-conquest-485021-q5"; 
topic_name = "labelTopic"; 
subscription_id = "labelTopic-sub"; 

# create a subscriber to the subscriber for the project using the subscription_id
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = 'projects/{}/topics/{}'.format(project_id,topic_name);

print(f"Listening for messages on {subscription_path}..\n")

# A callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = message.data.decode("utf-8")
    record = json.loads(data)  # Deserialize JSON → dictionary

    print("Received record:")
    for key, value in record.items():
        print(f"  {key}: {value}")

    print("-" * 40)
    message.ack()

print("Listening for messages...")    
with subscriber:
    # The call back function will be called for each message recieved from the topic 
    # throught the subscription.
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        
while True:
    time.sleep(10)