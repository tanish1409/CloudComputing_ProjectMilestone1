from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os 
import csv
import time

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="my-project-40667-485013";
topic_name = "labelTopic";   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

def publish_csv():
    with open("Labels.csv", newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            message_dict = dict(row)           # CSV row → dictionary
            message_json = json.dumps(message_dict)
            message_bytes = message_json.encode("utf-8")

            future = publisher.publish(topic_path, message_bytes)
            future.result()

            print(f"Published: {message_dict}")

            time.sleep(2)  # 2-second delay between messages. Just for a better video recording.

if __name__ == "__main__":
    publish_csv()
 