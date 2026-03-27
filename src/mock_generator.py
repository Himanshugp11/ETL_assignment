import boto3
import json
import uuid

# 1. Setup SQS Client for LocalStack
# Note: Using 'ap-south-1' to match your Go script's configuration
sqs = boto3.client(
    'sqs', 
    endpoint_url="http://localhost:4566", 
    region_name='ap-south-1',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

QUEUE_NAME = "test-queue"

# 2. Define the exact messages from the Go source
MESSAGES = [
    "malformed", # Message 0: Malformed string
    {
        "id": 3,
        "mail": "aaa@gmail.com",
        "name": "Mahmoud",
        "surname": "Mahmoudi",
        "route": [
            {"from": "B", "to": "C", "duration": 15, "started_at": "10/10/2022 11:10:00"},
            {"from": "C", "to": "E", "duration": 10, "started_at": "10/10/2022 11:17:15"}
        ]
    },
    {
        "id": 5,
        "mail": "mmm@nocompany.com",
        "name": "Kacper",
        "surname": "Kacperian",
        "locations": [
            {"location": "F", "timestamp": 1667999699},
            {"location": "G", "timestamp": 1668975653}
        ]
    },
    {
        "id": 6,
        "mail": "kkk@nocompany.com",
        "name": "Tulga",
        "surname": "Khan",
        "age": "25",
        "workplace": "home",
        "locations": [
            {"location": "A", "timestamp": 1667975699},
            {"location": "B", "timestamp": 1667975893}
        ]
    },
    {
        "id": 6,
        "mail": "kkk@nocompany.com",
        "name": "Tulga",
        "surname": "Khan",
        "age": "25",
        "workplace": "office1",
        "locations": [
            {"location": "A", "timestamp": 1667975699},
            {"location": "B", "timestamp": 1667975893}
        ]
    }
]

def run_generator():
    # Create the Queue
    print(f"Creating SQS queue [{QUEUE_NAME}]...")
    try:
        create_resp = sqs.create_queue(QueueName=QUEUE_NAME)
        queue_url = create_resp['QueueUrl']
        print(f"Queue created, url: {queue_url}")
    except Exception as e:
        print(f"Error creating queue: {e}")
        return

    # Prepare Batch Entries
    entries = []
    for msg in MESSAGES:
        body = msg if isinstance(msg, str) else json.dumps(msg)
        entries.append({
            'Id': str(uuid.uuid4()),
            'MessageBody': body,
            'DelaySeconds': 1
        })

    # Send Messages in Batch
    print("Sending messages to the queue...")
    send_resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)

    # Check for failures
    if 'Failed' in send_resp and len(send_resp['Failed']) > 0:
        for failure in send_resp['Failed']:
            print(f"Message failed: {failure.get('Message')}")
    else:
        print("All messages sent successfully!")

if __name__ == "__main__":
    run_generator()