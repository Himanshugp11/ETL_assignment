import boto3
import json
import psycopg2
from datetime import datetime
from botocore.exceptions import NoCredentialsError

# --- CONFIGURATION ---
QUEUE_URL = "http://localhost:4566/000000000000/test-queue"
REGION = "ap-south-1"

# Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "database": "events_db",
    "user": "admin",
    "password": "password123",
    "port": "5432"
}

# Initialize SQS Client with dummy credentials
sqs = boto3.client(
    'sqs', 
    endpoint_url="http://localhost:4566", 
    region_name=REGION,
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

def parse_date(date_val):
    try:
        if isinstance(date_val, (int, float)):
            return datetime.fromtimestamp(int(date_val)).strftime('%Y-%m-%d %H:%M:%S')
        dt = datetime.strptime(date_val, '%d/%m/%Y %H:%M:%S')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(date_val)

def persist_to_db(event):
    """Saves the converted event into PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Create table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trip_events (
                id INT,
                mail TEXT,
                name TEXT,
                departure TEXT,
                destination TEXT,
                start_date TIMESTAMP,
                end_date TIMESTAMP
            );
        """)
        
        # Insert Data
        cur.execute("""
            INSERT INTO trip_events (id, mail, name, departure, destination, start_date, end_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            event['id'], 
            event['mail'], 
            event['name'], 
            event['trip']['depaure'], # Required typo
            event['trip']['destination'],
            event['trip']['start_date'],
            event['trip']['end_date']
        ))
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ DB Error: {e}")

def transform(body):
    try:
        data = json.loads(body)
    except:
        return None

    res = {
        "id": data.get("id"),
        "mail": data.get("mail"),
        "name": f"{data.get('name', '')} {data.get('surname', '')}".strip(),
        "trip": {"depaure": "", "destination": "", "start_date": "", "end_date": ""}
    }

    if "route" in data and data["route"]:
        f, l = data["route"][0], data["route"][-1]
        res["trip"].update({
            "depaure": f.get("from"),
            "destination": l.get("to"),
            "start_date": parse_date(f.get("started_at")),
            "end_date": parse_date(l.get("started_at"))
        })
    elif "locations" in data and data["locations"]:
        f, l = data["locations"][0], data["locations"][-1]
        res["trip"].update({
            "depaure": f.get("location"),
            "destination": l.get("location"),
            "start_date": parse_date(f.get("timestamp")),
            "end_date": parse_date(l.get("timestamp"))
        })
    return res

def run_processor():
    print("🚀 Processor running...")
    while True:
        response = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=2)
        if 'Messages' not in response:
            print("✅ All messages processed.")
            break

        for msg in response['Messages']:
            converted = transform(msg['Body'])
            if converted:
                persist_to_db(converted)
                print(f"✔ Saved ID: {converted['id']}")
            
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg['ReceiptHandle'])

if __name__ == "__main__":
    run_processor()