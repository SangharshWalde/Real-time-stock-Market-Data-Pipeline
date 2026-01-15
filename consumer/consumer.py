# Real-Time Stock Data Consumer
import os
import json
import boto3
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Initialize environment
load_dotenv()

# S3 / MinIO Configuration
MINIO_URL = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
TARGET_BUCKET = os.getenv("MINIO_BUCKET", "bronze-transactions")

# Kafka Configuration
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "stock-quotes")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CONSUMER_GROUP = os.getenv("KAFKA_GROUP_ID", "transactions-consumer-group")

# Initialize S3 Client
object_storage = boto3.client(
    "s3",
    endpoint_url=MINIO_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def ensure_bucket_exists(bucket_name):
    """Checks if the bucket exists, creates it if not."""
    try:
        object_storage.head_bucket(Bucket=bucket_name)
        print(f"[INFO] Bucket '{bucket_name}' verified.")
    except Exception:
        print(f"[INFO] Creating bucket '{bucket_name}'...")
        object_storage.create_bucket(Bucket=bucket_name)

# Initialize Kafka Consumer
data_consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=CONSUMER_GROUP,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

if __name__ == "__main__":
    ensure_bucket_exists(TARGET_BUCKET)
    print(f"Listening for messages on topic '{TOPIC_NAME}'...")

    for message in data_consumer:
        record_data = message.value
        symbol = record_data.get("symbol", "UNKNOWN")
        timestamp = record_data.get("ingested_at", int(time.time()))
        
        file_key = f"{symbol}/{timestamp}.json"

        try:
            object_storage.put_object(
                Bucket=TARGET_BUCKET,
                Key=file_key,
                Body=json.dumps(record_data),
                ContentType="application/json"
            )
            print(f"[SAVED] {symbol} -> s3://{TARGET_BUCKET}/{file_key}")
        except Exception as e:
            print(f"[ERROR] Failed to save {symbol}: {e}")

                    