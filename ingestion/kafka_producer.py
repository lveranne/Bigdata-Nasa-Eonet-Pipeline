import os
import json
import time
import requests
from kafka import KafkaProducer

# 1. NASA API Key (Replace 'DEMO_KEY' with your own free key from api.nasa.gov)
NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY")

# 2. Kafka Producer initialization pointing to the Master Node
producer = KafkaProducer(
    bootstrap_servers=['192.168.56.10:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# NASA EONET v3 Endpoint
EONET_URL = "https://eonet.gsfc.nasa.gov/api/v3/events"

def fetch_and_produce():
    params = {
        "status": "open",
        "limit": 100,
        "api_key": NASA_API_KEY
    }
    
    try:
        response = requests.get(EONET_URL, params=params, timeout=10)
        
        # Inspect HTTP Rate Limit headers
        remaining = response.headers.get('X-RateLimit-Remaining')
        if remaining is not None:
            print(f"[RateLimit] Remaining requests for this window: {remaining}")
        
        # Handle Rate Limit reached (Error 429)
        if response.status_code == 429:
            print("[WARNING] Rate limit reached (HTTP 429)! Enforcing a 15-minute pause...")
            time.sleep(900)  # Wait 15 minutes before the next attempt
            return

        response.raise_for_status()
        events = response.json().get("events", [])
        
        published_count = 0
        for event in events:
            # Extract main category for the Kafka key
            categories = event.get("categories", [])
            category_key = categories[0]["id"] if categories else "general"
            
            # Publish to Kafka topic 'nasa-eonet-raw'
            producer.send(
                topic='nasa-eonet-raw',
                key=category_key,
                value=event
            )
            published_count += 1
            
        producer.flush()
        print(f"[OK] Successfully published {published_count} NASA EONET events to Kafka.")

    except requests.exceptions.RequestException as e:
        print(f"[API ERROR] Request failed for NASA EONET: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected Producer failure: {e}")

if __name__ == "__main__":
    # Adjusted according to DEMO_KEY limits (50 requests/day):
    # A delay of 1800 seconds (30 mins) = 48 requests per day.
    POLL_INTERVAL = 1800 if NASA_API_KEY == "DEMO_KEY" else 300  # 5 min for custom API key
    
    print(f"Starting NASA Kafka Producer (Key: {NASA_API_KEY}, Interval: {POLL_INTERVAL}s)")
    while True:
        fetch_and_produce()
        time.sleep(POLL_INTERVAL)
