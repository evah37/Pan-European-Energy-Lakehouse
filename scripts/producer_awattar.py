import time
import json
import requests
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'redpanda:9092'
TOPIC_NAME = 'energy-market-live'

# supported regions and their API endpoints
REGIONS = {
    "DE": "https://api.awattar.de/v1/marketdata",
    "AT": "https://api.awattar.at/v1/marketdata",
}


def fetch_awattar_data(region="DE"):
    """fetch day-ahead electricity prices from Awattar API for specified region"""
    url = REGIONS.get(region)
    if not url:
        logger.error(f"Unknown region: {region}")
        return []
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return response.json().get('data', [])
        else:
            logger.error(f"API Error for {region}: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Connection Failed for {region}: {e}")
        return []


def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    logger.info(f"Connected to Redpanda at {KAFKA_BROKER}")

    while True:
        total_count = 0
        for region, api_url in REGIONS.items():
            logger.info(f"Fetching live data from Awattar ({region})...")
            price_points = fetch_awattar_data(region)

            count = 0
            for point in price_points:
                message = {
                    "timestamp_ms": point['start_timestamp'],
                    "price_eur_mwh": point['marketprice'],
                    "unit": point['unit'],
                    "region": region,
                    "ingested_at": time.time()
                }
                producer.send(TOPIC_NAME, value=message)
                count += 1

            total_count += count
            logger.info(f"  {region}: sent {count} messages")

        producer.flush()
        logger.info(f"Total {total_count} messages sent to '{TOPIC_NAME}'. Sleeping for 60s...")
        time.sleep(60)


if __name__ == "__main__":
    time.sleep(10)
    run_producer()
