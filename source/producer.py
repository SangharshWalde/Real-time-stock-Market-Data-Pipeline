# Real-Time Stock Data Producer
import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Initialize environment
load_dotenv()

# Configuration Constants
FINNHUB_API_KEY = os.getenv("API_KEY_HONNUB")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC", "stock-quotes")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
FINNHUB_ENDPOINT = "https://finnhub.io/api/v1/quote"
TARGET_SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

# Initialize Kafka Producer
stream_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda message: json.dumps(message).encode("utf-8")
)

def retrieve_stock_data(ticker_symbol):
    """
    Fetches the latest quote for a given stock symbol from Finnhub.
    """
    request_url = f"{FINNHUB_ENDPOINT}?symbol={ticker_symbol}&token={FINNHUB_API_KEY}"
    try:
        api_response = requests.get(request_url)
        api_response.raise_for_status()
        
        stock_data = api_response.json()
        stock_data["symbol"] = ticker_symbol
        stock_data["ingested_at"] = int(time.time())
        
        return stock_data
    except Exception as error:
        print(f"[ERROR] Failed to fetch data for {ticker_symbol}: {error}")
        return None

if __name__ == "__main__":
    print(f"Starting producer for symbols: {TARGET_SYMBOLS}")
    while True:
        for ticker in TARGET_SYMBOLS:
            market_data = retrieve_stock_data(ticker)
            if market_data:
                print(f"Streaming update: {market_data}")
                stream_producer.send(KAFKA_TOPIC_NAME, value=market_data)
        
        # Rate limit to avoid API throttling
        time.sleep(6)

