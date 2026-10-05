import json
import time
import random
# pyrefly: ignore [missing-import]
from confluent_kafka import Producer

KAFKA_TOPIC = 'financial_events'
KAFKA_BROKER = 'localhost:9092'

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_mock_event():
    tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    return {
        'ticker': random.choice(tickers),
        'price': round(random.uniform(100, 1000), 2),
        'volume': random.randint(10, 10000),
        'sentiment_score': round(random.uniform(-1, 1), 2),
        'timestamp': time.time()
    }

def main():
    p = Producer({'bootstrap.servers': KAFKA_BROKER})
    print(f"Starting producer, sending to topic {KAFKA_TOPIC}...")
    
    try:
        while True:
            event = generate_mock_event()
            # Introduce artificial anomalies occasionally
            if random.random() < 0.05:
                event['volume'] *= 50  # Huge volume spike
                
            p.produce(KAFKA_TOPIC, json.dumps(event).encode('utf-8'), callback=delivery_report)
            p.poll(0)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        p.flush()

if __name__ == '__main__':
    main()
