import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import redis
import joblib
import numpy as np

ALERT_CHANNEL = "alerts"
RECENT_ALERTS_KEY = "alerts:recent"
ALERT_TTL_SECONDS = 24 * 3600
MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "models", "isolation_forest.joblib")

# Initialize Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Load the trained Isolation Forest (run `python src/train_model.py` first)
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Model not found at {MODEL_PATH}. Run: python src/train_model.py")
model = joblib.load(MODEL_PATH)


def build_features(volume, sentiment):
    # Must match train_model.build_features
    return np.array([[volume / 10_000.0, sentiment]])


def detect_anomaly(volume, sentiment):
    if volume is None or sentiment is None:
        return 0
    # Predict returns -1 for anomaly, 1 for normal
    pred = model.predict(build_features(volume, sentiment))[0]
    return 1 if pred == -1 else 0

detect_anomaly_udf = udf(detect_anomaly, LongType())

def process_batch(df, epoch_id):
    # Collect anomalies and send to Redis
    anomalies = df.filter(col("is_anomaly") == 1).collect()
    for row in anomalies:
        alert = {
            "ticker": row["ticker"],
            "price": row["price"],
            "volume": row["volume"],
            "sentiment": row["sentiment_score"],
            "timestamp": row["timestamp"]
        }
        payload = json.dumps(alert)
        key = f"alert:{row['ticker']}:{int(row['timestamp'])}"
        pipe = redis_client.pipeline()
        pipe.set(key, payload, ex=ALERT_TTL_SECONDS)
        pipe.lpush(RECENT_ALERTS_KEY, payload)
        pipe.ltrim(RECENT_ALERTS_KEY, 0, 199)
        pipe.publish(ALERT_CHANNEL, payload)
        pipe.execute()
        print(f"ALERT: Anomalous activity detected and stored in Redis - {alert}")

def main():
    spark = SparkSession.builder \
        .appName("FinancialSurveillance") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("sentiment_score", DoubleType(), True),
        StructField("timestamp", DoubleType(), True)
    ])

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "financial_events") \
        .load()

    # Parse JSON
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Apply ML model for anomaly detection
    analyzed_df = parsed_df.withColumn(
        "is_anomaly", 
        detect_anomaly_udf(col("volume"), col("sentiment_score"))
    )

    # Write anomalies to Redis using foreachBatch
    query = analyzed_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
