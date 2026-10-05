"""
Live alert dashboard.

Serves a web UI and streams alerts from Redis Pub/Sub to the browser using
Server-Sent Events (SSE).

Run:  python src/dashboard.py   ->  http://localhost:5000
"""
import json
import os
import redis
from flask import Flask, Response, jsonify, send_from_directory

ALERT_CHANNEL = "alerts"
RECENT_ALERTS_KEY = "alerts:recent"
STATIC_DIR = os.path.join(os.path.dirname(__file__), "dashboard")

app = Flask(__name__, static_folder=STATIC_DIR)
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(STATIC_DIR, filename)


@app.route("/api/alerts/recent")
def recent_alerts():
    raw = redis_client.lrange(RECENT_ALERTS_KEY, 0, 99)
    return jsonify([json.loads(a) for a in raw])


@app.route("/api/alerts/stream")
def stream_alerts():
    def event_stream():
        pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(ALERT_CHANNEL)
        try:
            yield ": connected\n\n"
            while True:
                msg = pubsub.get_message(timeout=15)
                if msg is None:
                    yield ": keep-alive\n\n"
                elif msg["type"] == "message":
                    yield f"data: {msg['data']}\n\n"
        finally:
            pubsub.close()

    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True, debug=False)
