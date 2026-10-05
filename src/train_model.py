"""
Train the Isolation Forest anomaly detector and persist it to models/.

The model learns what "normal" trading looks like, using features that match
what the streaming pipeline sees at inference time:
    [volume / VOLUME_SCALE, sentiment_score]

Volume is scaled so a typical trade sits in ~[0, 1]; manipulation-style
volume spikes land far outside that range and are isolated quickly.
"""
import os
import numpy as np
import joblib
from sklearn.ensemble import IsolationForest

MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "isolation_forest.joblib")


VOLUME_SCALE = 10_000.0


def build_features(volume, sentiment):
    volume = np.asarray(volume, dtype=float)
    sentiment = np.asarray(sentiment, dtype=float)
    return np.column_stack([volume / VOLUME_SCALE, sentiment])


def generate_training_data(n_normal=50_000, n_anomalies=1_000, seed=42):
    rng = np.random.default_rng(seed)

    # Normal market activity (mirrors producer.py's baseline distribution)
    vol = rng.integers(10, 10_000, n_normal)
    sent = rng.uniform(-1, 1, n_normal)

    # A small share of known manipulation-like patterns: volume spikes
    a_vol = rng.integers(10, 10_000, n_anomalies) * rng.integers(20, 100, n_anomalies)
    a_sent = rng.uniform(-1, 1, n_anomalies)

    X = np.vstack([build_features(vol, sent), build_features(a_vol, a_sent)])
    y = np.concatenate([np.zeros(n_normal), np.ones(n_anomalies)])
    return X, y


def main():
    X, y = generate_training_data()
    contamination = y.mean()

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)

    pred = (model.predict(X) == -1).astype(int)
    tp = int(((pred == 1) & (y == 1)).sum())
    fp = int(((pred == 1) & (y == 0)).sum())
    fn = int(((pred == 0) & (y == 1)).sum())
    precision = tp / max(tp + fp, 1)
    recall = tp / max(tp + fn, 1)

    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model, MODEL_PATH)

    print(f"Model saved to {os.path.abspath(MODEL_PATH)}")
    print(f"Training precision: {precision:.3f}  recall: {recall:.3f}")


if __name__ == "__main__":
    main()
