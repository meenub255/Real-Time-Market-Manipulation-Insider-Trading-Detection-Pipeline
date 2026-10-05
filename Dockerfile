FROM python:3.10-slim

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set default command (can be overridden in docker-compose)
CMD ["python", "src/dashboard.py"]
