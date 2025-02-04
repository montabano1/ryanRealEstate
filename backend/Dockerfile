FROM python:3.9-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ /app/src/
RUN mkdir -p data

# Add src directory to Python path
ENV PYTHONPATH=/app

ENV PORT=8080
EXPOSE 8080

# Change working directory to src
WORKDIR /app/src

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]
