FROM python:3.9-slim

# Install required packages
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy the mock server and sample data
COPY mock_delta_server.py .
COPY sample_data/*.csv /data/

# Install Flask, MinIO client, and requests
RUN pip install flask minio requests

# Create directories for config and data
RUN mkdir -p /config /data

# Expose port
EXPOSE 8080

# Run the mock server
CMD ["python", "mock_delta_server.py"]