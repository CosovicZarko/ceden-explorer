# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed to build some packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy backend files
COPY main.py service_account.json requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port for Cloud Run
EXPOSE 8080

# Set environment variable for credentials
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service_account.json"

# Command to run FastAPI with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

COPY main.py index.html service_account.json requirements.txt ./