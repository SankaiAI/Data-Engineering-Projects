FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements files
COPY scripts/requirements.txt scripts/requirements.txt
COPY dataSource/requirements.txt dataSource/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r scripts/requirements.txt
RUN pip install --no-cache-dir -r dataSource/requirements.txt

# Copy the entire project
COPY . .

# Set PYTHONPATH to include current directory
ENV PYTHONPATH=/app

# Default command (will be overridden by docker-compose)
CMD ["python"]