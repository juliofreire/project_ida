FROM python:3.11.12-bookworm

# Set the working directory
WORKDIR /app

# Install system dependencies
COPY requirements.txt .

# Install Python dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Set executable permissions for the start script
RUN chmod +x /app/start.sh
CMD ["/app/start.sh"]

