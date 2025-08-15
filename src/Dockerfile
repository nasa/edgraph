FROM python:3.8-slim

WORKDIR /app

# Copy only requirements.txt first for caching purposes
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Install netcat-openbsd and clean up apt cache
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Copy the entire context (which now includes the graph_ingest folder)
COPY . .


# Add to your Dockerfile
RUN apt-get update && apt-get install -y netcat-openbsd

# Copy wait-for-it.sh from its new location to the root and make it executable
#RUN cp graph_ingest/wait-for-it.sh /app/wait-for-it.sh && chmod +x /app/wait-for-it.sh