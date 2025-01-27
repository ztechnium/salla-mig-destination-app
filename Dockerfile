# Use Python 3.10.12 as the base image
FROM python:3.10.12 AS base

# Builder stage for installing dependencies
FROM base AS builder
WORKDIR /airbyte/integration_code

# Update and upgrade system packages
RUN apt-get update && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends \
       gcc \
       libffi-dev \
       libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install dependencies
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

# Final stage for a clean runtime image
FROM base
WORKDIR /airbyte/integration_code

# Copy installed dependencies from builder stage
COPY --from=builder /install /usr/local

# Set timezone for consistency
COPY --from=builder /usr/share/zoneinfo/Etc/UTC /etc/localtime
RUN echo "Etc/UTC" > /etc/timezone

# Copy the source code
COPY main.py ./main.py
COPY destination_salla ./destination_salla

# Set environment variables
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"

# Default entrypoint for the container
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

# Labels for Airbyte metadata
LABEL io.airbyte.version="0.1.0"
LABEL io.airbyte.name="airbyte/destination-salla"