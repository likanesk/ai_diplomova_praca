# Use a lightweight base image
FROM python:3.11-slim

# Install curl

RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install MinIO Client (mc)
RUN curl -O https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc && \
    mv mc /usr/local/bin/

# Create a non-root user
RUN adduser --disabled-password --gecos '' appuser

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Remove local .env file
RUN rm -f .env

# Change ownership of the application files to the non-root user
RUN chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

# Command to run your application
CMD ["fastapi", "run", "main.py", "--port", "8000"]


# 0. Network creation in CI/CD not needed it handled by K8
#   $ docker network create my_network

# 1. Run Docker container with Minio
# a.) production WITHOUT custom network
#   $ docker run -d -p 9000:9000 -p 9001:9001 --name minio \
# -e "MINIO_ACCESS_KEY=admin" \
# -e "MINIO_SECRET_KEY=password" \
# minio/minio server /data --console-address ":9001"
# b.) production WITH custom network
#   $ docker run -d -p 9000:9000 -p 9001:9001 --name minio --network my_network \
# -e "MINIO_ACCESS_KEY=admin" \
# -e "MINIO_SECRET_KEY=password" \
# minio/minio server /data --console-address ":9001"

# 2. Build Docker image
#   $ docker build -t ai-dp-rest-api .

# 3. Run Docker container with REST API project
# a.) production WITHOUT custom network
#   $ docker run -d  -p 8000:8000 --name ai-dp-rest-api ai-dp-rest-api
# b.) production WITH custom network
#   $ docker run -d  -p 8000:8000 --name ai-dp-rest-api --network my_network ai-dp-rest-api
