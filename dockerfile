# Dockerfile
FROM alpine:latest

USER root

RUN apk add --no-cache bash curl

# RUN apt-get install -y bash

# Copy the wait-for-it script
COPY wait-for-it.sh /wait-for-it.sh

# Copy your script
COPY debezium-connect.sh /debezium-connect.sh

# Make scripts executable
RUN chmod +x /wait-for-it.sh /debezium-connect.sh

# Command to wait for services and run your script
CMD ["./wait-for-it.sh", "clickhouse:8123", "postgres:5432"]
