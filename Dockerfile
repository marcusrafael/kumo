FROM ubuntu:latest
MAINTAINER marcusrafael mrxl@cin.ufpe.br

# Kumo Migration Service dependencies
RUN apt update && \
    apt install -y curl && \
    apt install -y python3-dev && \
    apt install -y python3-pip && \
    apt install -y qemu-utils && \
    apt install -y rabbitmq-server && \
    pip3 install celery && \
    pip3 install flask && \
    pip3 install gunicorn

# AWS driver dependency
RUN pip3 install boto3

# AZ driver dependencies
RUN pip3 install azure-mgmt-compute && \
    pip3 install azure-mgmt-network && \
    pip3 install azure-storage

# GCP driver dependencies
RUN curl -sSL https://sdk.cloud.google.com | bash && \
    pip3 install google-api-python-client && \
    pip3 install google-cloud-storage && \
    pip3 install oauth2client

ENV PATH $PATH:/root/google-cloud-sdk/bin
ENV PYTHONUNBUFFERED=1

COPY . /kumo
WORKDIR /kumo

# Start RabbitMQ, Celery Thread Pool and Kumo API
CMD service rabbitmq-server start && \
    celery -A kumo.conductor.conductor worker --loglevel=info & \
    gunicorn -b 0.0.0.0:5000 kumo.api.api:app
