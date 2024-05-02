# Dockerfile
FROM python:3.11

# Hostnames for dse and mqtt servers
ARG DSE_HOST
ARG MQTT_HOST

ENV PYTHONUNBUFFERED 1
ENV DSE_HOST=${DSE_HOST}
ENV MQTT_HOST=${MQTT_HOST}
ENV CQLENG_ALLOW_SCHEMA_MANAGEMENT 1

# Install prerequisites
RUN apt-get -y update &&  \
    apt-get install -y nano less libldap2-dev libsasl2-dev && \
    pip install --upgrade pip && \
    apt clean

# Create destination folders
RUN mkdir -p /code/radon-lib

# Install radon-lib
COPY radon-lib /code/radon-lib
WORKDIR /code/radon-lib
RUN pip install -r requirements.txt
RUN python setup.py develop
