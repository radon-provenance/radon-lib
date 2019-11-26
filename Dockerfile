# Dockerfile
FROM python:3.6

ENV PYTHONUNBUFFERED 1

ENV DSE_HOST 172.17.0.3
ENV MQTT_HOST 172.17.0.5
ENV CQLENG_ALLOW_SCHEMA_MANAGEMENT 1

RUN apt-get -y update &&  \
    apt-get install -y nano less && \
    pip install --upgrade pip

# Create destination folders

RUN mkdir -p /code/radon-lib

# Install radon-lib
COPY radon-lib /code/radon-lib
WORKDIR /code/radon-lib
RUN pip install -r requirements.txt
# Patch dse
RUN patch /usr/local/lib/python3.6/site-packages/dse/cqlengine/connection.py < patch/patch_dse.patch 
RUN python setup.py develop
