# Dockerfile for radon-admin image
FROM python:3.13

ENV CQLENG_ALLOW_SCHEMA_MANAGEMENT=1
ENV PYTHONUNBUFFERED=1

# Install prerequisites
RUN apt -y update && \
    apt install -y libldap2-dev libsasl2-dev libev4 libev-dev && \
    pip install --upgrade pip && \
    apt clean

# Install cqlsh
RUN pip install cqlsh
RUN echo 'alias cqlsh="cqlsh dse"' >> ~/.bashrc

# Create destination folders
RUN mkdir /code

# Install radon-lib
COPY . /code/radon-lib
WORKDIR /code/radon-lib
RUN python -m pip install .

