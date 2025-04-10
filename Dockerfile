# Dockerfile for building image with all ExDB CLI commands 
# and packages needed for running ETL workflow

# Use an official Python image as a base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app
ENV PATH=$PATH:/root/.local/bin

# Copy requirements file
COPY ./requirements.txt /app/requirements.txt

# Install system dependencies
RUN apt-get update \
    # Confirmed versions that work: build-essential=12.9 pkg-config=1.8.1-1 default-libmysqlclient-dev=1.1.0
    && apt-get install -y --no-install-recommends build-essential=12.* pkg-config=1.8.* default-libmysqlclient-dev=1.1.* wget=1.21.* libcairo2=1.16.* git=1:2.* \
    && rm -rf /var/lib/apt/lists/*

# Install mmseqs2
ADD https://github.com/soedinglab/MMseqs2/releases/download/13-45111/mmseqs-linux-avx2.tar.gz /opt/
RUN mkdir -p /opt/mmseqs2 \
    && tar xzf /opt/mmseqs-linux-avx2.tar.gz -C /opt/mmseqs2 --strip-components=1 \
    && rm /opt/mmseqs-linux-avx2.tar.gz \
    && ln -s /opt/mmseqs2/bin/mmseqs /usr/local/bin/mmseqs

# Install the required Python packages
RUN pip install --no-cache-dir --upgrade "pip>=23.0.0" "setuptools>=40.8.0" "wheel>=0.43.0" \
    && pip install --no-cache-dir --user -r /app/requirements.txt \
    && pip install --no-cache-dir "pymongo>=4.10.1"

# Install node and molrender.js
RUN mkdir -p /opt/modules/node_modules
WORKDIR /opt/modules/node_modules
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install --no-install-recommends -y nodejs=18.* npm=9.* \
    && npm i molrender \
    && apt-get -yqq install --no-install-recommends libgl1-mesa-dev=* xvfb=* xauth=* \
    && rm -rf /var/lib/apt/lists/*

# Install the local code
WORKDIR /app
COPY . /app/
RUN pip install --no-cache-dir . \
    && pip install --no-cache-dir git+https://github.com/rcsb/py-rcsb_utils_dictionary.git@pdb-ihm-2 \
    && pip install --no-cache-dir git+https://github.com/rcsb/py-rcsb_db.git@dev-dwp-ihm \
    && pip freeze
