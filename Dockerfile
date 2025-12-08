# Dockerfile for building image with all ExDB CLI commands 
# and packages needed for running ETL workflow

# Use an official Python image as a base image
# UPDATE TO 3.12
FROM python:3.10-slim-bookworm

# Set the working directory inside the container
WORKDIR /app
ENV PATH=$PATH:/root/.local/bin

# Copy project files
COPY pyproject.toml /app/
COPY . /app/

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

# Use Hatch to install the project and its dependencies
RUN hatch run pip install --no-cache-dir .

# Install the required Python utilities
RUN pip install --no-cache-dir --upgrade "pip>=23.0.0" "hatch>=1.16.2" "wheel>=0.43.0" "setuptools>=40.8.0" \
    && pip install --no-cache-dir "pymongo>=4.10.1"

# Install node and molrender.js
RUN mkdir -p /opt/modules/node_modules
WORKDIR /opt/modules/node_modules
RUN apt-get -y install --no-install-recommends --reinstall tzdata=*
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install --no-install-recommends -y nodejs=18.20.4+dfsg-1~deb12u1 libnode108=18.20.4+dfsg-1~deb12u1 npm=9.2.0~ds1-1 \
    libx11-dev=2:1.8.4-2+deb12u2 libxi-dev=2:1.8-1+b1 libxext-dev=2:1.3.4-1+b1 mesa-common-dev=22.3.6-1+deb12u1 \
    && npm i molrender@0.9.0 \
    && apt-get -yqq install --no-install-recommends libgl1-mesa-dev=22.3.6-1+deb12u1 xvfb=2:21.1.7-3+deb12u10 xauth=1:1.1.2-1 \
    && rm -rf /var/lib/apt/lists/*

# Install the local code
WORKDIR /app
RUN hatch run pip install --no-cache-dir .
    