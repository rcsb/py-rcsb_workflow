# Dockerfile for building image with all ExDB CLI commands 
# and packages needed for running ETL workflow

# Use an official Python image as a base image
FROM python:3.13-slim-bookworm

# Set the working directory inside the container
WORKDIR /app
ENV PATH=$PATH:/root/.local/bin

# Copy project files (use .dockerignore to exclude unnecessary files)
COPY . /app/

# Install all system dependencies in one layer
RUN apt-get update && \
    # Confirmed versions that work: build-essential=12.9 pkg-config=1.8.1-1 default-libmysqlclient-dev=1.1.0
    apt-get install -y --no-install-recommends \
        build-essential=12.* pkg-config=1.8.* \
        default-libmysqlclient-dev=1.1.* wget=1.21.* libcairo2=1.16.* git=1:2.* \
        libx11-dev=2:1.8.* libxi-dev=2:1.8-1+b1 libxext-dev=2:1.3.4-1+b1 mesa-common-dev=22.3.* \
        libgl1-mesa-dev=22.3.* xvfb=2:21.* xauth=1:1.1.2-1 tzdata=* \
    && rm -rf /var/lib/apt/lists/*

# Install mmseqs2
ADD https://github.com/soedinglab/MMseqs2/releases/download/13-45111/mmseqs-linux-avx2.tar.gz /opt/
RUN mkdir -p /opt/mmseqs2 \
    && tar xzf /opt/mmseqs-linux-avx2.tar.gz -C /opt/mmseqs2 --strip-components=1 \
    && rm /opt/mmseqs-linux-avx2.tar.gz \
    && ln -s /opt/mmseqs2/bin/mmseqs /usr/local/bin/mmseqs

# Install Python dependencies and the package
RUN pip install --no-cache-dir --upgrade "pip>=23.0.0" "wheel>=0.43.0" "setuptools>=40.8.0" \
    && pip install --no-cache-dir "pymongo>=4.10.1" \
    # Use pip instead of hatch or uv, since the latter will only install CLIs into the virtual envs
    && pip install --no-cache-dir . --extra-index-url https://pypi.anaconda.org/OpenEye/simple

# Install Node.js 22 (molrender >= 0.10.0 requires Node >= 22; Debian bookworm only ships 18)
COPY --from=node:22-bookworm-slim /usr/local/bin/node /usr/local/bin/node
COPY --from=node:22-bookworm-slim /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && node --version && npm --version

# Install node modules
WORKDIR /opt/modules/node_modules
RUN npm i molrender@0.10.0

# TEST ONLY: run molrender under gdb so a segfault (e.g. 36za) prints a native backtrace.
# The gl 8.0.2 pin was tested and still crashed, so it has been removed to match production.
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends gdb=13.* \
    && rm -rf /var/lib/apt/lists/*
COPY docker/molrender-gdb-wrapper.sh /tmp/molrender-gdb-wrapper.sh
RUN mv /opt/modules/node_modules/molrender/build/bin/molrender.js /opt/modules/node_modules/molrender/build/bin/molrender-real.js \
    && install -m 0755 /tmp/molrender-gdb-wrapper.sh /opt/modules/node_modules/molrender/build/bin/molrender.js \
    && rm /tmp/molrender-gdb-wrapper.sh
