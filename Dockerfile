# Dockerfile for building image with all ExDB CLI commands 
# and packages needed for running ETL workflow

FROM python:3.12-slim-bookworm

WORKDIR /app
ENV PATH=$PATH:/root/.local/bin

# Copy project files (use .dockerignore to exclude unnecessary files)
COPY . /app/

# Install all system dependencies in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential=12.* pkg-config=1.8.* \
        default-libmysqlclient-dev=1.1.* wget=1.21.* libcairo2=1.16.* git=1:2.* \
        nodejs=18.20.4+dfsg-1~deb12u1 libnode108=18.20.4+dfsg-1~deb12u1 npm=9.2.0~ds1-1 \
        libx11-dev=2:1.8.4-2+deb12u2 libxi-dev=2:1.8-1+b1 libxext-dev=2:1.3.4-1+b1 mesa-common-dev=22.3.6-1+deb12u1 \
        libgl1-mesa-dev=22.3.6-1+deb12u1 xvfb=2:21.1.7-3+deb12u10 xauth=1:1.1.2-1 tzdata=* \
    && rm -rf /var/lib/apt/lists/*

# Install mmseqs2
ADD https://github.com/soedinglab/MMseqs2/releases/download/13-45111/mmseqs-linux-avx2.tar.gz /opt/
RUN mkdir -p /opt/mmseqs2 \
    && tar xzf /opt/mmseqs-linux-avx2.tar.gz -C /opt/mmseqs2 --strip-components=1 \
    && rm /opt/mmseqs-linux-avx2.tar.gz \
    && ln -s /opt/mmseqs2/bin/mmseqs /usr/local/bin/mmseqs

# Install Python dependencies and the package
RUN pip install --no-cache-dir --upgrade "pip>=23.0.0" "hatch>=1.16.2" "wheel>=0.43.0" "setuptools>=40.8.0" \
    && pip install --no-cache-dir "pymongo>=4.10.1" \
    && hatch run pip install --no-cache-dir .

# Install node modules
WORKDIR /opt/modules/node_modules
RUN npm i molrender@0.9.0
