# Image for rendering structure JPEGs with molrender and uploading them to SeaweedFS S3.
# Build from this directory: docker build -t structure-images structure-images/

# molrender depends on headless-gl, which has no prebuilt binary for musl and is compiled here
FROM harbor.devops.k8s.rcsb.org/dockerhub/node:24-alpine AS build
RUN apk add --no-cache python3 make g++ pkgconf libx11-dev libxi-dev libxext-dev mesa-dev
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

FROM harbor.devops.k8s.rcsb.org/dockerhub/node:24-alpine
# Xvfb gives headless-gl an X display; mesa provides software OpenGL
RUN apk add --no-cache xvfb mesa-gl mesa-dri-gallium libx11 libxi libxext
WORKDIR /app
COPY --from=build /app/node_modules ./node_modules
COPY package.json ./
COPY src ./src
RUN ln -s /app/src/cli.js /usr/local/bin/structure-images

CMD ["structure-images", "--help"]
