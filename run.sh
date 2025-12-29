#!/usr/bin/env bash
set -euo pipefail
IMAGE_NAME="florida-scraper:latest"
echo "Building Docker image: ${IMAGE_NAME}"
docker build -t "${IMAGE_NAME}" .
echo "Running verification script inside container..."
docker run --rm --name florida-test "${IMAGE_NAME}" /bin/sh -c "python /app/verify.py || true; exec /bin/sh"
