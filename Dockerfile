FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app:${PYTHONPATH}"

# Copy the whole repository into the image in one step
COPY . /app

# Install dependencies and the package (editable)
RUN pip install --upgrade pip setuptools wheel \
 && pip install -e .

CMD ["python", "-m", "florida_property_scraper"]
