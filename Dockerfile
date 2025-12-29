# Use official slim Python image
FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app:${PYTHONPATH}"
COPY pyproject.toml setup.cfg setup.py requirements.txt* /app/ 2>/dev/null || true
COPY ./florida_property_scraper /app/florida_property_scraper
COPY . /app
RUN pip install --upgrade pip setuptools wheel \
 && pip install -e . || pip install --no-deps -e /app
CMD ["python","-m","florida_property_scraper"]
