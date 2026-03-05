FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/jobsearch

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY src/scrapers/proxy/requirements.txt /opt/jobsearch/src/scrapers/proxy/requirements.txt
RUN pip install --no-cache-dir -r /opt/jobsearch/src/scrapers/proxy/requirements.txt

CMD ["python", "/opt/jobsearch/src/scrapers/proxy/proxy_producer.py"]
