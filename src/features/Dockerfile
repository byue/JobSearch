FROM python:3.13-slim

WORKDIR /opt/jobsearch

COPY src/features/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src /opt/jobsearch/src

ENV PYTHONPATH=/opt/jobsearch/src

CMD ["uvicorn", "features.main:app", "--host", "0.0.0.0", "--port", "8010"]
