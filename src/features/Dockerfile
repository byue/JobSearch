FROM python:3.13-slim

WORKDIR /opt/jobsearch

ENV PYTHONPATH=/opt/jobsearch/src \
    XDG_CACHE_HOME=/opt/jobsearch/.cache \
    HF_HOME=/opt/jobsearch/.cache/huggingface

COPY src/features/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src /opt/jobsearch/src

RUN python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='BAAI/bge-small-en-v1.5')"

CMD ["uvicorn", "features.main:app", "--host", "0.0.0.0", "--port", "8010"]
