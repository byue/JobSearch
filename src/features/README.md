# Features Service

FastAPI service for shared feature extraction.

Current endpoints:
- `POST /job_skills`
- `POST /query_embedding`
- `POST /normalize_locations`

## `POST /job_skills`

Request:

```json
{
  "text": "Experience with Python, Docker, and Kubernetes"
}
```

Response:

```json
{
  "status": 200,
  "error": null,
  "skills": ["Python"],
  "embedding": [0.1, -0.2]
}
```

## `POST /query_embedding`

Request:

```json
{
  "text": "distributed systems"
}
```

Response:

```json
{
  "status": 200,
  "error": null,
  "embedding": [0.1, -0.2]
}
```

## `POST /normalize_locations`

Request:

```json
{
  "locations": ["Seattle, WA, USA", "London, UK"]
}
```

Response:

```json
{
  "status": 200,
  "error": null,
  "locations": [
    {"city": "Seattle", "region": "Washington", "country": "United States"},
    {"city": "London", "region": null, "country": "United Kingdom"}
  ]
}
```

Each normalized field may be `null` when the input is ambiguous or incomplete.

## Environment

- `JOBSEARCH_FEATURES_TECHNICAL_PATH`
- `JOBSEARCH_FEATURES_KEYWORD_PATH`
- `JOBSEARCH_FEATURES_SPACY_MODEL`
- `JOBSEARCH_FEATURES_EMBEDDING_MODEL`
- `JOBSEARCH_FEATURES_API_URL`
- `JOBSEARCH_FEATURES_API_TIMEOUT_SECONDS`

## Run Locally

```bash
PYTHONPATH=src .venv/bin/python -m uvicorn features.main:app --host 0.0.0.0 --port 8010
```

## CLI

```bash
make features-api ARGS='get-job-skills --text "Python and Docker experience"'
make features-api ARGS='normalize-locations --location "Seattle, WA, USA" --location "London, UK"'
make test-location-normalization ARGS='--location "Seattle, WA, USA"'
```

Direct script:

```bash
PYTHONPATH=src .venv/bin/python src/features/scripts/features_api_cli.py get-job-skills --text "Python and Docker experience"
PYTHONPATH=src .venv/bin/python src/features/scripts/features_api_cli.py normalize-locations --location "Seattle, WA, USA"
```
