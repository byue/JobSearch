# Features Service

FastAPI service for feature extraction from job text.

The job skills extractor merges:
- custom technical skills
- newline-delimited tech keywords

## Endpoint

- `POST /job_skills`

Request body:

```json
{
  "text": "Experience with Python, Docker, and Kubernetes"
}
```

Response body:

```json
{
  "status": 200,
  "error": null,
  "skills": ["Python"]
}
```

## Environment

- `JOBSEARCH_FEATURES_TECHNICAL_PATH`
- `JOBSEARCH_FEATURES_KEYWORD_PATH`
- `JOBSEARCH_FEATURES_SPACY_MODEL`

## CLI

Test the live features API with:

```bash
PYTHONPATH=src python src/features/scripts/features_api_cli.py get-job-skills --text "Python and Docker experience"
```
