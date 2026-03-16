"""FastAPI service for feature extraction endpoints."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

from fastapi import FastAPI

from features.job_skills import DEFAULT_SPACY_MODEL, SkillExtractor
from features.location_normalization import normalize_location
from features.schemas import (
    ExtractJobSkillsRequest,
    ExtractJobSkillsResponse,
    ExtractQueryEmbeddingRequest,
    ExtractQueryEmbeddingResponse,
    NormalizedLocation,
    NormalizeLocationsRequest,
    NormalizeLocationsResponse,
)

_LOGGER = logging.getLogger(__name__)

_DEFAULT_TECHNICAL_PATH = "/opt/jobsearch/src/features/data/technical_skills.csv"
_DEFAULT_KEYWORD_PATH = "/opt/jobsearch/src/features/data/tech_keywords.csv"
_DEFAULT_EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"


def _technical_filepath() -> str:
    return os.getenv("JOBSEARCH_FEATURES_TECHNICAL_PATH", _DEFAULT_TECHNICAL_PATH).strip() or _DEFAULT_TECHNICAL_PATH


def _keyword_filepath() -> str:
    return os.getenv("JOBSEARCH_FEATURES_KEYWORD_PATH", _DEFAULT_KEYWORD_PATH).strip() or _DEFAULT_KEYWORD_PATH


def _spacy_model() -> str:
    return os.getenv("JOBSEARCH_FEATURES_SPACY_MODEL", DEFAULT_SPACY_MODEL).strip() or DEFAULT_SPACY_MODEL


def _embedding_model_name() -> str:
    return os.getenv("JOBSEARCH_FEATURES_EMBEDDING_MODEL", _DEFAULT_EMBEDDING_MODEL).strip() or _DEFAULT_EMBEDDING_MODEL


@lru_cache(maxsize=1)
def _skill_extractor() -> SkillExtractor:
    return SkillExtractor(
        technical_filepath=_technical_filepath(),
        keyword_filepath=_keyword_filepath(),
        spacy_model=_spacy_model(),
    )


@lru_cache(maxsize=1)
def _text_embedding_class() -> Any:
    from fastembed import TextEmbedding

    return TextEmbedding


@lru_cache(maxsize=1)
def _embedding_model() -> Any:
    return _text_embedding_class()(model_name=_embedding_model_name())


def _extract_embedding(text: str) -> list[float]:
    embeddings = list(_embedding_model().embed([text]))
    if not embeddings:
        return []
    vector = embeddings[0]
    if hasattr(vector, "tolist"):
        raw_values = vector.tolist()
    else:
        raw_values = list(vector)
    return [float(value) for value in raw_values]


app = FastAPI(
    title="JobSearch Features API",
    version="1.0.0",
    description="Feature extraction service for job text.",
)


@app.on_event("startup")
async def startup_event() -> None:
    extractor = _skill_extractor()
    _embedding_model()
    _LOGGER.info(
        "[features] startup technical_path=%s keyword_path=%s skill_count=%s embedding_model=%s",
        extractor.technical_filepath,
        extractor.keyword_filepath,
        len(extractor.skills),
        _embedding_model_name(),
    )


@app.post("/job_skills", response_model=ExtractJobSkillsResponse)
def get_job_skills(payload: ExtractJobSkillsRequest) -> ExtractJobSkillsResponse:
    return ExtractJobSkillsResponse(
        status=200,
        error=None,
        skills=_skill_extractor().extract(payload.text),
        embedding=_extract_embedding(payload.text),
    )


@app.post("/query_embedding", response_model=ExtractQueryEmbeddingResponse)
def get_query_embedding(payload: ExtractQueryEmbeddingRequest) -> ExtractQueryEmbeddingResponse:
    return ExtractQueryEmbeddingResponse(
        status=200,
        error=None,
        embedding=_extract_embedding(payload.text),
    )


@app.post("/normalize_locations", response_model=NormalizeLocationsResponse)
def normalize_locations(payload: NormalizeLocationsRequest) -> NormalizeLocationsResponse:
    return NormalizeLocationsResponse(
        status=200,
        error=None,
        locations=[
            NormalizedLocation(city=city, region=state, country=country)
            for city, state, country in (normalize_location(raw) for raw in payload.locations)
        ],
    )
