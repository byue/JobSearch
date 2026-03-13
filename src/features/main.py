"""FastAPI service for feature extraction endpoints."""

from __future__ import annotations

import logging
import os
from functools import lru_cache

from fastapi import FastAPI

from features.job_skills import DEFAULT_SPACY_MODEL, SkillExtractor
from features.schemas import ExtractJobSkillsRequest, ExtractJobSkillsResponse

_LOGGER = logging.getLogger(__name__)

_DEFAULT_TECHNICAL_PATH = "/opt/jobsearch/src/features/data/technical_skills.csv"
_DEFAULT_KEYWORD_PATH = "/opt/jobsearch/src/features/data/tech_keywords.csv"


def _technical_filepath() -> str:
    return os.getenv("JOBSEARCH_FEATURES_TECHNICAL_PATH", _DEFAULT_TECHNICAL_PATH).strip() or _DEFAULT_TECHNICAL_PATH


def _keyword_filepath() -> str:
    return os.getenv("JOBSEARCH_FEATURES_KEYWORD_PATH", _DEFAULT_KEYWORD_PATH).strip() or _DEFAULT_KEYWORD_PATH


def _spacy_model() -> str:
    return os.getenv("JOBSEARCH_FEATURES_SPACY_MODEL", DEFAULT_SPACY_MODEL).strip() or DEFAULT_SPACY_MODEL


@lru_cache(maxsize=1)
def _skill_extractor() -> SkillExtractor:
    return SkillExtractor(
        technical_filepath=_technical_filepath(),
        keyword_filepath=_keyword_filepath(),
        spacy_model=_spacy_model(),
    )


app = FastAPI(
    title="JobSearch Features API",
    version="1.0.0",
    description="Feature extraction service for job text.",
)


@app.on_event("startup")
async def startup_event() -> None:
    extractor = _skill_extractor()
    _LOGGER.info(
        "[features] startup technical_path=%s keyword_path=%s skill_count=%s",
        extractor.technical_filepath,
        extractor.keyword_filepath,
        len(extractor.skills),
    )


@app.post("/job_skills", response_model=ExtractJobSkillsResponse)
def get_job_skills(payload: ExtractJobSkillsRequest) -> ExtractJobSkillsResponse:
    return ExtractJobSkillsResponse(
        status=200,
        error=None,
        skills=_skill_extractor().extract(payload.text),
    )
