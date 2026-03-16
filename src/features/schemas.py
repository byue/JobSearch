"""Pydantic schemas for the features service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ExtractJobSkillsRequest(BaseModel):
    text: str = Field(..., min_length=1)


class ExtractQueryEmbeddingRequest(BaseModel):
    text: str = Field(..., min_length=1)


class ExtractJobSkillsResponse(BaseModel):
    status: int = 200
    error: str | None = None
    skills: list[str]
    embedding: list[float] = Field(default_factory=list)


class ExtractQueryEmbeddingResponse(BaseModel):
    status: int = 200
    error: str | None = None
    embedding: list[float] = Field(default_factory=list)


class NormalizeLocationsRequest(BaseModel):
    locations: list[str] = Field(default_factory=list, max_length=100)


class NormalizedLocation(BaseModel):
    city: str | None = None
    region: str | None = None
    country: str | None = None


class NormalizeLocationsResponse(BaseModel):
    status: int = 200
    error: str | None = None
    locations: list[NormalizedLocation] = Field(default_factory=list)
