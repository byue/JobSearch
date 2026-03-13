"""Pydantic schemas for the features service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ExtractJobSkillsRequest(BaseModel):
    text: str = Field(..., min_length=1)

class ExtractJobSkillsResponse(BaseModel):
    status: int = 200
    error: str | None = None
    skills: list[str]
