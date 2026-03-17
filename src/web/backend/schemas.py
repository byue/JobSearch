"""Shared API schemas for unified job endpoints."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Location(BaseModel):
    """Structured location payload for job metadata."""

    model_config = ConfigDict(extra="forbid")

    country: str = ""
    state: str = ""
    city: str = ""


class GetJobsRequest(BaseModel):
    """Request payload for `/get_jobs`."""

    model_config = ConfigDict(extra="forbid")

    company: str | None = None
    query: str | None = None
    search_mode: str | None = None
    posted_within: str | None = None
    job_type: str | None = None
    job_level: str | None = None
    country: str | None = None
    region: str | None = None
    city: str | None = None
    pagination_index: int = Field(default=1, ge=1)


class JobMetadata(BaseModel):
    """Typed schema for one returned job/position."""

    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    runId: str | None = None
    name: str | None = None
    company: str | None = None
    jobCategory: str | None = None
    jobLevel: str | None = None
    locations: list[Location] = Field(default_factory=list)
    postedTs: int | None = None
    applyUrl: str | None = None
    detailsUrl: str | None = None


class PayRange(BaseModel):
    """Structured pay range extracted from a job posting."""

    model_config = ConfigDict(extra="forbid")

    minAmount: int | None = None
    maxAmount: int | None = None
    currency: str | None = None
    interval: str | None = None
    context: str | None = None


class PayDetails(BaseModel):
    """Normalized compensation details extracted from a job posting."""

    model_config = ConfigDict(extra="forbid")

    ranges: list[PayRange] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class JobDetailsSchema(BaseModel):
    """Typed schema for one detailed job payload."""

    model_config = ConfigDict(extra="ignore")

    id: str | None = None
    name: str | None = None
    company: str | None = None
    jobDescription: str | None = None
    postedTs: int | None = None
    applyUrl: str | None = None
    detailsUrl: str | None = None


class GetJobsResponse(BaseModel):
    """Response payload for `/get_jobs`."""

    model_config = ConfigDict(extra="allow")

    status: int | str | None = 200
    error: Any = None
    jobs: list[JobMetadata] = Field(default_factory=list)
    total_results: int | None = Field(default=None, ge=0)
    page_size: int | None = Field(default=None, ge=1)
    total_pages: int | None = Field(default=None, ge=1)
    pagination_index: int = Field(default=1, ge=1)
    has_next_page: bool = False


class GetCompaniesResponse(BaseModel):
    """Response payload for `/get_companies`."""

    model_config = ConfigDict(extra="allow")

    status: int | str | None = 200
    error: Any = None
    companies: list[str] = Field(default_factory=list)


class GetLocationFiltersResponse(BaseModel):
    """Response payload for `/get_location_filters`."""

    model_config = ConfigDict(extra="allow")

    status: int | str | None = 200
    error: Any = None
    countries: list[str] = Field(default_factory=list)
    regions: list[str] = Field(default_factory=list)
    cities: list[str] = Field(default_factory=list)


class GetJobDetailsRequest(BaseModel):
    """Request payload for `/get_job_details`."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=1)
    company: str = Field(min_length=1)
    runId: str | None = None


class GetJobDetailsResponse(BaseModel):
    """Response payload for `/get_job_details`."""

    model_config = ConfigDict(extra="allow")

    status: int | str | None = 200
    error: Any = None
    jobDescription: str | None = None
    skills: list[str] = Field(default_factory=list)
    postedTs: int | None = None
    detailsUrl: str | None = None
