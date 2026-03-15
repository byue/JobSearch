import unittest

from pydantic import ValidationError

from web.backend.schemas import (
    GetCompaniesResponse,
    GetJobDetailsRequest,
    GetJobDetailsResponse,
    GetJobsRequest,
    GetJobsResponse,
    JobDetailsSchema,
    JobMetadata,
    Location,
)


class SchemasTest(unittest.TestCase):
    def test_location_forbids_extra(self) -> None:
        with self.assertRaises(ValidationError):
            Location(country="US", state="CA", city="SF", extra="x")

    def test_get_jobs_request_validation(self) -> None:
        self.assertEqual(GetJobsRequest(company="amazon").pagination_index, 1)
        with self.assertRaises(ValidationError):
            GetJobsRequest(company="amazon", pagination_index=0)
        self.assertEqual(GetJobsRequest(company=None, query="python").query, "python")

    def test_job_metadata_and_details_schema(self) -> None:
        location = Location(country="US", state="CA", city="SF")
        metadata = JobMetadata(
            id="1",
            runId="run-1",
            name="Role",
            company="amazon",
            locations=[location],
            postedTs=123,
            applyUrl="https://apply",
            detailsUrl="https://details",
        )
        self.assertEqual(metadata.locations[0].country, "US")
        self.assertEqual(metadata.runId, "run-1")

        details = JobDetailsSchema(
            jobDescription="desc",
        )
        self.assertEqual(details.jobDescription, "desc")

        details_with_extra = JobDetailsSchema(
            jobDescription="desc",
            minimumQualifications=["ignored"],
        )
        self.assertEqual(details_with_extra.jobDescription, "desc")

    def test_response_models(self) -> None:
        jobs_response = GetJobsResponse(
            status=200,
            jobs=[JobMetadata(id="1")],
            total_results=1,
            page_size=25,
            total_pages=1,
            has_next_page=False,
        )
        self.assertEqual(jobs_response.status, 200)
        self.assertEqual(len(jobs_response.jobs), 1)
        self.assertEqual(jobs_response.page_size, 25)
        self.assertEqual(jobs_response.total_pages, 1)

        companies_response = GetCompaniesResponse(status=200, companies=["amazon", "apple"])
        self.assertEqual(companies_response.companies[0], "amazon")

        details_response = GetJobDetailsResponse(
            status=200,
            jobDescription="x",
            skills=["Python"],
            postedTs=123,
            detailsUrl="https://details",
        )
        self.assertEqual(details_response.jobDescription, "x")
        self.assertEqual(details_response.skills, ["Python"])
        self.assertEqual(details_response.postedTs, 123)
        self.assertEqual(details_response.detailsUrl, "https://details")

    def test_get_job_details_request_validation(self) -> None:
        self.assertEqual(GetJobDetailsRequest(job_id="1", company="amazon").company, "amazon")
        with self.assertRaises(ValidationError):
            GetJobDetailsRequest(job_id="", company="amazon")
        with self.assertRaises(ValidationError):
            GetJobDetailsRequest(job_id="1", company="")
        self.assertEqual(GetJobDetailsRequest(job_id="1", company="amazon", runId="run-1").runId, "run-1")


if __name__ == "__main__":
    unittest.main()
