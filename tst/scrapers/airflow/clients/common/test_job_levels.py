from __future__ import annotations

import unittest

from scrapers.airflow.clients.common.job_levels import (
    NORMALIZED_JOB_LEVEL_DISTINGUISHED,
    NORMALIZED_JOB_LEVEL_DIRECTOR,
    NORMALIZED_JOB_LEVEL_FELLOW,
    NORMALIZED_JOB_LEVEL_INTERN,
    NORMALIZED_JOB_LEVEL_JUNIOR,
    NORMALIZED_JOB_LEVEL_MID,
    NORMALIZED_JOB_LEVEL_PRINCIPAL,
    NORMALIZED_JOB_LEVEL_SENIOR,
    NORMALIZED_JOB_LEVEL_STAFF,
    get_normalized_job_level,
)


class JobLevelsTest(unittest.TestCase):
    def test_company_agnostic_levels(self) -> None:
        self.assertEqual(get_normalized_job_level("Software Engineering Intern"), NORMALIZED_JOB_LEVEL_INTERN)
        self.assertEqual(get_normalized_job_level("Junior Software Engineer"), NORMALIZED_JOB_LEVEL_JUNIOR)
        self.assertEqual(get_normalized_job_level("Mid-Level Software Engineer"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Senior Software Engineer"), NORMALIZED_JOB_LEVEL_SENIOR)
        self.assertEqual(get_normalized_job_level("Staff Machine Learning Engineer"), NORMALIZED_JOB_LEVEL_STAFF)
        self.assertEqual(get_normalized_job_level("Principal Engineer"), NORMALIZED_JOB_LEVEL_PRINCIPAL)
        self.assertEqual(get_normalized_job_level("Director, Engineering"), NORMALIZED_JOB_LEVEL_DIRECTOR)
        self.assertEqual(get_normalized_job_level("Distinguished Engineer"), NORMALIZED_JOB_LEVEL_DISTINGUISHED)
        self.assertEqual(get_normalized_job_level("Engineering Fellow"), NORMALIZED_JOB_LEVEL_FELLOW)
        self.assertEqual(get_normalized_job_level("Senior Staff Engineer"), NORMALIZED_JOB_LEVEL_STAFF)
        self.assertEqual(get_normalized_job_level("Senior Principal Engineer"), NORMALIZED_JOB_LEVEL_PRINCIPAL)
        self.assertEqual(get_normalized_job_level("Senior Director, Engineering"), NORMALIZED_JOB_LEVEL_DIRECTOR)
        self.assertEqual(get_normalized_job_level("Senior Distinguished Engineer"), NORMALIZED_JOB_LEVEL_DISTINGUISHED)
        self.assertEqual(get_normalized_job_level("Senior Fellow"), NORMALIZED_JOB_LEVEL_FELLOW)

    def test_company_specific_levels(self) -> None:
        self.assertEqual(get_normalized_job_level("Software Development Engineer I", "amazon"), NORMALIZED_JOB_LEVEL_JUNIOR)
        self.assertEqual(get_normalized_job_level("Software Development Engineer 1", "amazon"), NORMALIZED_JOB_LEVEL_JUNIOR)
        self.assertEqual(get_normalized_job_level("SDE II", "amazon"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Applied Scientist 2", "amazon"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Software Engineer III", "amazon"), NORMALIZED_JOB_LEVEL_SENIOR)
        self.assertEqual(get_normalized_job_level("System Development Engineer 3", "amazon"), NORMALIZED_JOB_LEVEL_SENIOR)
        self.assertEqual(get_normalized_job_level("Technical Program Manager III", "amazon"), NORMALIZED_JOB_LEVEL_SENIOR)
        self.assertEqual(get_normalized_job_level("Software Engineer II", "google"), NORMALIZED_JOB_LEVEL_JUNIOR)
        self.assertEqual(get_normalized_job_level("Software Engineer III", "google"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Software Engineer IV", "google"), NORMALIZED_JOB_LEVEL_SENIOR)
        self.assertEqual(get_normalized_job_level("SDE II", "google"), NORMALIZED_JOB_LEVEL_JUNIOR)
        self.assertEqual(get_normalized_job_level("Software Engineer - IC2", "microsoft"), NORMALIZED_JOB_LEVEL_JUNIOR)
        self.assertEqual(get_normalized_job_level("Software Engineer IC3", "microsoft"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Applied Scientist, IC4", "microsoft"), NORMALIZED_JOB_LEVEL_SENIOR)
        self.assertEqual(get_normalized_job_level("Principal Software Engineer IC5", "microsoft"), NORMALIZED_JOB_LEVEL_PRINCIPAL)
        self.assertEqual(get_normalized_job_level("Software Engineer IC6", "microsoft"), NORMALIZED_JOB_LEVEL_PRINCIPAL)
        self.assertEqual(get_normalized_job_level("Software Engineer 2", "microsoft"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Applied Scientist III", "microsoft"), NORMALIZED_JOB_LEVEL_SENIOR)
        self.assertEqual(get_normalized_job_level("Software Engineer IC2 II", "microsoft"), NORMALIZED_JOB_LEVEL_JUNIOR)
        self.assertEqual(get_normalized_job_level("SDE II"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Software Engineer II", "apple"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Software Engineer II", "meta"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Software Engineer L3", "netflix"), NORMALIZED_JOB_LEVEL_JUNIOR)
        self.assertEqual(get_normalized_job_level("Software Engineer L4", "netflix"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level("Senior Software Engineer (L5)", "netflix"), NORMALIZED_JOB_LEVEL_SENIOR)
        self.assertEqual(get_normalized_job_level("Software Engineer L4/L5", "netflix"), NORMALIZED_JOB_LEVEL_MID)

    def test_defaults_unmatched_titles_to_mid(self) -> None:
        self.assertEqual(get_normalized_job_level("Software Engineer"), NORMALIZED_JOB_LEVEL_MID)
        self.assertEqual(get_normalized_job_level(""), NORMALIZED_JOB_LEVEL_MID)


if __name__ == "__main__":
    unittest.main()
