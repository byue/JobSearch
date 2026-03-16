import unittest

from common.job_taxonomy import (
    infer_job_category_from_title,
)


class JobTaxonomyTest(unittest.TestCase):
    def test_amazon_title_inference(self) -> None:
        self.assertEqual(
            infer_job_category_from_title(title="Machine Learning Engineer II"),
            "machine_learning_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Software Engineer"),
            "software_engineer",
        )

    def test_apple_title_inference(self) -> None:
        self.assertEqual(
            infer_job_category_from_title(title="Software Engineer, Siri"),
            "software_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Backend Engineer"),
            "software_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Machine Learning Engineer"),
            "machine_learning_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Data Scientist, Retail Analytics"),
            "data_scientist",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Data Engineer"),
            "software_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Research Scientist - Foundation Models"),
            "data_scientist",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Software Engineering Manager"),
            "manager",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Data Science Manager"),
            "manager",
        )
        self.assertEqual(infer_job_category_from_title(title="Senior Engineering Manager"), "manager")
        self.assertIsNone(infer_job_category_from_title(title="Product Manager"))
        self.assertIsNone(infer_job_category_from_title(title="Technical Program Manager"))
        self.assertIsNone(infer_job_category_from_title(title="Designer"))

    def test_google_title_inference(self) -> None:
        self.assertEqual(
            infer_job_category_from_title(title="Software Engineer, Infrastructure"),
            "software_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Full-Stack Engineer"),
            "software_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Machine Learning Engineer, YouTube"),
            "machine_learning_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Research Scientist, Gemini"),
            "data_scientist",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Data Scientist, Product"),
            "data_scientist",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Software Engineering Manager II"),
            "manager",
        )
        self.assertIsNone(infer_job_category_from_title(title="Program Manager"))
        self.assertIsNone(infer_job_category_from_title(title="Product Manager"))
        self.assertIsNone(infer_job_category_from_title(title="UX Designer"))

    def test_meta_title_inference(self) -> None:
        self.assertEqual(
            infer_job_category_from_title(title="Software Engineer, Product"),
            "software_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Machine Learning Engineer"),
            "machine_learning_engineer",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Applied Scientist"),
            "data_scientist",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Research Scientist, Generative AI"),
            "data_scientist",
        )
        self.assertEqual(
            infer_job_category_from_title(title="Software Engineering Manager"),
            "manager",
        )


if __name__ == "__main__":
    unittest.main()
