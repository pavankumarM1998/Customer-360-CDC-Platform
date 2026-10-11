"""
Unit tests validating that all notebooks in notebooks/ and scripts in scripts/
are present and structured cleanly for Databricks.
"""

import os
import unittest
import compileall

class TestProjectStructure(unittest.TestCase):

    def test_scripts_compile(self):
        result = compileall.compile_dir("scripts", quiet=1)
        self.assertTrue(result, "All scripts in scripts/ should compile successfully without syntax errors.")

    def test_notebooks_present(self):
        self.assertTrue(os.path.exists("notebooks"), "notebooks/ directory should exist.")
        notebook_files = [f for f in os.listdir("notebooks") if f.endswith(".ipynb")]
        self.assertGreaterEqual(len(notebook_files), 50, "Expected at least 50 historical notebooks in notebooks/.")

    def test_resources_present(self):
        self.assertTrue(os.path.exists("resources"), "resources/ directory should exist.")
        job_files = [f for f in os.listdir("resources") if f.endswith(".job.yml")]
        self.assertGreaterEqual(len(job_files), 6, "Expected at least 6 workflow job definitions in resources/.")

if __name__ == "__main__":
    unittest.main()
