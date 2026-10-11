"""
Unit test suite verifying that all Databricks notebooks and helper scripts
compile cleanly with zero Python syntax or indentation errors.
"""

import compileall
import os
import unittest

class TestNotebookSyntax(unittest.TestCase):

    def test_notebooks_compile(self):
        result = compileall.compile_dir("notebooks", quiet=1)
        self.assertTrue(result, "All notebooks in notebooks/ should compile successfully without syntax errors.")

    def test_scripts_compile(self):
        result = compileall.compile_dir("scripts", quiet=1)
        self.assertTrue(result, "All scripts in scripts/ should compile successfully without syntax errors.")

    def test_notebook_count(self):
        notebook_files = [f for f in os.listdir("notebooks") if f.endswith(".py")]
        self.assertGreaterEqual(len(notebook_files), 15, "Expected at least 15 pipeline notebooks in notebooks/ directory.")

if __name__ == "__main__":
    unittest.main()
