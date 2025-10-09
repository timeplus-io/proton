import make_release
import unittest
import textwrap
import click


class TestMakeRelease(unittest.TestCase):
    def test_generate_release_notes(self):
        # Test can generate.
        changelog = textwrap.dedent("""
        # Changelog

        All notable changes to this project will be documented in this file.

        The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
        and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

        ## 3.9.0

        ### Added

        - Add CMake option `MONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX` (default is `TRUE`
          for backwards-compatibility).
        - Add API to manage Atlas Search Indexes.
                                    
        ## 3.8.0

        ### Fixed

        - Fix foo.
        """).lstrip()
        expected_release_notes = textwrap.dedent("""
        ## Added

        - Add CMake option `MONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX` (default is `TRUE`
          for backwards-compatibility).
        - Add API to manage Atlas Search Indexes.

        See the [full list of changes in Jira](https://jira.mongodb.org/issues/?jql=project%20%3D%20CXX%20AND%20fixVersion%20%3D%203.9.0).

        ## Feedback
        To report a bug or request a feature, please open a ticket in the MongoDB issue management tool Jira:

        - [Create an account](https://jira.mongodb.org) and login.
        - Navigate to the [CXX project](https://jira.mongodb.org/browse/CXX)
        - Click `Create`.
        """).lstrip()

        got = make_release.generate_release_notes("3.9.0", changelog)
        self.assertEqual(got, expected_release_notes)

        # Test exception occurs if CHANGELOG includes extra characters in title.
        with self.assertRaises(click.ClickException) as ctx:
            make_release.generate_release_notes(
                "3.9.0", "## 3.9.0 [Unreleased]")
        self.assertIn("Unexpected extra characters",
                      str(ctx.exception.message))

        # Test exception occurs if CHANGELOG does not include matching entry.
        with self.assertRaises(click.ClickException) as ctx:
            make_release.generate_release_notes(
                "3.9.0", "## 3.8.0")
        self.assertIn("Failed to find", str(ctx.exception.message))


if __name__ == "__main__":
    unittest.main()
