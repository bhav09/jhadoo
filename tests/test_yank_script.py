"""Tests for PyPI yank verification script."""

import json
import unittest
from io import StringIO
from unittest.mock import patch, MagicMock

import scripts.yank_old_pypi_versions as yank_script


class TestYankVerify(unittest.TestCase):

    @patch("scripts.yank_old_pypi_versions.get_pypi_releases")
    def test_verify_only_passes_when_old_versions_yanked(self, mock_releases):
        mock_releases.return_value = {
            "1.3.4": [{"yanked": False}],
            "1.3.3": [{"yanked": False}],
            "1.3.2": [{"yanked": False}],
            "1.2.0": [{"yanked": True}],
        }
        self.assertTrue(yank_script.verify_yank_state("jhadoo", keep=3))

    @patch("scripts.yank_old_pypi_versions.get_pypi_releases")
    def test_verify_only_fails_when_old_version_active(self, mock_releases):
        mock_releases.return_value = {
            "1.3.4": [{"yanked": False}],
            "1.3.3": [{"yanked": False}],
            "1.3.2": [{"yanked": False}],
            "1.2.0": [{"yanked": False}],
        }
        self.assertFalse(yank_script.verify_yank_state("jhadoo", keep=3))

    @patch("scripts.yank_old_pypi_versions.get_pypi_releases")
    @patch("scripts.yank_old_pypi_versions.yank_release", return_value=False)
    @patch("scripts.yank_old_pypi_versions.verify_yank_state", return_value=False)
    def test_main_exits_nonzero_when_yank_fails(self, mock_verify, mock_yank, mock_releases):
        mock_releases.return_value = {
            "1.3.4": [{"yanked": False}],
            "1.3.3": [{"yanked": False}],
            "1.3.2": [{"yanked": False}],
            "1.2.0": [{"yanked": False}],
        }
        with patch("sys.argv", ["yank", "--apply", "--token", "fake-token"]), \
             patch("sys.exit") as mock_exit:
            yank_script.main()
            mock_exit.assert_called_with(1)


if __name__ == "__main__":
    unittest.main()
