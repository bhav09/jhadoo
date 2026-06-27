"""Tests for PyPI yank verification script."""

import unittest
import urllib.error
from unittest.mock import patch

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

    def test_yank_release_returns_false_on_api_404(self):
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = urllib.error.HTTPError(
                url="https://pypi.org/api/projects/jhadoo/1.2.0",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            )
            result = yank_script.yank_release(
                "jhadoo", "1.2.0", "fake-token", "test reason", dry_run=False
            )
            self.assertFalse(result)

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
