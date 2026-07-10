"""Tests for the AppUninstaller — macOS Containers remnant messaging (TS07_TC_01)."""

import os
import unittest
import tempfile
from unittest.mock import patch

from jhadoo.config import Config
from jhadoo.uninstaller import AppUninstaller


class TestUninstallerSandbox(unittest.TestCase):

    def _make_uninstaller(self, archive_mode=False):
        config = Config()
        config.set("safety", {"archive_folder": "/tmp/jhadoo-archive-test"})
        return AppUninstaller(config, dry_run=False, archive_mode=archive_mode)

    def test_find_remnants_tags_containers_as_protected(self):
        """A remnant inside ~/Library/Containers must be tagged with
        protected='macos-sandbox' so clean_remnants can surface a clearer
        message when the kernel refuses the delete."""
        uninstaller = self._make_uninstaller()
        uninstaller.system = "darwin"

        with tempfile.TemporaryDirectory() as tmp:
            # Build a fake home so we can place Library/Containers/<bundle>
            home = tmp
            containers = os.path.join(home, "Library", "Containers")
            os.makedirs(containers, exist_ok=True)
            target = os.path.join(containers, "com.example.someapp")
            os.makedirs(target)
            with open(os.path.join(target, "marker.txt"), "w") as f:
                f.write("x")

            with patch('jhadoo.uninstaller.get_home_directory', return_value=home), \
                 patch('jhadoo.uninstaller.is_protected_path', return_value=False):
                remnants = uninstaller.find_app_remnants("someapp", "com.example.someapp")

            self.assertGreaterEqual(len(remnants), 1)
            container_remnants = [r for r in remnants if "Containers" in r["path"]]
            self.assertTrue(container_remnants, "Expected at least one Containers remnant")
            self.assertEqual(container_remnants[0].get("protected"), "macos-sandbox")

    @patch('jhadoo.uninstaller.shutil.rmtree')
    def test_clean_remnants_surfaces_sandbox_message_on_permission_error(self, mock_rmtree):
        """When shutil.rmtree raises PermissionError on a tagged sandbox
        remnant, clean_remnants must log the macOS-sandbox message and
        continue processing other remnants."""
        uninstaller = self._make_uninstaller()
        uninstaller.system = "darwin"

        sandbox_item = {
            "path": "/Users/test/Library/Containers/com.example.someapp",
            "name": "com.example.someapp",
            "size": 100,
            "type": "folder",
            "source": "Containers",
            "protected": "macos-sandbox",
        }
        normal_item = {
            "path": "/Users/test/Library/Caches/com.example.someapp",
            "name": "com.example.someapp",
            "size": 50,
            "type": "folder",
            "source": "Caches",
        }

        # First rmtree call (sandbox) raises PermissionError; second succeeds
        mock_rmtree.side_effect = [PermissionError("Operation not permitted"), None]

        with patch('jhadoo.uninstaller.logger.warning') as mock_warning, \
             patch('jhadoo.uninstaller.logger.info') as mock_info:
            bytes_saved = uninstaller.clean_remnants([sandbox_item, normal_item])

            # The normal item was still processed → bytes_saved reflects it
            self.assertEqual(bytes_saved, 50)

            # A warning was emitted with the macOS-sandbox message
            warning_calls = [str(c[0][0]) for c in mock_warning.call_args_list if c[0]]
            self.assertTrue(
                any("macOS sandbox blocks deletion" in m for m in warning_calls),
                f"Expected sandbox warning, got: {warning_calls}",
            )
            # The summary mentioned blocked remnants
            info_calls = [str(c[0][0]) for c in mock_info.call_args_list if c[0]]
            self.assertTrue(
                any("blocked by macOS sandbox" in m for m in info_calls),
                f"Expected blocked-by-sandbox summary, got: {info_calls}",
            )

    @patch('jhadoo.uninstaller.shutil.rmtree')
    def test_clean_remnants_handles_non_sandbox_permission_error(self, mock_rmtree):
        """A PermissionError on a non-sandbox remnant should still produce
        the actionable 'Run Jhadoo with sudo' message."""
        uninstaller = self._make_uninstaller()
        uninstaller.system = "darwin"

        item = {
            "path": "/Users/test/Library/Application Support/someapp",
            "name": "someapp",
            "size": 100,
            "type": "folder",
            "source": "Application Support",
            # no "protected" key
        }
        mock_rmtree.side_effect = PermissionError("Operation not permitted")

        with patch('jhadoo.uninstaller.logger.warning') as mock_warning:
            uninstaller.clean_remnants([item])
            warning_calls = [str(c[0][0]) for c in mock_warning.call_args_list if c[0]]
            self.assertTrue(
                any("Permission denied" in m and "sudo" in m for m in warning_calls),
                f"Expected permission-denied sudo hint, got: {warning_calls}",
            )


if __name__ == "__main__":
    unittest.main()
