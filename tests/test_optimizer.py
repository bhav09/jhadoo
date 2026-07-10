"""Tests for SystemOptimizer."""

import unittest
from unittest.mock import patch, MagicMock
from jhadoo.optimizer import SystemOptimizer


class TestSystemOptimizer(unittest.TestCase):

    def setUp(self):
        self.optimizer = SystemOptimizer(dry_run=False)

    @patch('jhadoo.optimizer.get_system')
    @patch('subprocess.run')
    def test_flush_dns_macos_success(self, mock_run, mock_get_system):
        mock_get_system.return_value = "darwin"
        self.optimizer.system = "darwin"
        
        # Mock both subprocess.run calls to succeed
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        
        success, output = self.optimizer.flush_dns()
        
        self.assertTrue(success)
        self.assertIn("flushed successfully", output)
        self.assertEqual(mock_run.call_count, 2)
        self.assertIn("Flush DNS", self.optimizer.stats["tasks_completed"])

    @patch('jhadoo.optimizer.get_system')
    @patch('subprocess.run')
    def test_flush_dns_macos_partial_success(self, mock_run, mock_get_system):
        mock_get_system.return_value = "darwin"
        self.optimizer.system = "darwin"

        # Mock dscacheutil to succeed, killall to fail
        def run_side_effect(cmd, *args, **kwargs):
            if "dscacheutil" in cmd:
                return MagicMock(returncode=0, stdout="", stderr="")
            elif "killall" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="No matching processes belonging to you were found")
            return MagicMock(returncode=1, stdout="", stderr="")

        mock_run.side_effect = run_side_effect

        with patch('jhadoo.optimizer.logger.info') as mock_info:
            success, output = self.optimizer.flush_dns()

            self.assertTrue(success)
            self.assertIn("flushed partially", output)
            self.assertIn("mDNSResponder reload failed", output)
            self.assertEqual(mock_run.call_count, 2)
            self.assertIn("Flush DNS", self.optimizer.stats["tasks_completed"])

            # TS02_TC_07: partial-success must be logged with a ⚠️ prefix at
            # info level so users notice mDNSResponder reload did not complete.
            info_calls = [str(c[0][0]) for c in mock_info.call_args_list if c[0]]
            has_warn_icon = any("⚠️" in m and "flushed partially" in m for m in info_calls)
            self.assertTrue(
                has_warn_icon,
                f"Expected ⚠️ prefix on partial-success log, got: {info_calls}",
            )

    @patch('jhadoo.optimizer.get_system')
    @patch('subprocess.run')
    def test_flush_dns_macos_failure(self, mock_run, mock_get_system):
        mock_get_system.return_value = "darwin"
        self.optimizer.system = "darwin"
        
        # Mock both to fail
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Permission denied")
        
        success, output = self.optimizer.flush_dns()
        
        self.assertFalse(success)
        self.assertIn("Failed to flush macOS DNS", output)
        self.assertEqual(mock_run.call_count, 2)
        self.assertNotIn("Flush DNS", self.optimizer.stats["tasks_completed"])

    @patch('jhadoo.optimizer.get_system')
    @patch('jhadoo.optimizer.shutil.which')
    @patch('subprocess.run')
    def test_clean_package_cache_linux_permission_denied(self, mock_run, mock_which, mock_get_system):
        mock_get_system.return_value = "linux"
        self.optimizer.system = "linux"
        
        # Mock apt-get to exist
        def which_side_effect(cmd):
            if cmd == "apt-get":
                return "/usr/bin/apt-get"
            return None
        mock_which.side_effect = which_side_effect
        
        # Mock apt-get clean to fail with permission denied (returncode=1)
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Permission denied")
        
        with patch('jhadoo.optimizer.logger.info') as mock_info:
            self.optimizer.clean_package_cache()
            
            # Verify that permission denied warning/info was logged
            info_calls = [call[0][0] for call in mock_info.call_args_list if len(call[0]) > 0]
            has_permission_msg = any("Could not run apt-get" in str(msg) and "Permission denied" in str(msg) for msg in info_calls)
            self.assertTrue(has_permission_msg)

    @patch('jhadoo.optimizer.get_system')
    @patch('jhadoo.optimizer.shutil.which')
    @patch('subprocess.run')
    def test_clean_package_cache_linux_success(self, mock_run, mock_which, mock_get_system):
        mock_get_system.return_value = "linux"
        self.optimizer.system = "linux"
        
        # Mock apt-get to exist
        def which_side_effect(cmd):
            if cmd == "apt-get":
                return "/usr/bin/apt-get"
            return None
        mock_which.side_effect = which_side_effect
        
        # Mock apt-get clean to succeed
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        
        self.optimizer.clean_package_cache()
        self.assertIn("Clean Package Cache", self.optimizer.stats["tasks_completed"])

