"""Tests for CleanupEngine core logic."""

import unittest
from unittest.mock import patch, MagicMock
import logging
from jhadoo.config import Config
from jhadoo.core import CleanupEngine


class TestCleanupEngineCore(unittest.TestCase):

    @patch('shutil.which')
    def test_cleanup_docker_not_installed(self, mock_which):
        # Mock docker not installed
        mock_which.return_value = None
        
        config = Config()
        config.set("docker", {"enabled": True})
        engine = CleanupEngine(config, dry_run=True)
        
        with patch('jhadoo.core.logger.warning') as mock_warning:
            engine.cleanup_docker()
            mock_warning.assert_any_call("⚠️  Docker not installed — skipping image cleanup")

    @patch('shutil.which')
    def test_startup_banner_docker_skipped(self, mock_which):
        # Mock docker not installed
        mock_which.return_value = None
        
        config = Config()
        config.set("docker", {"enabled": True})
        engine = CleanupEngine(config, dry_run=True)
        
        with patch('jhadoo.core.logger.info') as mock_info:
            # We call run() but mock everything inside to avoid actual scans
            with patch.object(engine, 'cleanup_targets', return_value=0), \
                 patch.object(engine, 'clean_bin_folder', return_value=0), \
                 patch.object(engine, 'analyze_git_repositories'), \
                 patch.object(engine, 'cleanup_docker'), \
                 patch.object(config, 'ensure_directories'):
                engine.run()
                
                # Check that info was called with SKIPPED status
                banner_calls = [call[0][0] for call in mock_info.call_args_list if len(call[0]) > 0]
                has_skipped = any("Docker cleanup: SKIPPED (not installed)" in str(msg) for msg in banner_calls)
                self.assertTrue(has_skipped)

    @patch('jhadoo.core.confirm_deletion')
    def test_cleanup_cancelled(self, mock_confirm):
        # Mock user cancelling deletion
        mock_confirm.return_value = False
        
        config = Config()
        engine = CleanupEngine(config, dry_run=False)
        
        # Mock _scan_all_targets to return some candidates so confirmation is triggered
        with patch.object(engine, '_scan_all_targets', return_value=[{"path": "/some/path", "size": 1000 * 1024 * 1024}]), \
             patch('sys.stdin.isatty', return_value=True), \
             patch('jhadoo.core.logger.info') as mock_info:
            
            result = engine.run()
            
            self.assertTrue(result["success"])
            self.assertTrue(result.get("cancelled"))
            
            # Verify that early exit message was logged
            info_calls = [call[0][0] for call in mock_info.call_args_list if len(call[0]) > 0]
            has_cancelled_msg = any("❌ Cleanup cancelled by user" in str(msg) for msg in info_calls)
            self.assertTrue(has_cancelled_msg)

    @patch('jhadoo.core.as_completed')
    def test_cleanup_keyboard_interrupt(self, mock_as_completed):
        # Force KeyboardInterrupt when iterating futures
        mock_as_completed.side_effect = KeyboardInterrupt()
        
        config = Config()
        engine = CleanupEngine(config, dry_run=False)
        
        # Mock scan to return candidates
        with patch.object(engine, '_scan_all_targets', return_value=[{"path": "/some/path", "size": 1000 * 1024 * 1024}]), \
             patch('sys.stdin.isatty', return_value=True), \
             patch('jhadoo.core.confirm_deletion', return_value=True), \
             patch('jhadoo.core.logger.warning') as mock_warning, \
             patch.object(engine, 'save_deletion_manifest') as mock_save_manifest:
            
            result = engine.run()
            
            self.assertFalse(result["success"])
            self.assertTrue(result.get("interrupted"))
            
            # Verify that interrupt warning was logged
            warning_calls = [call[0][0] for call in mock_warning.call_args_list if len(call[0]) > 0]
            has_interrupt_msg = any("Interrupted by user" in str(msg) for msg in warning_calls)
            self.assertTrue(has_interrupt_msg)
            
            # Verify that manifest save was attempted
            mock_save_manifest.assert_called_once()


