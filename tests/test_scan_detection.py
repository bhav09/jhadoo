"""Tests for target scanning, disk guards, and QA helpers."""

import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from jhadoo.config import Config
from jhadoo.core import (
    CleanupEngine,
    _is_virtualenv,
    _is_js_dependency_tree,
)


class TestSignatureDetection(unittest.TestCase):

    def test_is_virtualenv_pyvenv_cfg(self):
        with tempfile.TemporaryDirectory() as tmp:
            venv = os.path.join(tmp, ".mysecret_env")
            os.makedirs(venv)
            with open(os.path.join(venv, "pyvenv.cfg"), "w", encoding="utf-8") as f:
                f.write("home = /usr/bin\n")
            self.assertTrue(_is_virtualenv(venv))

    def test_is_virtualenv_negative(self):
        with tempfile.TemporaryDirectory() as tmp:
            plain = os.path.join(tmp, "my_env")
            os.makedirs(plain)
            self.assertFalse(_is_virtualenv(plain))

    def test_is_js_dependency_tree_positive(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = os.path.join(tmp, "project")
            deps = os.path.join(project, "js_deps")
            os.makedirs(os.path.join(deps, ".bin"))
            with open(os.path.join(project, "package.json"), "w", encoding="utf-8") as f:
                f.write("{}")
            self.assertTrue(_is_js_dependency_tree(deps, project))

    def test_is_js_dependency_tree_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = os.path.join(tmp, "project")
            deps = os.path.join(project, "js_deps")
            os.makedirs(deps)
            with open(os.path.join(project, "package.json"), "w", encoding="utf-8") as f:
                f.write("{}")
            self.assertFalse(_is_js_dependency_tree(deps, project))

    def test_is_js_dependency_tree_no_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = os.path.join(tmp, "project")
            vendor = os.path.join(project, "vendor")
            os.makedirs(os.path.join(vendor, ".bin"))
            self.assertFalse(_is_js_dependency_tree(vendor, project))

    def test_is_js_dependency_tree_pnpm_scoped(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = os.path.join(tmp, "project")
            deps = os.path.join(project, "dependencies")
            os.makedirs(os.path.join(deps, "@babel", "core"))
            with open(os.path.join(project, "pnpm-lock.yaml"), "w", encoding="utf-8") as f:
                f.write("lockfileVersion: 5.4\n")
            self.assertTrue(_is_js_dependency_tree(deps, project))

    def test_is_js_dependency_tree_lockfile_only_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = os.path.join(tmp, "project")
            deps = os.path.join(project, "js_deps")
            os.makedirs(os.path.join(deps, ".bin"))
            with open(os.path.join(project, "package-lock.json"), "w", encoding="utf-8") as f:
                f.write("{}")
            self.assertTrue(_is_js_dependency_tree(deps, project))


class TestTargetScanning(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def _engine(self):
        config = Config()
        config.set("main_folder", self.root)
        return CleanupEngine(config, dry_run=True)

    def _targets(self):
        return [
            {"name": "venv", "days_threshold": 7, "enabled": True},
            {"name": "node_modules", "days_threshold": 14, "enabled": True},
        ]

    @patch("jhadoo.core.is_protected_path", return_value=False)
    @patch.object(CleanupEngine, "get_last_modified_time")
    def test_hidden_renamed_venv_detected(self, mock_mtime, _mock_protected):
        mock_mtime.return_value = datetime.now() - timedelta(days=30)
        venv_path = os.path.join(self.root, "project", ".mysecret_env")
        os.makedirs(venv_path)
        with open(os.path.join(venv_path, "pyvenv.cfg"), "w", encoding="utf-8") as f:
            f.write("home = /usr/bin\n")

        engine = self._engine()
        candidates = engine._scan_all_targets(self.root, self._targets())
        by_path = {c["path"]: c for c in candidates}
        self.assertIn(venv_path, by_path)
        self.assertEqual(by_path[venv_path]["target_name"], "venv")

    @patch("jhadoo.core.is_protected_path", return_value=False)
    @patch.object(CleanupEngine, "get_last_modified_time")
    def test_renamed_js_deps_detected(self, mock_mtime, _mock_protected):
        mock_mtime.return_value = datetime.now() - timedelta(days=30)
        project = os.path.join(self.root, "project")
        deps = os.path.join(project, "js_deps")
        os.makedirs(os.path.join(deps, ".bin"))
        with open(os.path.join(project, "package.json"), "w", encoding="utf-8") as f:
            f.write("{}")

        engine = self._engine()
        candidates = engine._scan_all_targets(self.root, self._targets())
        by_path = {c["path"]: c for c in candidates}
        self.assertIn(deps, by_path)
        self.assertEqual(by_path[deps]["target_name"], "node_modules")

    @patch("jhadoo.core.is_protected_path", return_value=False)
    @patch.object(CleanupEngine, "get_last_modified_time")
    def test_standard_node_modules_by_name(self, mock_mtime, _mock_protected):
        mock_mtime.return_value = datetime.now() - timedelta(days=30)
        project = os.path.join(self.root, "project")
        nm = os.path.join(project, "node_modules")
        os.makedirs(nm)

        engine = self._engine()
        candidates = engine._scan_all_targets(self.root, self._targets())
        paths = {c["path"] for c in candidates}
        self.assertIn(nm, paths)

    @patch("jhadoo.core.is_protected_path", return_value=False)
    @patch.object(CleanupEngine, "get_last_modified_time")
    def test_git_directory_not_detected_as_venv(self, mock_mtime, _mock_protected):
        mock_mtime.return_value = datetime.now() - timedelta(days=30)
        git_path = os.path.join(self.root, "project", ".git")
        os.makedirs(os.path.join(git_path, "objects"))

        engine = self._engine()
        candidates = engine._scan_all_targets(self.root, [{"name": "venv", "days_threshold": 7, "enabled": True}])
        paths = {c["path"] for c in candidates}
        self.assertNotIn(git_path, paths)

    @patch("jhadoo.core.is_protected_path", return_value=False)
    @patch.object(CleanupEngine, "get_last_modified_time")
    def test_renamed_js_deps_empty_not_detected(self, mock_mtime, _mock_protected):
        mock_mtime.return_value = datetime.now() - timedelta(days=30)
        project = os.path.join(self.root, "project")
        deps = os.path.join(project, "js_deps")
        os.makedirs(deps)
        with open(os.path.join(project, "package.json"), "w", encoding="utf-8") as f:
            f.write("{}")

        engine = self._engine()
        candidates = engine._scan_all_targets(self.root, self._targets())
        paths = {c["path"] for c in candidates}
        self.assertNotIn(deps, paths)


class TestArchiveDiskGuard(unittest.TestCase):

    @patch("jhadoo.core.validate_path_safety", return_value=(True, ""))
    @patch("shutil.disk_usage")
    def test_archive_skipped_when_disk_full(self, mock_disk_usage, _mock_safe):
        mock_disk_usage.return_value = MagicMock(free=100)

        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "target")
            os.makedirs(src)
            with open(os.path.join(src, "file.txt"), "w", encoding="utf-8") as f:
                f.write("x" * 1000)

            config = Config()
            archive_root = os.path.join(tmp, "archive")
            config.set("safety", {"archive_folder": archive_root})
            engine = CleanupEngine(config, dry_run=False, archive_mode=True)

            item = {"path": src, "size": 5000}
            with patch("jhadoo.core.logger.error") as mock_error:
                result = engine.delete_or_archive_item(item)
                self.assertFalse(result)
                mock_error.assert_called()
                self.assertTrue(os.path.exists(src))

    @patch("jhadoo.core.validate_path_safety", return_value=(True, ""))
    @patch("time.sleep")
    def test_test_delay_runs_before_archive(self, mock_sleep, _mock_safe):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "target")
            os.makedirs(src)

            config = Config()
            archive_root = os.path.join(tmp, "archive")
            config.set("safety", {"archive_folder": archive_root})

            engine = CleanupEngine(config, dry_run=False, archive_mode=True)
            item = {"path": src, "size": 1}

            with patch.dict(os.environ, {"JHADOO_TEST_DELAY": "2"}):
                with patch("shutil.move") as mock_move:
                    engine.delete_or_archive_item(item)
                    mock_sleep.assert_called_once_with(2.0)
                    mock_move.assert_called_once()

    @patch("jhadoo.core.validate_path_safety", return_value=(True, ""))
    @patch("shutil.disk_usage")
    def test_delete_unaffected_by_disk_check(self, mock_disk_usage, _mock_safe):
        mock_disk_usage.return_value = MagicMock(free=100)

        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "target")
            os.makedirs(src)

            config = Config()
            engine = CleanupEngine(config, dry_run=False, archive_mode=False)
            item = {"path": src, "size": 5000}

            with patch("shutil.rmtree") as mock_rmtree:
                result = engine.delete_or_archive_item(item)
                self.assertTrue(result)
                mock_rmtree.assert_called_once_with(src)
                mock_disk_usage.assert_not_called()


if __name__ == "__main__":
    unittest.main()
