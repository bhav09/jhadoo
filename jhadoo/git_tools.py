"""Git repository analysis tools."""

import subprocess
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from pathlib import Path


class GitAnalyzer:
    """Analyzes Git repositories for cleanup opportunities."""

    def __init__(self, repo_path: str = "."):
        self.repo_path = repo_path
    
    def _run_git(self, args: List[str]) -> Tuple[bool, str]:
        """Run a git command safely."""
        try:
            result = subprocess.run(
                ['git'] + args,
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=False
            )
            return result.returncode == 0, result.stdout.strip()
        except FileNotFoundError:
            return False, "Git not found in PATH"
        except Exception as e:
            return False, str(e)

    def is_git_repo(self) -> bool:
        """Check if target is a valid git repository."""
        success, _ = self._run_git(['rev-parse', '--is-inside-work-tree'])
        return success

    def find_stale_branches(self, main_branch: str = "main") -> List[Dict[str, str]]:
        """Find local branches that have been merged into main/master."""
        # Try 'master' if 'main' doesn't exist?
        # For now, let's assume 'main' or allow user config, but we'll try to detect
        
        success, branches = self._run_git(['branch', '--merged', main_branch])
        if not success:
            # Fallback to master if main failed
            if main_branch == "main":
                main_branch = "master"
                success, branches = self._run_git(['branch', '--merged', main_branch])
        
        if not success:
            return []

        stale = []
        for line in branches.split('\n'):
            branch = line.strip()
            if branch and branch != '*' and branch != main_branch:
                # get last commit date
                success, date_str = self._run_git(['log', '-1', '--format=%cd', '--date=short', branch])
                if success:
                    stale.append({
                        "name": branch,
                        "merged_into": main_branch,
                        "last_commit": date_str
                    })
        return stale

    def find_large_files(self, size_mb: int = 50) -> List[Dict[str, Any]]:
        """Find large files in the repository history (pack bloat)."""
        # This is a bit complex in pure git, usually requires iterating objects.
        # simpler approach: scan current working tree for untracked/ignored large files 
        # OR scan loose objects.
        # Let's stick to working directory large files for now as "cleanup"
        # finding blobs in history is advanced.
        
        large_files = []
        limit_bytes = size_mb * 1024 * 1024
        
        for root, _, files in os.walk(self.repo_path):
            if '.git' in root:
                continue
                
            for file in files:
                try:
                    path = os.path.join(root, file)
                    size = os.path.getsize(path)
                    if size > limit_bytes:
                        large_files.append({
                            "path": path,
                            "size_mb": size / (1024 * 1024),
                            "rel_path": os.path.relpath(path, self.repo_path)
                        })
                except OSError:
                    pass
                    
        return sorted(large_files, key=lambda x: x['size_mb'], reverse=True)

    def check_health(self) -> Dict[str, Any]:
        """Check general repository health."""
        if not self.is_git_repo():
            return {"error": "Not a git repository"}
            
        health = {
            "stale_branches": self.find_stale_branches(),
            "large_files": self.find_large_files(),
            "git_dir_size_mb": 0.0
        }
        
        # Check .git size
        git_dir = os.path.join(self.repo_path, '.git')
        if os.path.exists(git_dir):
            total_size = 0
            for root, _, files in os.walk(git_dir):
                for f in files:
                    try:
                        total_size += os.path.getsize(os.path.join(root, f))
                    except: pass
            health["git_dir_size_mb"] = total_size / (1024 * 1024)
            
        return health
