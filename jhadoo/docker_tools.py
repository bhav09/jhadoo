"""Docker cleanup tools."""

import subprocess
import shutil
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple


class DockerCleaner:
    """Tools for cleaning up Docker resources."""

    def __init__(self):
        self.docker_cmd = shutil.which("docker")
        
    def is_docker_available(self) -> bool:
        return self.docker_cmd is not None

    def _run_docker(self, args: List[str]) -> Tuple[bool, str]:
        """Run docker command safely."""
        if not self.docker_cmd:
            return False, "Docker not installed"
            
        try:
            result = subprocess.run(
                [self.docker_cmd] + args,
                capture_output=True,
                text=True,
                check=False
            )
            return result.returncode == 0, result.stdout.strip()
        except Exception as e:
            return False, str(e)

    def find_unused_images(self, days_threshold: int = 60) -> List[Dict[str, Any]]:
        """Find images that haven't been used/created in X days."""
        # We'll use 'CreatedSince' as a proxy for age if 'LastUsed' isn't easily available
        # standard `docker images` gives creation date.
        
        # docker images --format "{{.ID}}|{{.Repository}}|{{.Tag}}|{{.CreatedAt}}"
        success, output = self._run_docker([
            'images', 
            '--format', '{{.ID}}|{{.Repository}}|{{.Tag}}|{{.CreatedAt}}'
        ])
        
        if not success:
            return []
            
        candidates = []
        cutoff = datetime.now() - timedelta(days=days_threshold)
        
        for line in output.split('\n'):
            if not line: continue
            try:
                parts = line.split('|')
                if len(parts) < 4: continue
                
                img_id, repo, tag, created_str = parts
                
                # Docker date format is messy: "2023-10-20 10:30:00 +0000 UTC"
                # Keep it simple, try parsing just the date part YYYY-MM-DD
                date_part = created_str.split()[0]
                created_date = datetime.strptime(date_part, "%Y-%m-%d")
                
                if created_date < cutoff:
                    candidates.append({
                        "id": img_id,
                        "repo": repo,
                        "tag": tag,
                        "created": created_str,
                        "age_days": (datetime.now() - created_date).days
                    })
            except Exception:
                continue
                
        return candidates

    def prune_images(self, images: List[Dict[str, Any]]) -> List[str]:
        """Remove specified images. Returns list of deleted IDs."""
        deleted = []
        for img in images:
            success, _ = self._run_docker(['rmi', img['id']])
            if success:
                deleted.append(img['id'])
        return deleted

    def system_prune(self) -> Tuple[bool, str]:
        """Run docker system prune -f (Aggressive!)."""
        # We might want to be careful here. Maybe just use this for manual recommendation?
        # Actually user asked for specific "Unused image for last 60 days" rule.
        # So we should stick to find_unused_images + prune_images
        return self._run_docker(['system', 'prune', '-f'])
