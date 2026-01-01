"""Restore functionality for undoing cleanups."""

import os
import shutil
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
from .utils.safety import bytes_to_human_readable


class JobRestorer:
    """Handles restoration of deleted items from archive."""

    def __init__(self, config: Any):
        self.config = config
        self.manifest_path = config.get("logging", {}).get("manifest_file")
        self.archive_root = config.get("safety", {}).get("archive_folder")
    
    def load_manifest(self) -> Optional[Dict[str, Any]]:
        """Load the most recent deletion manifest."""
        if not self.manifest_path or not os.path.exists(self.manifest_path):
            return None
            
        try:
            with open(self.manifest_path, 'r') as f:
                return json.load(f)
        except Exception:
            return None

    def list_restorable_items(self) -> List[Dict[str, Any]]:
        """List items that can be restored from the last run."""
        manifest = self.load_manifest()
        if not manifest:
            return []
            
        restorable = []
        for item in manifest.get("items", []):
            archive_path = item.get("archived_to")
            if archive_path and os.path.exists(archive_path):
                restorable.append(item)
                
        return restorable

    def restore_item(self, item: Dict[str, Any]) -> bool:
        """Restore a single item from archive to original location."""
        archive_path = item.get("archived_to")
        original_path = item.get("path")
        
        if not archive_path or not os.path.exists(archive_path):
            return False
            
        try:
            # Ensure parent dict exists
            os.makedirs(os.path.dirname(original_path), exist_ok=True)
            
            # If original location exists, move it out of the way?
            # Or fail? Safe bet is to rename it if it conflicts.
            if os.path.exists(original_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = f"{original_path}.conflict_{timestamp}"
                shutil.move(original_path, backup_name)
                print(f"⚠️  Existing path found, moved to: {backup_name}")
            
            shutil.move(archive_path, original_path)
            return True
        except Exception as e:
            print(f"❌ Failed to restore {original_path}: {e}")
            return False

    def restore_all(self) -> int:
        """Restore all items from the last run."""
        items = self.list_restorable_items()
        if not items:
            print("No restorable items found (did you use --archive mode previously?)")
            return 0
            
        print(f"\n🔄 Restoring {len(items)} items...")
        restored_count = 0
        
        for item in items:
            print(f"   Restoring: {item['path']}...")
            if self.restore_item(item):
                restored_count += 1
        
        return restored_count
