"""Installer and package setup sweeper for macOS, Windows, and Linux."""

import os
import shutil
import logging
from typing import List, Dict, Any
from pathlib import Path

from .utils.os_compat import get_system, get_home_directory, is_protected_path
from .utils.safety import bytes_to_human_readable, confirm_deletion, log_user_cancelled
from .restore import JobRestorer

logger = logging.getLogger(__name__)


class InstallerSweeper:
    """Finds and sweeps bulky, unused installation packages across common folders."""

    def __init__(self, config: Any, dry_run: bool = False, archive_mode: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.archive_mode = archive_mode
        self.system = get_system()
        self.deleted_items: List[Dict[str, Any]] = []

    def get_scan_directories(self) -> List[str]:
        """Get default paths to scan for installer files."""
        home = get_home_directory()
        dirs = [
            os.path.join(home, "Downloads"),
            os.path.join(home, "Desktop")
        ]
        
        # Add Homebrew cache on Mac
        if self.system == "darwin":
            brew_cache = os.path.join(home, "Library/Caches/Homebrew")
            if os.path.exists(brew_cache):
                dirs.append(brew_cache)
                
        # Filter directories that exist
        return [d for d in dirs if os.path.exists(d)]

    def find_installers(self) -> List[Dict[str, Any]]:
        """Scan directories for platform-specific installers."""
        scan_paths = self.get_scan_directories()
        logger.info(f"\n🔍 Scanning for installers in: {', '.join(scan_paths)}")
        
        candidates = []
        
        # Extensions & names to target
        if self.system == "darwin":
            targets = {".dmg", ".pkg"}
            name_keywords = set()
        elif self.system == "windows":
            targets = {".msi", ".exe"}
            name_keywords = {"setup", "install", "installer", "setup_"}
        else:  # Linux
            targets = {".deb", ".rpm", ".snap"}
            name_keywords = {"setup", "install", "installer"}

        for scan_dir in scan_paths:
            try:
                for root, _, files in os.walk(scan_dir):
                    # Skip VCS directories or caches
                    if any(p in root for p in [".git", "node_modules", "venv", ".venv"]):
                        continue
                        
                    for f in files:
                        file_path = os.path.join(root, f)
                        if is_protected_path(file_path):
                            continue
                            
                        _, ext = os.path.splitext(f)
                        ext = ext.lower()
                        
                        match = False
                        if ext in targets:
                            # If it's a Windows exe, only scan if it matches keywords (avoid normal executables)
                            if self.system == "windows" and ext == ".exe":
                                if any(kw in f.lower() for kw in name_keywords):
                                    match = True
                            else:
                                match = True
                                
                        # General installer archive match
                        if not match and ext == ".zip":
                            if any(kw in f.lower() for kw in ["setup", "installer", "install"]):
                                match = True
                                
                        if match:
                            try:
                                size = os.path.getsize(file_path)
                                # Only target installers larger than 5MB to avoid tiny tools
                                if size > 5 * 1024 * 1024:
                                    candidates.append({
                                        "path": file_path,
                                        "name": f,
                                        "size": size,
                                        "type": "file",
                                        "source": os.path.basename(scan_dir)
                                    })
                            except OSError:
                                pass
            except Exception as e:
                logger.debug(f"Could not scan directory {scan_dir}: {e}")
                
        return sorted(candidates, key=lambda x: x["size"], reverse=True)

    def _compute_archive_path(self, archive_root: str, src_path: str) -> str:
        """Compute archive path securely."""
        from hashlib import sha1
        abs_path = os.path.abspath(src_path)
        drive, tail = os.path.splitdrive(abs_path)
        tail = tail.lstrip("\\/")
        prefix = sha1(abs_path.encode("utf-8")).hexdigest()[:8]
        return os.path.join(archive_root, "installers", prefix, tail)

    def clean_installers(self, candidates: List[Dict[str, Any]]) -> int:
        """Archive or delete the chosen installers."""
        if not candidates:
            return 0
            
        total_size = sum(item["size"] for item in candidates)
        logger.info(f"\n🚀 Starting installer cleanup...")
        
        archive_root = self.config.get("safety", {}).get("archive_folder")
        successful = 0
        bytes_saved = 0
        
        for item in candidates:
            path = item["path"]
            try:
                if self.archive_mode:
                    os.makedirs(archive_root, exist_ok=True)
                    archive_path = self._compute_archive_path(archive_root, path)
                    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
                    shutil.move(path, archive_path)
                    item["archived_to"] = archive_path
                    logger.info(f"   📦 Archived: {item['name']} → {archive_path}")
                else:
                    os.remove(path)
                    logger.info(f"   🗑️  Deleted: {item['name']} ({bytes_to_human_readable(item['size'])})")
                
                self.deleted_items.append(item)
                successful += 1
                bytes_saved += item["size"]
            except Exception as e:
                logger.error(f"   ❌ Failed to process {item['name']}: {e}")
                
        logger.info(f"\n✓ Processed {successful}/{len(candidates)} installers ({bytes_to_human_readable(bytes_saved)} freed).")
        return bytes_saved

    def run(self) -> int:
        """Run standard scan and prompt flow."""
        candidates = self.find_installers()
        
        if not candidates:
            logger.info("\n✓ No large installer files found.")
            return 0
            
        total_size = sum(item["size"] for item in candidates)
        logger.info(f"\n📦 Found {len(candidates)} installers ({bytes_to_human_readable(total_size)}):")
        
        for i, item in enumerate(candidates[:15]):  # Show first 15
            logger.info(f"   {i+1:2d}. {item['name']:50s} | {bytes_to_human_readable(item['size']):10s} | Source: {item['source']}")
        if len(candidates) > 15:
            logger.info(f"   ...and {len(candidates)-15} more files.")
            
        if self.dry_run:
            logger.info(f"\n[Dry Run] Total would delete: {bytes_to_human_readable(total_size)}")
            return 0
            
        # Prompt user
        if confirm_deletion(f"\n⚠️  About to {'archive' if self.archive_mode else 'delete'} {len(candidates)} installers ({bytes_to_human_readable(total_size)}). Continue?", default=False):
            bytes_saved = self.clean_installers(candidates)
            
            # Save manifest if archived so they can undo!
            if self.deleted_items:
                try:
                    import json
                    from .utils.safety import create_deletion_manifest
                    manifest_file = self.config.get("logging", {}).get("manifest_file")
                    manifest = create_deletion_manifest(self.deleted_items)
                    os.makedirs(os.path.dirname(manifest_file), exist_ok=True)
                    with open(manifest_file, 'w') as f:
                        json.dump(manifest, f, indent=2)
                    logger.info(f"📝 Deletion manifest saved for restoration: {manifest_file}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not save manifest: {e}")
                    
            return bytes_saved
        else:
            logger.info(log_user_cancelled("Installers cleanup"))
            return 0
