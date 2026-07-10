"""System-wide optimization tools for macOS, Windows, and Linux."""

import os
import shutil
import subprocess
import logging
from typing import List, Dict, Any, Tuple
from pathlib import Path

from .utils.os_compat import get_system, get_home_directory, is_protected_path, normalize_path
from .utils.safety import bytes_to_human_readable

logger = logging.getLogger(__name__)


class SystemOptimizer:
    """Orchestrates system-wide optimizations on Mac, Windows, and Linux."""

    def __init__(self, dry_run: bool = False, archive_mode: bool = False):
        self.dry_run = dry_run
        self.archive_mode = archive_mode
        self.system = get_system()
        self.stats = {
            "bytes_freed": 0,
            "tasks_completed": []
        }

    def flush_dns(self) -> Tuple[bool, str]:
        """Flush the system DNS cache."""
        logger.info("\n🌐 Flushing DNS cache...")
        success = False
        output = ""

        if self.system == "darwin":  # macOS
            # Split commands to handle partial success (e.g. non-admin users)
            success_dscache = False
            success_killall = False
            errors = []

            # 1. dscacheutil
            try:
                res1 = subprocess.run(["dscacheutil", "-flushcache"], capture_output=True, text=True, check=False)
                if res1.returncode == 0:
                    success_dscache = True
                else:
                    errors.append(f"dscacheutil failed: {res1.stderr.strip() or res1.stdout.strip()}")
            except Exception as e:
                errors.append(f"dscacheutil error: {e}")

            # 2. killall
            try:
                res2 = subprocess.run(["killall", "-HUP", "mDNSResponder"], capture_output=True, text=True, check=False)
                if res2.returncode == 0:
                    success_killall = True
                else:
                    err_msg = res2.stderr.strip() or res2.stdout.strip()
                    if "No matching processes belonging to you were found" in err_msg or "Operation not permitted" in err_msg:
                        err_msg += " (mDNSResponder reload requires administrator privileges. Try running Jhadoo with sudo or ignore if DNS resolves fine)"
                    errors.append(f"killall failed: {err_msg}")
            except Exception as e:
                errors.append(f"killall error: {e}")

            if success_dscache and success_killall:
                success = True
                output = "macOS DNS cache flushed successfully."
            elif success_dscache:
                success = True  # Grade dscacheutil success as overall success with a note
                output = "flushed partially (dscacheutil succeeded, but mDNSResponder reload failed: process not owned by you or requires sudo). Run: sudo killall -HUP mDNSResponder"
            else:
                output = f"Failed to flush macOS DNS: {'; '.join(errors)}"

        elif self.system == "windows":  # Windows
            cmd = ["ipconfig", "/flushdns"]
            try:
                res = subprocess.run(cmd, capture_output=True, text=True, check=False)
                if res.returncode == 0:
                    success = True
                    output = "Windows DNS cache flushed successfully."
                else:
                    output = res.stderr or res.stdout
            except Exception as e:
                output = str(e)

        else:  # Linux
            # Try resolvectl first, then fallback to systemd-resolve
            resolved_cmds = [
                ["resolvectl", "flush-caches"],
                ["systemd-resolve", "--flush-caches"],
                ["/etc/init.d/dns-clean", "restart"]
            ]
            for cmd in resolved_cmds:
                try:
                    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
                    if res.returncode == 0:
                        success = True
                        output = f"Linux DNS cache flushed via {' '.join(cmd)}."
                        break
                except FileNotFoundError:
                    continue
                except Exception as e:
                    output = str(e)
            
            if not success:
                output = "No compatible DNS resolver (systemd-resolved) found to flush."

        if success:
            # Partial-success messages start with "flushed partially" — flag
            # them with a warning icon so the user notices that mDNSResponder
            # reload did not complete (TS02_TC_07), but keep them at info level
            # so the overall `success=True` contract is preserved.
            if output.startswith("flushed partially"):
                logger.info(f"   ⚠️  {output}")
            else:
                logger.info(f"   ✓ {output}")
            self.stats["tasks_completed"].append("Flush DNS")
        else:
            logger.warning(f"   ⚠️  Could not flush DNS: {output}")

        return success, output

    def refresh_workspace(self) -> Tuple[bool, str]:
        """Reload standard system desktop interfaces (Finder, Dock, Explorer, etc.)."""
        logger.info("\n🖥️  Refreshing workspace shell...")
        success = False
        output = ""

        if self.dry_run:
            logger.info("   [Dry Run] Would reload workspace shell.")
            return True, "Dry run"

        if self.system == "darwin":
            try:
                subprocess.run(["killall", "Finder"], check=False, capture_output=True)
                subprocess.run(["killall", "Dock"], check=False, capture_output=True)
                success = True
                output = "Finder and Dock restarted."
            except Exception as e:
                output = str(e)

        elif self.system == "windows":
            # Restart explorer.exe safely
            try:
                # Stop explorer
                subprocess.run(["taskkill", "/f", "/im", "explorer.exe"], capture_output=True, check=False)
                # Restart explorer asynchronously
                subprocess.Popen(["explorer.exe"], start_new_session=True)
                success = True
                output = "Windows Explorer restarted successfully."
            except Exception as e:
                output = f"Could not restart Explorer: {e}"

        else:  # Linux
            # Restarting X11/Wayland components can be intrusive, let's run lightweight cache refreshes
            cmds = [
                ["gsettings", "reset-recursively", "org.gnome.desktop"],  # GNOME refresh
                ["fc-cache", "-f"],  # Refresh system font cache
                ["update-mime-database", f"{get_home_directory()}/.local/share/mime"]  # Refresh local MIME associations
            ]
            success_count = 0
            for cmd in cmds:
                try:
                    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
                    if res.returncode == 0:
                        success_count += 1
                except FileNotFoundError:
                    continue
            
            if success_count > 0:
                success = True
                output = "Linux desktop caches and fonts updated."
            else:
                output = "No standard desktop components to refresh."

        if success:
            logger.info(f"   ✓ {output}")
            self.stats["tasks_completed"].append("Refresh Workspace")
        else:
            logger.warning(f"   ⚠️  Workspace refresh skipped or unsupported: {output}")

        return success, output

    def clean_temp_files(self) -> int:
        """Purge system temporary folders safely."""
        logger.info("\n🗑️  Cleaning system temporary directories...")
        bytes_freed = 0
        temp_dirs = []

        if self.system == "windows":
            # AppData Temp and System Temp
            appdata_temp = os.environ.get("TEMP") or os.environ.get("TMP")
            system_temp = os.path.join(os.environ.get("SystemRoot", "C:\\Windows"), "Temp")
            if appdata_temp and os.path.exists(appdata_temp):
                temp_dirs.append(appdata_temp)
            if os.path.exists(system_temp):
                temp_dirs.append(system_temp)

        elif self.system == "darwin":
            # macOS user temp files and system /private/tmp
            user_temp = os.environ.get("TMPDIR")
            if user_temp and os.path.exists(user_temp):
                temp_dirs.append(user_temp)
            if os.path.exists("/private/tmp"):
                temp_dirs.append("/private/tmp")

        else:  # Linux
            if os.path.exists("/tmp"):
                temp_dirs.append("/tmp")
            user_cache = os.path.join(get_home_directory(), ".cache")
            if os.path.exists(user_cache):
                # Clean specific, safe directories in .cache, e.g. thumbnail directories or pip logs
                safe_sub_caches = [
                    os.path.join(user_cache, "thumbnails"),
                    os.path.join(user_cache, "pip"),
                    os.path.join(user_cache, "yarn"),
                    os.path.join(user_cache, "mozilla")
                ]
                for sc in safe_sub_caches:
                    if os.path.exists(sc):
                        temp_dirs.append(sc)

        # Iterate and purge entries inside temp directories safely
        for d in temp_dirs:
            if is_protected_path(d) and d not in ["/tmp", "/private/tmp"]:
                continue
            
            logger.info(f"   Scanning: {d}")
            try:
                for entry in os.scandir(d):
                    entry_path = entry.path
                    
                    # Essential safety check
                    if is_protected_path(entry_path):
                        continue
                        
                    # Skip critical system locks or current user session files
                    if entry.name.startswith("."):
                        continue
                        
                    try:
                        # Compute size
                        size = 0
                        if entry.is_file(follow_symlinks=False):
                            size = entry.stat().st_size
                        elif entry.is_dir(follow_symlinks=False):
                            for root, _, files in os.walk(entry_path):
                                for f in files:
                                    try:
                                        p = os.path.join(root, f)
                                        size += os.path.getsize(p)
                                    except: pass
                        
                        if self.dry_run:
                            logger.info(f"      [Dry Run] Would delete: {entry_path} ({bytes_to_human_readable(size)})")
                            bytes_freed += size
                        else:
                            # Try to remove. If locked/active, fail gracefully
                            if entry.is_file(follow_symlinks=False) or os.path.islink(entry_path):
                                os.remove(entry_path)
                            elif entry.is_dir(follow_symlinks=False):
                                shutil.rmtree(entry_path)
                            bytes_freed += size
                            logger.info(f"      🗑️  Removed: {entry.name} ({bytes_to_human_readable(size)})")
                    except Exception as e:
                        # Silently skip locked/in-use files as is common for temp directories
                        logger.debug(f"Could not remove temporary file {entry_path}: {e}")
            except Exception as e:
                logger.warning(f"   ⚠️  Could not scan temp directory {d}: {e}")

        self.stats["bytes_freed"] += bytes_freed
        if bytes_freed > 0:
            logger.info(f"   ✓ Reclaimed {bytes_to_human_readable(bytes_freed)} from temporary folders.")
            self.stats["tasks_completed"].append("Clean Temp Files")
        else:
            logger.info("   ✓ Temporary folders are already clean.")

        return bytes_freed

    def clean_package_cache(self) -> int:
        """Recommend or run system package manager cache clear (Linux/macOS Homebrew)."""
        logger.info("\n📦 Refreshing system package manager caches...")
        bytes_freed = 0

        if self.dry_run:
            logger.info("   [Dry Run] Would refresh package caches.")
            return 0

        # macOS: Homebrew cache
        if self.system == "darwin":
            brew = shutil.which("brew")
            if brew:
                logger.info("   Running 'brew cleanup'...")
                try:
                    res = subprocess.run([brew, "cleanup", "-s"], capture_output=True, text=True, check=False)
                    if res.returncode == 0:
                        logger.info("   ✓ Homebrew caches cleaned successfully.")
                        self.stats["tasks_completed"].append("Clean Package Cache")
                except Exception as e:
                    logger.debug(f"Brew cleanup error: {e}")

        # Windows: Windows Update store warning or cleanup recommendation (requires admin)
        elif self.system == "windows":
            # We can recommend standard disk cleanup commands
            logger.info("   💡 To clean Windows Update caches, run: 'cleanmgr /sagerun:1' in an Elevated Command Prompt.")

        # Linux: APT / YUM / Pacman caches
        else:
            pms = {
                "apt-get": ["apt-get", "clean"],
                "yum": ["yum", "clean", "all"],
                "dnf": ["dnf", "clean", "all"],
                "pacman": ["pacman", "-Sc", "--noconfirm"]
            }
            cleaned = False
            for pm, args in pms.items():
                cmd_path = shutil.which(pm)
                if cmd_path:
                    logger.info(f"   Running package manager cleanup: {' '.join(args)} (may require sudo)...")
                    try:
                        # Run command. Since this might require root, let's capture but don't crash
                        res = subprocess.run(args, capture_output=True, text=True, check=False)
                        if res.returncode == 0:
                            logger.info(f"   ✓ {pm} package caches cleared.")
                            cleaned = True
                            break
                        else:
                            # Explain that sudo is required
                            logger.info(f"   ⚠️  Could not run {pm}: {res.stderr.strip() or 'permission denied (try running Jhadoo as root)'}")
                    except Exception as e:
                        logger.debug(f"PM cleanup error: {e}")
            
            if cleaned:
                self.stats["tasks_completed"].append("Clean Package Cache")

        return bytes_freed

    def run_all(self) -> Dict[str, Any]:
        """Run all enabled system-wide optimization operations."""
        logger.info(f"\n============================================================")
        logger.info(f"⚡ Running System Optimizations ({self.system.upper()})")
        logger.info(f"============================================================")

        self.flush_dns()
        self.clean_temp_files()
        self.clean_package_cache()
        self.refresh_workspace()

        logger.info(f"\n============================================================")
        logger.info(f"✅ System Optimizations Completed!")
        logger.info(f"   Tasks accomplished: {', '.join(self.stats['tasks_completed']) or 'None'}")
        if self.stats["bytes_freed"] > 0:
            logger.info(f"   Total space freed: {bytes_to_human_readable(self.stats['bytes_freed'])}")
        logger.info(f"============================================================\n")

        return self.stats
