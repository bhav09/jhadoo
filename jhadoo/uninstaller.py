"""Smart application uninstaller and remnant sweeper for macOS, Windows, and Linux."""

import os
import shutil
import logging
import re
import subprocess
from typing import List, Dict, Any, Tuple, Set
from pathlib import Path

from .utils.os_compat import get_system, get_home_directory, is_protected_path, normalize_path
from .utils.safety import bytes_to_human_readable, confirm_deletion, log_user_cancelled

logger = logging.getLogger(__name__)


class AppUninstaller:
    """Discovers installed applications and sweeps leftover configurations, logs, and caches."""

    def __init__(self, config: Any, dry_run: bool = False, archive_mode: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.archive_mode = archive_mode
        self.system = get_system()
        self.deleted_items: List[Dict[str, Any]] = []

    def list_installed_apps(self) -> List[Dict[str, str]]:
        """List currently installed applications on the system."""
        apps = []

        if self.system == "darwin":  # macOS
            search_dirs = ["/Applications", os.path.join(get_home_directory(), "Applications")]
            for sdir in search_dirs:
                if not os.path.exists(sdir):
                    continue
                try:
                    for item in os.listdir(sdir):
                        if item.endswith(".app"):
                            app_path = os.path.join(sdir, item)
                            app_name = item[:-4]
                            bundle_id = self._get_macos_bundle_id(app_path)
                            apps.append({
                                "name": app_name,
                                "path": app_path,
                                "bundle_id": bundle_id or "",
                                "type": "App Bundle"
                            })
                except Exception as e:
                    logger.debug(f"Error scanning Mac apps inside {sdir}: {e}")

        elif self.system == "windows":  # Windows
            # Read Windows Registry for installed applications
            try:
                import winreg
            except ImportError:
                return []

            registry_paths = [
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall"),
                (winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Uninstall")
            ]

            seen = set()
            for hkey, subkey in registry_paths:
                try:
                    with winreg.OpenKey(hkey, subkey) as key:
                        for i in range(1024):
                            try:
                                name = winreg.EnumKey(key, i)
                                with winreg.OpenKey(key, name) as subkey_item:
                                    try:
                                        display_name, _ = winreg.QueryValueEx(subkey_item, "DisplayName")
                                        if display_name and display_name not in seen:
                                            seen.add(display_name)
                                            uninstall_string, _ = winreg.QueryValueEx(subkey_item, "UninstallString")
                                            install_location, _ = winreg.QueryValueEx(subkey_item, "InstallLocation")
                                            apps.append({
                                                "name": display_name,
                                                "uninstall_string": uninstall_string or "",
                                                "path": install_location or "",
                                                "type": "Registry Entry"
                                            })
                                    except OSError:
                                        pass
                            except OSError:
                                break
                except OSError:
                    pass

        else:  # Linux
            # Parse package managers or desktop configurations
            # Check dpkg-query first
            dpkg = shutil.which("dpkg-query")
            if dpkg:
                try:
                    res = subprocess.run([dpkg, "-W", "-f=${Package}|${Version}\n"], capture_output=True, text=True, check=False)
                    if res.returncode == 0:
                        for line in res.stdout.split("\n"):
                            if line:
                                parts = line.split("|")
                                if len(parts) >= 1:
                                    apps.append({
                                        "name": parts[0],
                                        "path": f"/usr/share/doc/{parts[0]}",
                                        "version": parts[1] if len(parts) > 1 else "",
                                        "type": "Debian Package"
                                    })
                except Exception:
                    pass

            # Fallback/complement with .desktop files
            desktop_dirs = ["/usr/share/applications", os.path.join(get_home_directory(), ".local/share/applications")]
            for ddir in desktop_dirs:
                if os.path.exists(ddir):
                    try:
                        for item in os.listdir(ddir):
                            if item.endswith(".desktop"):
                                fpath = os.path.join(ddir, item)
                                name = item[:-8]
                                try:
                                    with open(fpath, "r", encoding="utf-8", errors="ignore") as file:
                                        for line in file:
                                            if line.startswith("Name="):
                                                name = line.split("=")[1].strip()
                                                break
                                except: pass
                                if not any(a["name"] == name for a in apps):
                                    apps.append({
                                        "name": name,
                                        "path": fpath,
                                        "type": "Desktop Entry"
                                    })
                    except Exception:
                        pass

        return sorted(apps, key=lambda x: x["name"].lower())

    def _get_macos_bundle_id(self, app_path: str) -> str:
        """Extract Bundle Identifier from Info.plist of macOS .app bundles."""
        plist_path = os.path.join(app_path, "Contents", "Info.plist")
        if not os.path.exists(plist_path):
            return ""
        try:
            # Simple fallback using grep-like plist parsing to avoid full plistlib dependence issues
            with open(plist_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                # Find <key>CFBundleIdentifier</key> and match <string>value</string> right after
                match = re.search(r"<key>CFBundleIdentifier</key>\s*<string>([^<]+)</string>", content)
                if match:
                    return match.group(1).strip()
        except:
            pass
        return ""

    def find_app_remnants(self, app_name: str, bundle_id: str = "") -> List[Dict[str, Any]]:
        """Find leftover system files matching the app's name or bundle identifier securely."""
        logger.info(f"\n🔍 Searching for leftovers for application: {app_name}...")
        remnants = []
        home = get_home_directory()

        # Clean strings for strict matching
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "", app_name).lower()
        if not clean_name:
            return remnants

        # Match regex
        # Use word boundaries so we don't match partial structures incorrectly
        name_pattern = re.compile(rf"\b{re.escape(clean_name)}\b", re.IGNORECASE)

        # Platform specific scanning hubs
        scan_hubs = []
        if self.system == "darwin":  # macOS AppCleaner targets
            scan_hubs = [
                os.path.join(home, "Library/Application Support"),
                os.path.join(home, "Library/Caches"),
                os.path.join(home, "Library/Preferences"),
                os.path.join(home, "Library/Containers"),
                os.path.join(home, "Library/Saved Application State"),
                os.path.join(home, "Library/LaunchAgents"),
                "/Library/Application Support",
                "/Library/Caches",
                "/Library/Preferences",
                "/Library/LaunchAgents",
                "/Library/LaunchDaemons"
            ]
        elif self.system == "windows":  # Windows AppData
            appdata = os.environ.get("APPDATA")
            localappdata = os.environ.get("LOCALAPPDATA")
            programdata = os.environ.get("PROGRAMDATA", "C:\\ProgramData")
            if appdata: scan_hubs.append(appdata)
            if localappdata: scan_hubs.append(localappdata)
            if os.path.exists(programdata): scan_hubs.append(programdata)
        else:  # Linux
            scan_hubs = [
                os.path.join(home, ".config"),
                os.path.join(home, ".local/share"),
                os.path.join(home, ".cache")
            ]

        # Scan designated directories securely
        for hub in scan_hubs:
            if not os.path.exists(hub) or is_protected_path(hub):
                continue
            try:
                for entry in os.scandir(hub):
                    entry_path = entry.path
                    
                    if is_protected_path(entry_path):
                        continue

                    # Safety check: Avoid sweeping the root hub itself or dotfiles
                    if entry.name.startswith("."):
                        continue

                    # Match identifier patterns
                    matched = False
                    
                    # 1. Match plist / bundle ID for macOS
                    if bundle_id and bundle_id.lower() in entry.name.lower():
                        matched = True
                    
                    # 2. Check clean name match
                    if not matched:
                        # Full component check or word boundary check
                        if name_pattern.search(entry.name):
                            matched = True
                        elif clean_name in entry.name.lower():
                            # Strictly double check components to avoid over-greedy deletion
                            parts = re.split(r"[^a-zA-Z0-9]", entry.name.lower())
                            if clean_name in parts:
                                matched = True

                    if matched:
                        try:
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

                            remnant = {
                                "path": entry_path,
                                "name": entry.name,
                                "size": size,
                                "type": "folder" if entry.is_dir(follow_symlinks=False) else "file",
                                "source": os.path.basename(hub)
                            }
                            # Tag macOS-sandboxed remnants so clean_remnants can
                            # surface a clearer message when the kernel refuses
                            # the delete (TS07_TC_01). We check both the parent
                            # dir name (cheap, common case) and the full
                            # Library/Containers path segment (defensive).
                            if self.system == "darwin":
                                parent_name = os.path.basename(os.path.dirname(entry_path))
                                normalized = os.path.normpath(entry_path)
                                if parent_name == "Containers" or \
                                   f"{os.sep}Library{os.sep}Containers{os.sep}" in normalized + os.sep:
                                    remnant["protected"] = "macos-sandbox"
                            remnants.append(remnant)
                        except OSError:
                            pass
            except Exception as e:
                logger.debug(f"Could not scan remnants in {hub}: {e}")

        return sorted(remnants, key=lambda x: x["size"], reverse=True)

    def _compute_archive_path(self, archive_root: str, src_path: str) -> str:
        """Compute archive destination path securely."""
        from hashlib import sha1
        abs_path = os.path.abspath(src_path)
        drive, tail = os.path.splitdrive(abs_path)
        tail = tail.lstrip("\\/")
        prefix = sha1(abs_path.encode("utf-8")).hexdigest()[:8]
        return os.path.join(archive_root, "uninstall", prefix, tail)

    def clean_remnants(self, remnants: List[Dict[str, Any]]) -> int:
        """Delete or archive remnant folders."""
        if not remnants:
            return 0

        bytes_saved = 0
        successful = 0
        sandbox_blocked = 0
        archive_root = self.config.get("safety", {}).get("archive_folder")

        for item in remnants:
            path = item["path"]
            protected_reason = item.get("protected")
            try:
                if self.archive_mode:
                    os.makedirs(archive_root, exist_ok=True)
                    archive_path = self._compute_archive_path(archive_root, path)
                    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
                    shutil.move(path, archive_path)
                    item["archived_to"] = archive_path
                    logger.info(f"   📦 Archived leftover: {item['name']} → {archive_path}")
                else:
                    if item["type"] == "file":
                        os.remove(path)
                    else:
                        shutil.rmtree(path)
                    logger.info(f"   🗑️  Removed leftover: {item['name']} ({bytes_to_human_readable(item['size'])})")

                self.deleted_items.append(item)
                successful += 1
                bytes_saved += item["size"]
            except PermissionError as e:
                # macOS sandbox blocks deletion inside ~/Library/Containers
                # even when the path is owned by the user. Surface a clear,
                # actionable message instead of a bare exception traceback.
                sandbox_blocked += 1
                if protected_reason == "macos-sandbox":
                    logger.warning(
                        f"   🔒 macOS sandbox blocks deletion of {path}. "
                        f"Run Jhadoo with sudo, or remove manually: rm -rf \"{path}\""
                    )
                else:
                    logger.warning(
                        f"   🔒 Permission denied for {item['name']}: {e}. "
                        f"Run Jhadoo with sudo, or remove manually: rm -rf \"{path}\""
                    )
            except Exception as e:
                logger.error(f"   ❌ Failed to remove {item['name']}: {e}")

        summary = f"\n✓ Successfully removed {successful}/{len(remnants)} remnants ({bytes_to_human_readable(bytes_saved)} freed)."
        if sandbox_blocked:
            summary += f" {sandbox_blocked} blocked by macOS sandbox — see warnings above."
        logger.info(summary)
        return bytes_saved

    def run_cli_flow(self, query: str = ""):
        """Interactive CLI search and destroy uninstaller."""
        apps = self.list_installed_apps()
        if not apps:
            logger.info("\n✓ No installed applications detected to uninstall.")
            return

        selected_app = None

        if query:
            # Filter matches
            matches = [a for a in apps if query.lower() in a["name"].lower()]
            if not matches:
                logger.info(f"\n❌ No applications found matching: '{query}'")
                return
            elif len(matches) == 1:
                selected_app = matches[0]
            else:
                logger.info(f"\n📦 Found multiple applications matching '{query}':")
                for i, match in enumerate(matches):
                    logger.info(f"   {i+1:2d}. {match['name']} ({match['type']})")
                
                try:
                    ans = input("\nSelect application number to uninstall (or 'q' to quit): ").strip()
                    if ans.lower() == 'q':
                        return
                    idx = int(ans) - 1
                    if 0 <= idx < len(matches):
                        selected_app = matches[idx]
                except ValueError:
                    logger.info("❌ Invalid choice. Exiting.")
                    return
        else:
            # List first 30 and search
            logger.info(f"\n📦 Currently Installed Applications ({len(apps)} found):")
            for i, app in enumerate(apps[:30]):
                logger.info(f"   {i+1:3d}. {app['name']:50s} | Type: {app['type']}")
            if len(apps) > 30:
                logger.info(f"   ...and {len(apps)-30} more. Run with 'jhadoo --uninstall [APP_NAME]' to search.")

            try:
                ans = input("\nEnter name or number of application to uninstall (or 'q' to quit): ").strip()
                if ans.lower() == 'q' or not ans:
                    return
                
                if ans.isdigit():
                    idx = int(ans) - 1
                    if 0 <= idx < len(apps):
                        selected_app = apps[idx]
                else:
                    matches = [a for a in apps if ans.lower() in a["name"].lower()]
                    if len(matches) == 1:
                        selected_app = matches[0]
                    elif len(matches) > 1:
                        logger.info(f"\n📦 Multiple matches for '{ans}':")
                        for i, match in enumerate(matches):
                            logger.info(f"   {i+1:2d}. {match['name']} ({match['type']})")
                        choice = input("\nSelect application number: ").strip()
                        idx = int(choice) - 1
                        if 0 <= idx < len(matches):
                            selected_app = matches[idx]
                    else:
                        logger.info(f"❌ No applications match: '{ans}'")
            except Exception:
                logger.info("❌ Invalid input or error occurred.")
                return

        if not selected_app:
            logger.info("❌ No application selected.")
            return

        logger.info(f"\n🎯 Selected: {selected_app['name']}")
        
        # Windows Registry trigger string if applicable
        if self.system == "windows" and selected_app.get("uninstall_string"):
            logger.info(f"   Windows Uninstall Command: {selected_app['uninstall_string']}")
            if confirm_deletion(f"\nRun the vendor's standard uninstaller first?", default=True):
                try:
                    logger.info("   Starting uninstaller command...")
                    # Shell execute since registry uninstall strings have custom params
                    subprocess.run(selected_app["uninstall_string"], shell=True, check=False)
                except Exception as e:
                    logger.error(f"   Error running uninstaller: {e}")

        # Scan for deep remnants
        remnants = self.find_app_remnants(selected_app["name"], selected_app.get("bundle_id", ""))
        
        if not remnants:
            logger.info("\n✓ No leftover configuration or cache directories found.")
            return

        total_size = sum(item["size"] for item in remnants)
        logger.info(f"\n🔍 Found {len(remnants)} leftover files and cache folders ({bytes_to_human_readable(total_size)}):")
        for i, item in enumerate(remnants):
            lock_icon = " 🔒" if item.get("protected") == "macos-sandbox" else ""
            logger.info(f"   {i+1:2d}. {item['name']:40s} | {bytes_to_human_readable(item['size']):10s} | Hub: {item['source']}{lock_icon}")

        if self.dry_run:
            logger.info(f"\n[Dry Run] Total would delete: {bytes_to_human_readable(total_size)}")
            return

        if confirm_deletion(f"\n⚠️  About to {'archive' if self.archive_mode else 'delete'} {len(remnants)} leftovers ({bytes_to_human_readable(total_size)}). Continue?", default=False):
            bytes_saved = self.clean_remnants(remnants)
            
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
            logger.info(log_user_cancelled("App uninstall"))
            return 0
