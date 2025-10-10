"""OS compatibility utilities for cross-platform support."""

import os
import platform
from pathlib import Path
from typing import Set


def get_system() -> str:
    """Get the current operating system."""
    return platform.system().lower()


def normalize_path(path: str) -> str:
    """Normalize path for the current OS."""
    return str(Path(path).resolve())


def get_default_bin_folder() -> str:
    """Get the default bin/trash folder for the current OS."""
    system = get_system()
    
    if system == "darwin":  # macOS
        # Use custom bin instead of .Trash due to permission issues
        return str(Path.home() / "bin" / "jhadoo_trash")
    elif system == "windows":
        return str(Path.home() / "AppData" / "Local" / "jhadoo_trash")
    else:  # Linux and others
        return str(Path.home() / ".local" / "share" / "Trash" / "jhadoo")


def get_protected_paths() -> Set[str]:
    """Get system paths that should never be touched."""
    system = get_system()
    protected = set()
    
    if system == "darwin":  # macOS
        protected.update([
            "/System",
            "/Library",
            "/Applications",
            "/private",
            "/usr",
            "/bin",
            "/sbin",
            "/var",
            "/tmp"
        ])
    elif system == "windows":
        protected.update([
            "C:\\Windows",
            "C:\\Program Files",
            "C:\\Program Files (x86)",
            "C:\\ProgramData",
            "C:\\System Volume Information"
        ])
    else:  # Linux
        protected.update([
            "/bin",
            "/boot",
            "/dev",
            "/etc",
            "/lib",
            "/lib64",
            "/proc",
            "/root",
            "/run",
            "/sbin",
            "/sys",
            "/usr",
            "/var"
        ])
    
    return protected


def is_protected_path(path: str) -> bool:
    """Check if a path is in a protected system directory."""
    normalized = normalize_path(path)
    protected_paths = get_protected_paths()
    
    for protected in protected_paths:
        protected_norm = normalize_path(protected)
        if normalized.startswith(protected_norm):
            return True
    
    return False


def is_case_sensitive_fs() -> bool:
    """Check if the filesystem is case-sensitive."""
    system = get_system()
    
    # macOS and Windows are typically case-insensitive
    # Linux is typically case-sensitive
    if system in ["darwin", "windows"]:
        return False
    return True


def get_home_directory() -> str:
    """Get the user's home directory in a cross-platform way."""
    return str(Path.home())


