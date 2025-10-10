"""Safety utilities for preventing accidental deletions."""

import os
from typing import List, Dict, Any
from pathlib import Path


def confirm_deletion(message: str, default: bool = False) -> bool:
    """Prompt user for confirmation."""
    valid = {"yes": True, "y": True, "no": False, "n": False}
    
    if default:
        prompt = " [Y/n] "
        default_response = True
    else:
        prompt = " [y/N] "
        default_response = False
    
    while True:
        print(message + prompt, end="")
        choice = input().lower().strip()
        
        if choice == "":
            return default_response
        elif choice in valid:
            return valid[choice]
        else:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').")


def bytes_to_human_readable(size_bytes: int) -> str:
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def check_size_threshold(total_size: int, threshold_mb: float) -> tuple[bool, str]:
    """Check if deletion size exceeds safety threshold.
    
    Returns:
        tuple: (exceeds_threshold, formatted_message)
    """
    threshold_bytes = threshold_mb * 1024 * 1024
    
    if total_size > threshold_bytes:
        human_size = bytes_to_human_readable(total_size)
        human_threshold = bytes_to_human_readable(threshold_bytes)
        message = f"⚠️  Warning: Total deletion size ({human_size}) exceeds threshold ({human_threshold})"
        return True, message
    
    return False, ""


def is_path_excluded(path: str, exclusion_list: List[str]) -> bool:
    """Check if a path matches any exclusion pattern."""
    normalized_path = os.path.normpath(path).lower()
    
    for excluded in exclusion_list:
        excluded_norm = os.path.normpath(excluded).lower()
        
        # Check exact match or if path is under excluded directory
        if normalized_path == excluded_norm or normalized_path.startswith(excluded_norm + os.sep):
            return True
    
    return False


def create_deletion_manifest(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create a manifest of items to be deleted for potential undo."""
    from datetime import datetime
    
    manifest = {
        "timestamp": datetime.now().isoformat(),
        "items": []
    }
    
    for item in items:
        manifest["items"].append({
            "path": item["path"],
            "size": item["size"],
            "last_modified": item.get("last_modified", ""),
            "type": item.get("type", "unknown"),
            "archived_to": item.get("archived_to", None)
        })
    
    return manifest


def validate_path_safety(path: str) -> tuple[bool, str]:
    """Validate that a path is safe to delete.
    
    Returns:
        tuple: (is_safe, error_message)
    """
    from .os_compat import is_protected_path
    
    # Check if path exists
    if not os.path.exists(path):
        return False, f"Path does not exist: {path}"
    
    # Check if it's a protected system path
    if is_protected_path(path):
        return False, f"Cannot delete protected system path: {path}"
    
    # Check if it's too close to root
    path_obj = Path(path)
    if len(path_obj.parts) <= 2:
        return False, f"Path too close to root directory: {path}"
    
    return True, ""


