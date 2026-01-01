"""Configuration management for jhadoo."""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from .utils.os_compat import get_default_bin_folder, get_home_directory


class Config:
    """Configuration manager for cleanup operations."""
    
    DEFAULT_CONFIG = {
        "main_folder": None,  # Will be set to user's home by default
        "bin_folder": None,  # Will be set based on OS
        "targets": [
            {
                "name": "venv",
                "days_threshold": 7,
                "enabled": True
            },
            {
                "name": "node_modules",
                "days_threshold": 14,
                "enabled": True
            },
            {
                "name": "__pycache__",
                "days_threshold": 30,
                "enabled": True
            },
            {
                "name": ".pytest_cache",
                "days_threshold": 30,
                "enabled": False
            },
            {
                "name": "build",
                "days_threshold": 14,
                "enabled": False
            },
            {
                "name": "dist",
                "days_threshold": 14,
                "enabled": False
            },
            {
                "name": ".tox",
                "days_threshold": 30,
                "enabled": False
            },
            {
                "name": "target",
                "days_threshold": 14,
                "enabled": False
            }
        ],
        "safety": {
            "size_threshold_mb": 5000,  # Warn if deleting more than 5GB
            "require_confirmation_above_mb": 500,  # Ask for confirmation above 500MB
            "dry_run": False,
            "backup_mode": False,
            "archive_folder": None  # Will be set based on home directory
        },
        "exclusions": [],  # List of paths to never touch
        "notifications": {
            "enabled": True,
            "on_completion": True,
            "on_error": True
        },
        "logging": {
            "log_file": None,  # Will be set to script directory
            "manifest_file": None,  # Will be set to script directory
            "level": "INFO"
        },
        "git": {
            "enabled": True,
            "check_stale_branches": True,
            "check_large_files": True,
            "large_file_threshold_mb": 50
        },
        "docker": {
            "enabled": True,
            "unused_image_days": 60
        }
    }
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration.
        
        Args:
            config_path: Path to custom config file (JSON)
        """
        self.config = self.DEFAULT_CONFIG.copy()
        self._set_defaults()
        
        if config_path and os.path.exists(config_path):
            self.load_from_file(config_path)
    
    def _set_defaults(self):
        """Set OS-specific defaults."""
        home = get_home_directory()
        
        if self.config["main_folder"] is None:
            self.config["main_folder"] = home
        
        if self.config["bin_folder"] is None:
            self.config["bin_folder"] = get_default_bin_folder()
        
        if self.config["safety"]["archive_folder"] is None:
            self.config["safety"]["archive_folder"] = os.path.join(home, ".jhadoo_archive")
        
        if self.config["logging"]["log_file"] is None:
            self.config["logging"]["log_file"] = os.path.join(home, ".jhadoo", "cleanup_log.csv")
        
        if self.config["logging"]["manifest_file"] is None:
            self.config["logging"]["manifest_file"] = os.path.join(home, ".jhadoo", "deletion_manifest.json")
    
    def load_from_file(self, config_path: str):
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                user_config = json.load(f)
            
            # Deep merge with defaults
            self._merge_config(user_config)
        except Exception as e:
            print(f"Warning: Could not load config from {config_path}: {e}")
            print("Using default configuration.")
    
    def _merge_config(self, user_config: Dict[str, Any]):
        """Merge user config with defaults."""
        for key, value in user_config.items():
            if key in self.config:
                if isinstance(value, dict) and isinstance(self.config[key], dict):
                    self.config[key].update(value)
                else:
                    self.config[key] = value
    
    def save_to_file(self, config_path: str):
        """Save current configuration to JSON file."""
        try:
            dir_path = os.path.dirname(config_path)
            if dir_path:  # Only create directory if path has a directory component
                os.makedirs(dir_path, exist_ok=True)
            with open(config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            print(f"Configuration saved to {config_path}")
        except Exception as e:
            print(f"Error saving config to {config_path}: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any):
        """Set configuration value."""
        self.config[key] = value
    
    def get_enabled_targets(self) -> List[Dict[str, Any]]:
        """Get list of enabled cleanup targets."""
        return [t for t in self.config.get("targets", []) if t.get("enabled", True)]
    
    def ensure_directories(self):
        """Ensure all necessary directories exist."""
        dirs_to_create = [
            self.config["bin_folder"],
            self.config["safety"]["archive_folder"],
            os.path.dirname(self.config["logging"]["log_file"]),
            os.path.dirname(self.config["logging"]["manifest_file"])
        ]
        
        for directory in dirs_to_create:
            if directory:
                os.makedirs(directory, exist_ok=True)