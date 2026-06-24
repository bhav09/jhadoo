"""Tests for Config validation and loading."""

import os
import tempfile
import pytest
from jhadoo.config import Config, ConfigLoadError


def test_config_valid_none():
    """Test that Config(None) loads default configuration successfully."""
    config = Config(None)
    assert config is not None
    assert config.get("main_folder") is not None


def test_config_empty_string():
    """Test that Config("") raises ConfigLoadError."""
    with pytest.raises(ConfigLoadError) as exc_info:
        Config("")
    assert "cannot be empty" in str(exc_info.value)


def test_config_nonexistent_file():
    """Test that Config("nonexistent.json") raises ConfigLoadError."""
    with pytest.raises(ConfigLoadError) as exc_info:
        Config("nonexistent_file_path_12345.json")
    assert "file not found" in str(exc_info.value).lower()


def test_config_invalid_json():
    """Test that Config with an invalid JSON file raises ConfigLoadError."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write("{invalid json")
        temp_path = f.name

    try:
        with pytest.raises(ConfigLoadError) as exc_info:
            Config(temp_path)
        assert "Invalid JSON" in str(exc_info.value)
    finally:
        os.remove(temp_path)


def test_config_valid_json():
    """Test that Config with a valid JSON file loads and merges successfully."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write('{"main_folder": "/tmp/test_jhadoo"}')
        temp_path = f.name

    try:
        config = Config(temp_path)
        assert config.get("main_folder") == "/tmp/test_jhadoo"
    finally:
        os.remove(temp_path)
