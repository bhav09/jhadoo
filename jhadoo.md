# jhadoo - Smart Cleanup Tool

## Project Overview
jhadoo (Hindi for "jhadoo") is a universal Python package for automated cleanup of development environments. It intelligently finds and removes **any unused files or folders** (venv, node_modules, build, dist, cache, etc.), cleans trash folders, and maintains detailed logs of space savings. **Completely folder-agnostic** - works with any folder name you specify!

## Package Structure
```
jhadoo/
├── jhadoo/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py
│   ├── core.py
│   ├── config.py
│   ├── notifications.py
│   └── utils/
│       ├── __init__.py
│       ├── os_compat.py
│       ├── progress.py
│       └── safety.py
├── tests/
├── setup.py
├── setup.cfg
├── pyproject.toml
├── README.md
├── LICENSE
├── MANIFEST.in
└── requirements.txt
```

## Features

### Core Functionality
- ✅ Scan directories for **ANY** target files/folders (works with any name!)
- ✅ Delete based on age threshold (configurable per target)
- ✅ Clean bin/trash folders
- ✅ CSV logging with cumulative totals
- ✅ **Completely file/folder agnostic** - not limited to specific types!

### Safety Features
- ✅ Dry-run mode: Preview deletions before executing
- ✅ Size safety cap: Warn if total deletion exceeds threshold (default: 5GB)
- ✅ Confirmation prompts: Interactive approval for large deletions (>500MB)
- ✅ Backup option: Archive to folder instead of permanent deletion
- ✅ Exclusion list: Whitelist critical folders
- ✅ System protection: Never touches OS system directories
- ✅ Path validation: Multiple safety checks before deletion

### OS Compatibility
- ✅ Automatic OS detection (Windows/macOS/Linux)
- ✅ Path normalization (handles backslashes vs forward slashes)
- ✅ System folder protection (platform-specific protected paths)
- ✅ Case sensitivity handling
- ✅ Default paths for each platform

### User Experience
- ✅ Progress indicators: Real-time progress bars for large scans
- ✅ Desktop notifications: Alerts on completion (platform-specific)
- ✅ Summary dashboard: Trends, statistics, and predictions
- ✅ Undo capability: Deletion manifest for recovery
- ✅ Human-readable output: Emoji-enhanced, clear messages
- ✅ Config generation: Easy setup with sample configuration

## Configuration
Default configuration supports:
- Multiple target folder types
- Per-target age thresholds
- Size limits and warnings
- Archive vs permanent deletion

## API Endpoints
N/A (CLI tool)

## Database Schema
N/A (CSV-based logging)

### Log File Schema (cleanup_log.csv)
- `datetime`: Timestamp of cleanup run
- `folders_deleted_mb`: Space freed from target folders
- `bin_deleted_mb`: Space freed from bin/trash
- `total_deleted_mb`: Total space freed this run
- `cumulative_folders_mb`: All-time total from folders
- `cumulative_bin_mb`: All-time total from bin
- `cumulative_total_mb`: All-time total space freed

### Deletion Manifest Schema (deletion_manifest.json)
- `timestamp`: When deletion occurred
- `items`: List of deleted items with metadata
  - `path`: Full path to deleted item
  - `size`: Size in bytes
  - `last_modified`: Last modification timestamp
  - `type`: 'folder' or 'file'
  - `archived_to`: Path if backed up (optional)

## Known Issues/Constraints
- macOS .Trash folder has restricted access (automatically uses custom bin folder)
- Large directories may take time to scan (progress indicators provided)
- Network/mounted drives: treated like local drives (may have longer timeouts)
- Windows notifications require optional `win10toast` package

## Testing
Run basic tests:
```bash
pytest tests/
```

Test CLI manually:
```bash
jhadoo --help
jhadoo --version
jhadoo --dry-run
jhadoo --generate-config
jhadoo --dashboard
```

## Version History
- v1.0.0: Initial release with comprehensive features
  - Core cleanup functionality
  - Safety features (dry-run, size caps, confirmations, archive mode)
  - OS compatibility (macOS, Windows, Linux)
  - Progress indicators and notifications
  - Dashboard with statistics and trends
  - Undo capability with deletion manifest
  - PyPI-ready package structure

## Installation
```bash
pip install jhadoo
```

## Usage
```bash
# Preview what would be deleted
jhadoo --dry-run

# Run cleanup with defaults
jhadoo

# Run with custom config
jhadoo --config myconfig.json

# Archive instead of delete
jhadoo --archive
```


