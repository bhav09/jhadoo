# jhadoo

Auto-clean unused development environments, installers, cache files, and remnants. Built to run natively across macOS, Windows, and Linux.

[![PyPI version](https://badge.fury.io/py/jhadoo.svg)](https://badge.fury.io/py/jhadoo) [![Total Downloads](https://static.pepy.tech/badge/jhadoo?style=flat&units=international_system)](https://pepy.tech/projects/jhadoo) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

AI-assisted coding tools (like Cursor, Copilot, Bolt, or Windsurf) make prototyping incredibly fast, but they leave behind a massive footprint of heavyweight folders (`venv`, `node_modules`, build caches) for every small experiment. A few weeks of coding can silently eat 20 to 50 GB of disk space.

Jhadoo is a lightweight, cross-platform terminal utility that scans and purges stale workspace environments, package caches, local installers, and leftover app configurations. It helps maintain a fast, continuous vibe coding workflow without manual filesystem housekeeping.

## Quick Start (Select Your OS)

### macOS & Linux
```bash
pipx install jhadoo && jhadoo --dry-run
```

### Windows (PowerShell)
```powershell
pipx install jhadoo; jhadoo --dry-run
```

## Supported Operating Systems

Jhadoo provides native, equal-parity integration across:
*   **macOS**: Scans app bundles, cleans CocoaPods/Homebrew caches, and supports macOS system paths.
*   **Windows**: Queries registry uninstall entries, purges `%TEMP%`, and includes fallback support for Windows console contexts.
*   **Linux**: Integrates with package managers (`apt`, `dnf`, `pacman`) and cleans up user configurations (`~/.config`, `~/.local`).

## Core Features

*   **Terminal Dashboard (`jhadoo --tui`)** - A fully interactive 24-bit Truecolor console interface:
    *   *Disk Tree Explorer*: Drill down into directories sorted by size and flag files or folders for archiving using standard Arrow Keys or Vim hotkeys (`h/j/k/l`).
    *   *Live Telemetry*: Monitor logical CPU utilization, memory metrics, and I/O rates alongside a calculated system health index.
*   **System-Wide Optimizer (`jhadoo --optimize`)** - Flushes local DNS caches, purges system temporary directories, and updates standard workspace configurations.
*   **Application Uninstaller (`jhadoo --uninstall [APP_NAME]`)** - Removes designated applications along with deep configurations, plists, launch daemons, and hidden leftovers.
*   **Installer Sweeper (`jhadoo --installers`)** - Identifies and safely purges leftover `.dmg`, `.pkg`, `.msi`, `.exe`, `.deb`, and `.rpm` files in Downloads and Desktop directories.
*   **Smart Staleness Verification** - Evaluates true project activity. Skips folders modified solely by automated OS metadata (like `.DS_Store` or `Thumbs.db`) that trigger false freshness values.

## Zero-Risk Safety Guardrails

Jhadoo is built with conservative defaults to protect system directories and critical user data:

1.  **System Path Guardian**: Scans are explicitly restricted from modifying critical OS-specific directories (such as `/System` on macOS, `C:\Windows` on Windows, or `/boot` and `/etc` on Linux).
2.  **Dry-Run Mode (`--dry-run` / `-n`)**: Preview exactly what folders, caches, or applications will be targeted before any changes occur on disk.
3.  **Archive and Restore (`--archive` / `--restore`)**: Instead of permanently deleting assets, Jhadoo can move them to a secure local archive directory (`~/.jhadoo_archive`). Use `jhadoo --restore` to instantly revert the previous session's cleanup.
4.  **No Greedy Wildcards**: Leftover config directories are identified strictly using bounded package or vendor matches, keeping shared directories safe.

## Installation

### Standard Installation
```bash
# Recommended (install as a global, isolated binary)
pipx install jhadoo

# Standard pip installation
pip install jhadoo
```

*Note: For Windows users wanting to run the interactive curses TUI, please install the dependency wrapper:*
```bash
pip install windows-curses
```

### Run Directly from the Source Code (GitHub Repo)
If you want to run, contribute, or test the tool directly from the git repository on your system without installing the package globally, follow this procedure:

1.  **Clone the Repository**:
    ```bash
    git clone https://github.com/bhav09/jhadoo.git
    cd jhadoo
    ```

2.  **Set Up a Virtual Environment (Optional but recommended)**:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
    ```

3.  **Install in Editable Mode**:
    This registers the `jhadoo` command locally and links it directly to your source files:
    ```bash
    pip install -e .
    ```

4.  **Alternatively, Run as a Python Module**:
    If you do not want to install it at all, you can invoke the CLI entry module directly:
    ```bash
    python3 -m jhadoo --dry-run
    ```

## Command Reference

### Default Scanning and Core Cleaning
```bash
jhadoo                # Run the standard developer project cleanup scan
jhadoo --dry-run      # Safely preview potential savings without modifying anything
jhadoo --archive      # Back up matching targets to ~/.jhadoo_archive instead of deleting
jhadoo --restore      # Revert the last run's archived deletions back to original paths
jhadoo --dashboard    # View past savings history, clean statistics, and trend predictions
```

### Specialized Operations
```bash
jhadoo --tui         # Launch the interactive console disk and telemetry dashboard
jhadoo --optimize    # Run system maintenance (flush DNS, purge temp files, clean package stores)
jhadoo --installers  # Scan Downloads and Desktop for setups and installer binaries
jhadoo --uninstall   # Interactive prompt to remove applications and their configuration remnants
```

## Privacy and Telemetry

*   **Local Processing**: File paths, usernames, and system details never leave your computer.
*   **Minimal Telemetry**: To track the overall open-source impact, Jhadoo runs aggregate telemetry metrics (global bytes freed, runtime, and OS type) securely enabled on installation. 
*   **Opt-Out Anytime**:
    *   Disable: `jhadoo --telemetry-off`
    *   Check status: `jhadoo --telemetry-status`

## License

MIT License. See [LICENSE](LICENSE) for more details.
