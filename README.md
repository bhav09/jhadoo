# 🧹 jhadoo — one command to reclaim disk space

[![PyPI version](https://badge.fury.io/py/jhadoo.svg)](https://badge.fury.io/py/jhadoo) [![Total Downloads](https://static.pepy.tech/badge/jhadoo?style=flat&units=international_system)](https://pepy.tech/projects/jhadoo) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Every AI-assisted coding session spins up a fresh `venv`, `node_modules`, or build cache. Multiply that by dozens of projects and your disk is quietly eaten alive. **jhadoo** finds every stale environment across your entire machine and reclaims the space in seconds — so you can keep vibe coding without ever worrying about storage.

```bash
pipx install jhadoo && jhadoo --dry-run
```

## Why jhadoo?

Vibe coding with Cursor, Copilot, Bolt, Windsurf, or any AI tool is incredibly productive — but it leaves behind a trail of heavyweight folders that pile up fast:

| Folder | Typical size | Created by |
|---|---|---|
| `venv` / `.venv` | 200 MB – 2 GB | Every Python project |
| `node_modules` | 300 MB – 1 GB+ | Every JS/TS project |
| `__pycache__` | 1 – 50 MB | Python runtime |
| `build` / `dist` | varies | Every build cycle |

A few weeks of vibe coding can silently consume **20–50 GB**. jhadoo scans your home directory (or any directory you point it at), identifies projects you haven't touched in a while, and cleans up their heavy folders — automatically, safely, and in parallel.

## Features

- **Universal** — cleans venv, node_modules, build, dist, target, caches, or any custom folder name
- **Smart staleness detection** — only cleans folders whose parent project is genuinely inactive (ignores OS metadata like .DS_Store, Thumbs.db, etc. that create false freshness)
- **Safe** — dry‑run, size caps, confirmations, archive with one‑click restore
- **Fast** — single-pass parallel scan; prunes heavy dirs (.git, .cache, Library, $RECYCLE.BIN, etc.)
- **Scheduled** — built‑in cron / Task Scheduler support
- **Git Analysis** — detect stale branches & large files (`--git-check`)
- **Docker Cleanup** — prune unused images (`--docker`)
- **Dashboard** — track your savings over time (`--dashboard`)
- **Cross‑platform** — macOS, Windows, Linux
- **Private** — anonymous opt‑out‑anytime telemetry; no IPs, paths, or file names collected

## Usage

```bash
jhadoo                      # clean now (scans home directory)
jhadoo --dry-run            # safe preview
jhadoo --dir /path/to/dir   # scan a custom root directory
jhadoo --archive            # move instead of delete
jhadoo --restore            # undo last archive
jhadoo --dashboard          # view savings & trends
```

See [`examples/`](examples/) for config, scheduling, Python API, and more.

## Install

```bash
# Recommended
pipx install jhadoo

# Or
pip install jhadoo
```

## Privacy & Telemetry

Anonymous telemetry tracks global space savings (random ID, bytes saved, OS, version — nothing else).
- Disable: `jhadoo --telemetry-off`
- Status: `jhadoo --telemetry-status`

## License

MIT — see [LICENSE](LICENSE)

---

If this saved you GBs, please ⭐️ the repo.
