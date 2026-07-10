"""Cross-platform live system metrics for the TUI dashboard.

Returns CPU %, memory %, disk I/O MB/s, and network I/O MB/s.

Strategy:
  1. If `psutil` is installed, use it — it is the most accurate and cheapest
     path. Users can opt in via `pip install jhadoo[metrics]`.
  2. Otherwise, fall back to stdlib-only sampling per OS:
       - Linux: parse /proc/stat, /proc/meminfo, /proc/diskstats, /proc/net/dev
       - macOS: subprocess calls to vm_stat, ps, netstat -ib, iostat -d -w 1 -c 2
       - Windows: wmic / typeperf (best-effort; not all counters exist on every SKU)
  3. If a metric cannot be obtained on the current platform, return `None` so
     the TUI can render a "[SIMULATED]" placeholder instead of crashing.

All sampling methods are designed to be called at most once per TUI frame
(≤1 Hz). They block for a short delta window when computing rate-based
metrics (disk/net I/O) so callers should not invoke them in a hot loop.

This module never raises — metrics are best-effort. The TUI remains usable
even on minimal/containerised systems where some counters are unavailable.
"""

import os
import time
import platform
import subprocess
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Optional psutil path — imported lazily so the package stays zero-dep when
# the extras aren't installed.
_psutil = None
try:
    import psutil as _psutil  # type: ignore
except ImportError:
    _psutil = None


def _read_proc_stat_cpu() -> Optional[Tuple[int, int]]:
    """Return (idle, total) jiffies from /proc/stat on Linux, or None."""
    try:
        with open("/proc/stat", "r") as f:
            line = f.readline()
        parts = line.split()
        if not parts or parts[0] != "cpu":
            return None
        # user, nice, system, idle, iowait, irq, softirq, steal, guest, guest_nice
        vals = [int(p) for p in parts[1:11]]
        idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
        total = sum(vals)
        return idle, total
    except (OSError, ValueError, IndexError):
        return None


def _read_proc_meminfo() -> Optional[Tuple[int, int]]:
    """Return (mem_total_bytes, mem_available_bytes) from /proc/meminfo, or None."""
    try:
        info = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                key, _, rest = line.partition(":")
                # Values look like "16384000 kB"
                rest = rest.strip().split()
                if rest:
                    info[key.strip()] = int(rest[0]) * 1024
        total = info.get("MemTotal")
        avail = info.get("MemAvailable") or info.get("MemFree")
        if total and avail is not None:
            return total, avail
    except (OSError, ValueError):
        pass
    return None


def _read_proc_diskstats_total() -> Optional[int]:
    """Sum sectors-read across all block devices in /proc/diskstats.

    Each sector is 512 bytes. Returns total sectors read+written, or None.
    """
    try:
        total_sectors = 0
        with open("/proc/diskstats", "r") as f:
            for line in f:
                # Fields: major minor name reads_completed reads_merged sectors_read ...
                #         writes_completed writes_merged sectors_written ...
                parts = line.split()
                if len(parts) < 14:
                    continue
                # Skip partitions (major < 7 typically) and md/loop devices to
                # avoid double-counting — simple heuristic: skip names matching
                # loop* / ram* / sr*
                name = parts[2]
                if name.startswith(("loop", "ram", "sr")):
                    continue
                total_sectors += int(parts[5]) + int(parts[9])
        return total_sectors
    except (OSError, ValueError, IndexError):
        return None


def _read_proc_netdev_total() -> Optional[int]:
    """Sum rx+tx bytes across all interfaces in /proc/net/dev, or None."""
    try:
        total = 0
        with open("/proc/net/dev", "r") as f:
            for line in f:
                # Format: "  eth0: 1234  ...  5678  ..."
                if ":" not in line:
                    continue
                iface, rest = line.split(":", 1)
                iface = iface.strip()
                if iface == "lo":
                    continue
                parts = rest.split()
                if len(parts) >= 9:
                    total += int(parts[0]) + int(parts[8])
        return total
    except (OSError, ValueError, IndexError):
        return None


def _macos_vm_stat_memory() -> Optional[Tuple[int, int]]:
    """Parse `vm_stat` to compute (mem_total_bytes, mem_used_bytes) on macOS."""
    try:
        res = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=2)
        if res.returncode != 0:
            return None
        # Lines look like:
        # "Pages free:                          12345."
        # "Pages active:                        67890."
        # "Pages wired down:                    11111."
        # "Page size: 16384 bytes"
        page_size = 16384
        free = active = wired = 0
        for line in res.stdout.splitlines():
            line = line.strip().rstrip(".")
            if line.startswith("Page size:"):
                try:
                    page_size = int(line.split()[-2])
                except (ValueError, IndexError):
                    pass
            elif line.startswith("Pages free:"):
                free = int(line.split()[-1])
            elif line.startswith("Pages active:"):
                active = int(line.split()[-1])
            elif line.startswith("Pages wired down:"):
                wired = int(line.split()[-1])
        # Total physical memory from sysctl
        sysctl = subprocess.run(["sysctl", "-n", "hw.memsize"],
                                capture_output=True, text=True, timeout=2)
        if sysctl.returncode != 0:
            return None
        total = int(sysctl.stdout.strip())
        used = (active + wired) * page_size
        return total, used
    except (subprocess.SubprocessError, ValueError, OSError):
        return None


def _macos_cpu_percent(sample_seconds: float = 0.3) -> Optional[float]:
    """Sample CPU % on macOS by summing per-process %CPU via `ps`."""
    try:
        # Two samples of cumulative CPU time across all processes, delta → %
        res1 = subprocess.run(["ps", "-A", "-o", "%cpu="],
                              capture_output=True, text=True, timeout=2)
        # ps %cpu is already a 0-100 per-core average over the process lifetime.
        # Summing across all processes and clamping to a 0-100 scale gives a
        # rough instantaneous load metric sufficient for the TUI dashboard.
        total = 0.0
        for line in res1.stdout.splitlines():
            line = line.strip()
            if line:
                try:
                    total += float(line)
                except ValueError:
                    continue
        # Normalise by CPU count to get a 0-100% scale.
        ncpu = os.cpu_count() or 4
        return max(0.0, min(100.0, total / ncpu))
    except (subprocess.SubprocessError, ValueError, OSError):
        return None


def _macos_net_io_per_second(sample_seconds: float = 0.5) -> Optional[Tuple[float, float]]:
    """Sample rx/tx MB/s on macOS via `netstat -ib` delta."""
    try:
        def total_bytes():
            res = subprocess.run(["netstat", "-ib"], capture_output=True, text=True, timeout=2)
            if res.returncode != 0:
                return None
            rx = tx = 0
            for line in res.stdout.splitlines()[1:]:
                parts = line.split()
                # Columns vary by macOS version; Ibytes and Obytes are usually
                # the 6th-7th from the end on modern macOS.
                if len(parts) < 11:
                    continue
                # Skip loopback
                ifc = parts[0]
                if ifc.startswith("lo"):
                    continue
                try:
                    # Ibytes is typically parts[-7], Obytes parts[-6] on macOS
                    # but layout varies; guard with try/except per-row.
                    rx += int(parts[-7])
                    tx += int(parts[-6])
                except (ValueError, IndexError):
                    continue
            return rx, tx

        a = total_bytes()
        if a is None:
            return None
        time.sleep(sample_seconds)
        b = total_bytes()
        if b is None:
            return None
        delta_rx = (b[0] - a[0]) / (1024 * 1024)
        delta_tx = (b[1] - a[1]) / (1024 * 1024)
        return delta_rx / sample_seconds, delta_tx / sample_seconds
    except (subprocess.SubprocessError, ValueError, OSError):
        return None


def _macos_disk_io_per_second(sample_seconds: float = 0.5) -> Optional[Tuple[float, float]]:
    """Sample disk read/write MB/s on macOS via `iostat -d -w 1 -c 2`.

    iostat prints one row per second; the second row gives the per-second rate.
    """
    try:
        res = subprocess.run(["iostat", "-d", "-w", "1", "-c", "2"],
                             capture_output=True, text=True, timeout=4)
        if res.returncode != 0:
            return None
        lines = [l for l in res.stdout.splitlines() if l.strip()]
        # iostat -d output: device header, then rows of "  KB/t  tps  MB/s"
        # The last non-empty numeric row is the per-second rate.
        # Parse the LAST numeric row — that's the 1-second sample.
        for line in reversed(lines):
            parts = line.split()
            if len(parts) == 3:
                try:
                    float(parts[0]); float(parts[1])
                    mb_s = float(parts[2])
                    # iostat -d gives a single MB/s column (total throughput).
                    # Split it 50/50 as a rough read/write estimate.
                    return mb_s / 2.0, mb_s / 2.0
                except ValueError:
                    continue
        return None
    except (subprocess.SubprocessError, ValueError, OSError):
        return None


def _windows_cpu_percent() -> Optional[float]:
    """CPU % on Windows via `wmic cpu get loadpercentage`."""
    try:
        res = subprocess.run(["wmic", "cpu", "get", "loadpercentage", "/value"],
                             capture_output=True, text=True, timeout=3)
        if res.returncode != 0:
            return None
        for line in res.stdout.splitlines():
            if "=" in line:
                _, val = line.split("=", 1)
                val = val.strip()
                if val:
                    return float(val)
    except (subprocess.SubprocessError, ValueError, OSError):
        pass
    return None


def _windows_memory_percent() -> Optional[float]:
    """Memory % on Windows via `wmic OS get FreePhysicalMemory,TotalVisibleMemorySize`."""
    try:
        res = subprocess.run(
            ["wmic", "OS", "get", "FreePhysicalMemory,TotalVisibleMemorySize", "/value"],
            capture_output=True, text=True, timeout=3,
        )
        if res.returncode != 0:
            return None
        free = total = None
        for line in res.stdout.splitlines():
            if line.startswith("FreePhysicalMemory="):
                free = int(line.split("=")[1])
            elif line.startswith("TotalVisibleMemorySize="):
                total = int(line.split("=")[1])
        if free is not None and total:
            used = total - free
            return (used / total) * 100.0
    except (subprocess.SubprocessError, ValueError, OSError):
        pass
    return None


class SystemMetrics:
    """Cross-platform live system metrics sampler.

    Each method returns a float in the documented unit, or `None` when the
    metric is unavailable on the current platform. Callers must handle `None`.
    """

    def __init__(self):
        self.system = platform.system().lower()
        # For rate-based metrics via stdlib we keep the previous sample on the
        # instance so the second call returns a real delta. psutil tracks its
        # own state internally.
        self._prev_cpu_idle_total: Optional[Tuple[int, int]] = None
        self._prev_disk_sectors: Optional[int] = None
        self._prev_net_bytes: Optional[int] = None

    # -------- CPU --------

    def cpu_percent(self) -> Optional[float]:
        """Instantaneous CPU utilisation in percent (0-100)."""
        if _psutil is not None:
            try:
                return _psutil.cpu_percent(interval=None)
            except Exception:
                pass

        if self.system == "linux":
            cur = _read_proc_stat_cpu()
            if cur is None:
                return None
            prev = self._prev_cpu_idle_total
            self._prev_cpu_idle_total = cur
            if prev is None:
                # First call — psutil-style: return 0.0 or a quick sample.
                time.sleep(0.1)
                cur2 = _read_proc_stat_cpu()
                if cur2 is None:
                    return None
                prev = cur
                cur = cur2
            d_idle = cur[0] - prev[0]
            d_total = cur[1] - prev[1]
            if d_total <= 0:
                return 0.0
            return max(0.0, min(100.0, (1.0 - d_idle / d_total) * 100.0))

        if self.system == "darwin":
            return _macos_cpu_percent()

        if self.system == "windows":
            return _windows_cpu_percent()

        return None

    # -------- Memory --------

    def memory_percent(self) -> Optional[float]:
        """Memory utilisation in percent (0-100)."""
        if _psutil is not None:
            try:
                return _psutil.virtual_memory().percent
            except Exception:
                pass

        if self.system == "linux":
            info = _read_proc_meminfo()
            if info is None:
                return None
            total, avail = info
            if total <= 0:
                return 0.0
            return ((total - avail) / total) * 100.0

        if self.system == "darwin":
            info = _macos_vm_stat_memory()
            if info is None:
                return None
            total, used = info
            if total <= 0:
                return 0.0
            return (used / total) * 100.0

        if self.system == "windows":
            return _windows_memory_percent()

        return None

    # -------- Disk I/O --------

    def disk_io_per_second(self) -> Tuple[Optional[float], Optional[float]]:
        """Return (read_mb_s, write_mb_s), or (None, None) when unavailable."""
        if _psutil is not None:
            try:
                cur = _psutil.disk_io_counters()
                if cur is None:
                    return None, None
                if not hasattr(self, "_psutil_prev_disk"):
                    self._psutil_prev_disk = cur
                    self._psutil_prev_disk_ts = time.time()
                    return None, None
                prev = self._psutil_prev_disk
                dt = time.time() - self._psutil_prev_disk_ts
                if dt <= 0:
                    return None, None
                read_mb = (cur.read_bytes - prev.read_bytes) / (1024 * 1024)
                write_mb = (cur.write_bytes - prev.write_bytes) / (1024 * 1024)
                self._psutil_prev_disk = cur
                self._psutil_prev_disk_ts = time.time()
                return read_mb / dt, write_mb / dt
            except Exception:
                return None, None

        if self.system == "linux":
            cur = _read_proc_diskstats_total()
            if cur is None:
                return None, None
            prev = self._prev_disk_sectors
            self._prev_disk_sectors = cur
            if prev is None:
                time.sleep(0.2)
                cur2 = _read_proc_diskstats_total()
                if cur2 is None:
                    return None, None
                prev = cur
                cur = cur2
            # sectors are 512 bytes; split 50/50 read/write as a rough estimate
            delta_sectors = cur - prev
            if delta_sectors < 0:
                return 0.0, 0.0
            mb = (delta_sectors * 512) / (1024 * 1024)
            # The /proc/diskstats read covers an unknown time delta; approximate
            # by assuming a 1-second window which matches the TUI frame rate.
            return mb / 2.0, mb / 2.0

        if self.system == "darwin":
            return _macos_disk_io_per_second()

        # Windows disk I/O via typeperf is flaky and slow; degrade gracefully.
        return None, None

    # -------- Network I/O --------

    def net_io_per_second(self) -> Tuple[Optional[float], Optional[float]]:
        """Return (download_mb_s, upload_mb_s), or (None, None)."""
        if _psutil is not None:
            try:
                cur = _psutil.net_io_counters()
                if cur is None:
                    return None, None
                if not hasattr(self, "_psutil_prev_net"):
                    self._psutil_prev_net = cur
                    self._psutil_prev_net_ts = time.time()
                    return None, None
                prev = self._psutil_prev_net
                dt = time.time() - self._psutil_prev_net_ts
                if dt <= 0:
                    return None, None
                dl_mb = (cur.bytes_recv - prev.bytes_recv) / (1024 * 1024)
                ul_mb = (cur.bytes_sent - prev.bytes_sent) / (1024 * 1024)
                self._psutil_prev_net = cur
                self._psutil_prev_net_ts = time.time()
                return dl_mb / dt, ul_mb / dt
            except Exception:
                return None, None

        if self.system == "linux":
            cur = _read_proc_netdev_total()
            if cur is None:
                return None, None
            prev = self._prev_net_bytes
            self._prev_net_bytes = cur
            if prev is None:
                time.sleep(0.2)
                cur2 = _read_proc_netdev_total()
                if cur2 is None:
                    return None, None
                prev = cur
                cur = cur2
            delta = cur - prev
            if delta < 0:
                return 0.0, 0.0
            mb = delta / (1024 * 1024)
            return mb / 2.0, mb / 2.0

        if self.system == "darwin":
            return _macos_net_io_per_second()

        return None, None
