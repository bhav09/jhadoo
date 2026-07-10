"""Tests for jhadoo.metrics — live system telemetry sampler (TS09_TC_03/04/07).

Verifies that SystemMetrics returns floats or None on each platform branch
without raising, and that the psutil path is used when available.
"""

import unittest
from unittest.mock import patch, MagicMock

from jhadoo import metrics as metrics_module
from jhadoo.metrics import SystemMetrics


class TestSystemMetrics(unittest.TestCase):

    def test_cpu_percent_returns_float_or_none(self):
        """On any platform, cpu_percent must return a float or None — never raise."""
        m = SystemMetrics()
        # We can't predict the exact value, just the shape.
        val = m.cpu_percent()
        self.assertTrue(val is None or isinstance(val, float))
        if val is not None:
            self.assertGreaterEqual(val, 0.0)
            self.assertLessEqual(val, 100.0)

    def test_memory_percent_returns_float_or_none(self):
        m = SystemMetrics()
        val = m.memory_percent()
        self.assertTrue(val is None or isinstance(val, float))
        if val is not None:
            self.assertGreaterEqual(val, 0.0)
            self.assertLessEqual(val, 100.0)

    def test_disk_io_returns_pair_of_floats_or_nones(self):
        m = SystemMetrics()
        r, w = m.disk_io_per_second()
        self.assertTrue(r is None or isinstance(r, float))
        self.assertTrue(w is None or isinstance(w, float))

    def test_net_io_returns_pair_of_floats_or_nones(self):
        m = SystemMetrics()
        dl, ul = m.net_io_per_second()
        self.assertTrue(dl is None or isinstance(dl, float))
        self.assertTrue(ul is None or isinstance(ul, float))

    def test_psutil_path_is_used_when_available(self):
        """When psutil is importable, SystemMetrics should use it for CPU %."""
        fake_psutil = MagicMock()
        fake_psutil.cpu_percent.return_value = 42.0
        fake_psutil.virtual_memory.return_value = MagicMock(percent=55.0)

        with patch.object(metrics_module, "_psutil", fake_psutil):
            m = SystemMetrics()
            self.assertEqual(m.cpu_percent(), 42.0)
            self.assertEqual(m.memory_percent(), 55.0)
            fake_psutil.cpu_percent.assert_called_once()
            fake_psutil.virtual_memory.assert_called_once()

    def test_psutil_unavailable_falls_back_to_stdlib(self):
        """When _psutil is None, the stdlib fallback must run without raising."""
        with patch.object(metrics_module, "_psutil", None):
            m = SystemMetrics()
            # Force a known system to exercise the fallback branch deterministically.
            with patch("platform.system", return_value="Linux"):
                m.system = "linux"
                with patch("jhadoo.metrics._read_proc_stat_cpu", return_value=(100, 200)):
                    # First call seeds prev; second call returns a value
                    m.cpu_percent()
                    with patch("jhadoo.metrics._read_proc_stat_cpu", return_value=(110, 240)):
                        val = m.cpu_percent()
                    # d_idle=10, d_total=40 → 75% busy
                    self.assertAlmostEqual(val, 75.0, places=1)

            with patch("platform.system", return_value="Linux"):
                m.system = "linux"
                with patch("jhadoo.metrics._read_proc_meminfo", return_value=(1000, 200)):
                    val = m.memory_percent()
                    self.assertAlmostEqual(val, 80.0, places=1)

    def test_psutil_disk_io_delta(self):
        """When psutil is available, disk_io_per_second returns a delta-based rate."""
        fake_psutil = MagicMock()
        # Two-call delta: prev=(100,200) → cur=(1100,2200) over 0.5s = 2000/2 MB each
        first = MagicMock(read_bytes=100, write_bytes=200)
        second = MagicMock(read_bytes=1100, write_bytes=2200)
        fake_psutil.disk_io_counters.side_effect = [first, second]

        with patch.object(metrics_module, "_psutil", fake_psutil), \
             patch("jhadoo.metrics.time.time", side_effect=[0.0, 0.5, 0.0, 0.5]):
            m = SystemMetrics()
            # First call seeds prev and returns (None, None)
            r0, w0 = m.disk_io_per_second()
            self.assertIsNone(r0)
            self.assertIsNone(w0)
            # Second call returns the delta rate
            r1, w1 = m.disk_io_per_second()
            # 1000 bytes delta over 0.5 s = 2000 B/s = ~0.0019 MB/s
            self.assertGreater(r1, 0.0)
            self.assertGreater(w1, 0.0)

    def test_no_exception_on_unknown_system(self):
        """An unknown system string must not raise — methods return None."""
        with patch.object(metrics_module, "_psutil", None):
            m = SystemMetrics()
            m.system = "unknownbsd"
            self.assertIsNone(m.cpu_percent())
            self.assertIsNone(m.memory_percent())
            r, w = m.disk_io_per_second()
            self.assertIsNone(r)
            self.assertIsNone(w)
            d, u = m.net_io_per_second()
            self.assertIsNone(d)
            self.assertIsNone(u)


if __name__ == "__main__":
    unittest.main()
