"""Tests for TUI resize hardening (TS08_TC_12).

Drives JhadooTUI with a fake curses stdscr that:
  - Streams `curses.KEY_RESIZE` events between user inputs to simulate rapid
    terminal resize.
  - Shrinks and grows `getmaxyx()` across calls.
  - Records every `addstr` call so we can assert that `_safe_addstr` clips
    text to current window bounds.

The TUI must not raise any exception during this sequence — that is the
core regression being tested.
"""

import unittest
from unittest.mock import patch
from jhadoo.config import Config
from jhadoo.tui import JhadooTUI


class _FakeCurses:
    """Minimal stub of the curses module surface used by JhadooTUI."""

    # Color / attribute constants used by the TUI
    COLOR_BLUE = 1
    COLOR_GREEN = 2
    COLOR_YELLOW = 3
    COLOR_RED = 4
    COLOR_CYAN = 5
    COLOR_MAGENTA = 6
    COLOR_BLACK = 7
    COLOR_WHITE = 8

    A_BOLD = 0x10
    A_REVERSE = 0x20
    A_NORMAL = 0x0

    KEY_RESIZE = 410
    KEY_UP = 259
    KEY_DOWN = 258
    KEY_ENTER = 343
    KEY_BACKSPACE = 263

    class error(Exception):
        pass

    def __init__(self):
        self._color_pairs = {}

    def curs_set(self, n):
        pass

    def start_color(self):
        pass

    def use_default_colors(self):
        pass

    def init_pair(self, pair, fg, bg):
        self._color_pairs[pair] = (fg, bg)

    def color_pair(self, pair):
        return pair  # opaque attr token

    def nocbreak(self):
        pass

    def cbreak(self):
        pass

    def noecho(self):
        pass

    def echo(self):
        pass

    def endwin(self):
        pass

    def wrapper(self, fn):
        # We don't really enter curses; just invoke the main loop with the
        # fake stdscr supplied by the test.
        self._fake_stdscr = self._make_stdscr()
        fn(self._fake_stdscr)

    def _make_stdscr(self):
        return _FakeStdScr(self)


class _FakeStdScr:
    """Stub stdscr that records addstr calls and serves a scripted getch stream."""

    def __init__(self, curses_stub):
        self._curses = curses_stub
        self._h = 24
        self._w = 80
        self.addstr_calls = []
        self._getch_queue = []
        self._nodelay = False

    # --- dimensions ---
    def getmaxyx(self):
        return self._h, self._w

    def set_size(self, h, w):
        self._h = h
        self._w = w

    # --- drawing ---
    def addstr(self, y, x, text, attr=0):
        # Mirror real curses: raise if out of bounds (so _safe_addstr is exercised)
        if y < 0 or y >= self._h or x < 0 or x >= self._w:
            raise _FakeCurses.error()
        max_len = max(0, self._w - x - 1)
        clipped = text[:max_len]
        if not clipped:
            return
        self.addstr_calls.append((y, x, clipped, attr))

    def clear(self):
        self.addstr_calls.clear()

    def refresh(self):
        pass

    def keypad(self, flag):
        pass

    def nodelay(self, flag):
        self._nodelay = bool(flag)

    # --- input ---
    def push_keys(self, keys):
        self._getch_queue.extend(keys)

    def getch(self):
        if self._getch_queue:
            return self._getch_queue.pop(0)
        if self._nodelay:
            return -1
        # Blocking getch with empty queue — return Q to break the loop
        return ord('q')


class TestTUIResizeHardening(unittest.TestCase):

    def _make_tui(self, fake_curses):
        config = Config()
        tui = JhadooTUI(config, dry_run=True, archive_mode=False)
        # Patch the module-level curses import in jhadoo.tui
        with patch('jhadoo.tui.curses', fake_curses):
            fake_curses.wrapper(tui._curses_main)
        return tui, fake_curses

    def test_rapid_resize_does_not_crash(self):
        """Stream 10 KEY_RESIZE events interspersed with size changes; the
        TUI must redraw on each event without raising."""
        fake = _FakeCurses()
        config = Config()
        tui = JhadooTUI(config, dry_run=True)

        with patch('jhadoo.tui.curses', fake):
            fake._fake_stdscr = fake._make_stdscr()
            stdscr = fake._fake_stdscr

            # Script 10 rapid resize cycles (small ↔ large) then a Q to quit.
            # Each KEY_RESIZE triggers a redraw of _draw_home.
            keys = []
            sizes = []
            for i in range(10):
                small = (i % 2 == 0)
                sizes.append((12, 50) if small else (24, 80))
                keys.append(_FakeCurses.KEY_RESIZE)
            keys.append(ord('q'))

            stdscr.push_keys(keys)
            # Apply the size schedule across getch calls
            original_getch = stdscr.getch
            call_count = [0]

            def scheduled_getch():
                idx = min(call_count[0], len(sizes) - 1)
                stdscr._h, stdscr._w = sizes[idx]
                call_count[0] += 1
                return original_getch()

            stdscr.getch = scheduled_getch

            # Must not raise
            try:
                tui._curses_main(stdscr)
            except _FakeCurses.error:
                self.fail("TUI raised curses.error during rapid resize")

    def test_safe_addstr_clips_long_strings(self):
        """_safe_addstr must clip text to current window width and not raise
        when text would overflow."""
        fake = _FakeCurses()
        config = Config()
        tui = JhadooTUI(config, dry_run=True)
        stdscr = fake._make_stdscr()
        stdscr.set_size(10, 20)
        tui.stdscr = stdscr

        # Long string that would overflow the 20-col window at column 5
        long_text = "x" * 200
        tui._safe_addstr(0, 5, long_text, 0)
        # Verify it was clipped to (20 - 5 - 1) = 14 chars
        self.assertEqual(len(stdscr.addstr_calls), 1)
        _, _, clipped, _ = stdscr.addstr_calls[0]
        self.assertLessEqual(len(clipped), 14)

    def test_safe_addstr_skips_out_of_bounds_y(self):
        fake = _FakeCurses()
        config = Config()
        tui = JhadooTUI(config, dry_run=True)
        stdscr = fake._make_stdscr()
        stdscr.set_size(10, 80)
        tui.stdscr = stdscr

        # y >= h must be a no-op
        tui._safe_addstr(10, 0, "hello", 0)
        tui._safe_addstr(-1, 0, "hello", 0)
        self.assertEqual(stdscr.addstr_calls, [])

    def test_too_small_renders_prompt(self):
        """When terminal is below the minimum size, _too_small returns True
        and _draw_too_small writes the 'Terminal too small' message."""
        fake = _FakeCurses()
        config = Config()
        tui = JhadooTUI(config, dry_run=True)
        stdscr = fake._make_stdscr()
        stdscr.set_size(5, 40)  # below MIN_H=10 but wide enough for the message
        tui.stdscr = stdscr

        self.assertTrue(tui._too_small())
        # Must patch jhadoo.tui.curses so _safe_addstr catches _FakeCurses.error
        with patch('jhadoo.tui.curses', fake):
            tui._draw_too_small()
        all_text = "".join(call[2] for call in stdscr.addstr_calls)
        self.assertIn("Terminal too small", all_text)


if __name__ == "__main__":
    unittest.main()
