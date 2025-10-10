"""Desktop notification utilities."""

import os
import platform


def send_notification(title: str, message: str, sound: bool = False):
    """Send a desktop notification.
    
    Args:
        title: Notification title
        message: Notification message
        sound: Whether to play a sound
    """
    system = platform.system().lower()
    
    try:
        if system == "darwin":  # macOS
            _send_macos_notification(title, message, sound)
        elif system == "windows":
            _send_windows_notification(title, message)
        elif system == "linux":
            _send_linux_notification(title, message)
    except Exception as e:
        # Silently fail if notifications don't work
        pass


def _send_macos_notification(title: str, message: str, sound: bool = False):
    """Send notification on macOS using osascript."""
    sound_param = " sound name \"default\"" if sound else ""
    script = f'display notification "{message}" with title "{title}"{sound_param}'
    os.system(f"osascript -e '{script}'")


def _send_windows_notification(title: str, message: str):
    """Send notification on Windows using win10toast."""
    try:
        from win10toast import ToastNotifier
        toaster = ToastNotifier()
        toaster.show_toast(title, message, duration=5, threaded=True)
    except ImportError:
        # Fallback: just print if library not available
        print(f"Notification: {title} - {message}")


def _send_linux_notification(title: str, message: str):
    """Send notification on Linux using notify-send."""
    os.system(f'notify-send "{title}" "{message}"')


def notify_completion(total_size_mb: float, items_deleted: int):
    """Send completion notification."""
    send_notification(
        "jhadoo - Cleanup Complete",
        f"Freed {total_size_mb:.2f} MB across {items_deleted} items",
        sound=True
    )


def notify_error(error_message: str):
    """Send error notification."""
    send_notification(
        "jhadoo - Error",
        f"Cleanup encountered an error: {error_message}",
        sound=False
    )


def notify_dry_run_complete(items_count: int, estimated_size_mb: float):
    """Send dry-run completion notification."""
    send_notification(
        "jhadoo - Dry Run Complete",
        f"Would delete {items_count} items ({estimated_size_mb:.2f} MB)",
        sound=False
    )


