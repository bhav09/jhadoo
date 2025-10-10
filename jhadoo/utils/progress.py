"""Progress indicators for long-running operations."""

import sys
from typing import Optional


class ProgressBar:
    """Simple progress bar for terminal display."""
    
    def __init__(self, total: int, prefix: str = "", width: int = 50):
        self.total = total
        self.prefix = prefix
        self.width = width
        self.current = 0
    
    def update(self, count: int = 1, suffix: str = ""):
        """Update progress bar."""
        self.current += count
        
        if self.total == 0:
            percent = 100
            filled = self.width
        else:
            percent = min(100, int(100 * self.current / self.total))
            filled = int(self.width * self.current / self.total)
        
        bar = "█" * filled + "░" * (self.width - filled)
        
        # Clear line and print progress
        sys.stdout.write(f"\r{self.prefix} |{bar}| {percent}% {suffix}")
        sys.stdout.flush()
        
        if self.current >= self.total:
            sys.stdout.write("\n")
            sys.stdout.flush()
    
    def finish(self):
        """Complete the progress bar."""
        self.current = self.total
        self.update()


class Spinner:
    """Simple spinner for indeterminate progress."""
    
    def __init__(self, message: str = "Working..."):
        self.message = message
        self.frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        self.current_frame = 0
    
    def spin(self):
        """Show next spinner frame."""
        frame = self.frames[self.current_frame % len(self.frames)]
        sys.stdout.write(f"\r{frame} {self.message}")
        sys.stdout.flush()
        self.current_frame += 1
    
    def finish(self, final_message: Optional[str] = None):
        """Stop the spinner."""
        if final_message:
            sys.stdout.write(f"\r✓ {final_message}\n")
        else:
            sys.stdout.write(f"\r✓ {self.message}\n")
        sys.stdout.flush()


