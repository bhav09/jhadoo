"""jhadoo - Smart cleanup tool for development environments."""

__version__ = "1.3.0"
__author__ = "Bhavishya"
__description__ = "Smart multi-platform cleanup tool for a seamless vibe coding experience - auto-cleans unused files, caches, apps, installers, and project build bloat"

from .config import Config
from .core import CleanupEngine
from .cli import main
from .scheduler import Scheduler

__all__ = ['Config', 'CleanupEngine', 'Scheduler', 'main', '__version__']


