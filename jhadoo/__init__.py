"""jhadoo - Smart cleanup tool for development environments."""

__version__ = "1.0.1"
__author__ = "Bhavishya"
__description__ = "Automated cleanup tool for any unused files and folders in development environments"

from .config import Config
from .core import CleanupEngine
from .cli import main
from .scheduler import Scheduler

__all__ = ['Config', 'CleanupEngine', 'Scheduler', 'main', '__version__']


