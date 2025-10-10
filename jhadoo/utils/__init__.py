"""Utility modules for jhadoo."""

from .os_compat import (
    get_system,
    normalize_path,
    get_default_bin_folder,
    get_protected_paths,
    is_protected_path,
    is_case_sensitive_fs,
    get_home_directory
)

from .progress import ProgressBar, Spinner

from .safety import (
    confirm_deletion,
    bytes_to_human_readable,
    check_size_threshold,
    is_path_excluded,
    create_deletion_manifest,
    validate_path_safety
)

__all__ = [
    'get_system',
    'normalize_path',
    'get_default_bin_folder',
    'get_protected_paths',
    'is_protected_path',
    'is_case_sensitive_fs',
    'get_home_directory',
    'ProgressBar',
    'Spinner',
    'confirm_deletion',
    'bytes_to_human_readable',
    'check_size_threshold',
    'is_path_excluded',
    'create_deletion_manifest',
    'validate_path_safety'
]


