"""Setup script for jhadoo package."""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

setup(
    name="jhadoo",
    version="1.3.6",
    author="Bhavishya",
    author_email="your.email@example.com",  # Update with your email
    description="Smart multi-platform cleanup tool (macOS, Windows, Linux) - auto-cleans unused files, caches, apps, installers, and project build bloat",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bhav09/jhadoo",
    project_urls={
        "Bug Tracker": "https://github.com/bhav09/jhadoo/issues",
        "Documentation": "https://github.com/bhav09/jhadoo#readme",
        "Source Code": "https://github.com/bhav09/jhadoo",
    },
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: System :: Filesystems",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires=">=3.10",
    install_requires=[
        # No external dependencies required for core functionality
    ],
    extras_require={
        "notifications": [
            "win10toast>=0.9; platform_system=='Windows'",
        ],
        "metrics": [
            # Optional: enables live CPU/Memory/Disk/Network telemetry in the
            # TUI dashboard. Without it, the TUI falls back to stdlib sampling
            # on Linux/macOS and degrades to a [SIMULATED] placeholder on
            # platforms where stdlib sampling isn't implemented.
            "psutil>=5.9",
        ],
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "black>=23.0",
            "flake8>=6.0",
            "mypy>=1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "jhadoo=jhadoo.cli:main",
        ],
    },
    keywords=[
        "cleanup",
        "disk-space",
        "file-management",
        "folder-cleanup",
        "development-tools",
        "build-cleanup",
        "cache-cleanup",
        "automation",
        "devops",
        "universal-cleaner",
        "multi-language",
        "folder-agnostic",
    ],
    include_package_data=True,
    zip_safe=False,
)


