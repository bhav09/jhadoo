"""Setup script for jhadoo package."""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

setup(
    name="jhadoo",
    version="1.1.0",
    author="Bhavishya",
    author_email="your.email@example.com",  # Update with your email
    description="Smart cleanup tool for developers - removes unused venv, node_modules, Docker images, scans Git repos, and more",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/jhadoo",  # Update with your GitHub URL
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/jhadoo/issues",
        "Documentation": "https://github.com/yourusername/jhadoo#readme",
        "Source Code": "https://github.com/yourusername/jhadoo",
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


