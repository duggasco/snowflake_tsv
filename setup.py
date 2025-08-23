#!/usr/bin/env python3
"""
Setup script for Snowflake ETL Pipeline Package
Final optimized version combining best practices from both implementations
"""

from setuptools import setup, find_packages
from pathlib import Path

def get_version():
    """Extract version from __init__.py"""
    init_file = Path(__file__).parent / "snowflake_etl" / "__init__.py"
    with open(init_file, "r") as f:
        for line in f:
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip('"').strip("'")
    return "0.0.0"

def get_long_description():
    """Read README.md for package description"""
    readme_file = Path(__file__).parent / "README.md"
    if readme_file.exists():
        with open(readme_file, "r", encoding="utf-8") as f:
            return f.read()
    return "Snowflake ETL Pipeline - Enterprise-grade ETL solution for large TSV files"

# Define dependency groups
INSTALL_REQUIRES = [
    "snowflake-connector-python>=3.0.0",
    "pandas>=1.5.0",
    "numpy>=1.20.0",
    "jsonschema>=4.0.0",
    "chardet>=4.0.0",
    "psutil>=5.8.0",
    "tqdm>=4.60.0",
]

TEST_REQUIRES = [
    "pytest>=7.0.0",
    "pytest-cov>=3.0.0",
    "pytest-mock>=3.6.0",
    "pytest-asyncio>=0.20.0",
    "faker>=15.0.0",
]

DOCS_REQUIRES = [
    "sphinx>=4.0.0",
    "sphinx-rtd-theme>=1.0.0",
    "sphinx-autodoc-typehints>=1.19.0",
]

LINT_REQUIRES = [
    "black>=22.0.0",
    "flake8>=4.0.0",
    "mypy>=0.950",
    "pylint>=2.15.0",
    "isort>=5.10.0",
    "pre-commit>=2.20.0",
]

PERFORMANCE_REQUIRES = [
    "memory-profiler>=0.60.0",
    "py-spy>=0.3.0",
]

DEV_TOOLS_REQUIRES = [
    "ipython>=8.0.0",
    "ipdb>=0.13.0",
]

setup(
    name="snowflake-etl-pipeline",
    version=get_version(),
    author="Snowflake ETL Team",
    author_email="etl-team@example.com",
    description="Enterprise-grade ETL pipeline for processing large TSV files into Snowflake",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourorg/snowflake-etl-pipeline",
    project_urls={
        "Bug Tracker": "https://github.com/yourorg/snowflake-etl-pipeline/issues",
        "Documentation": "https://github.com/yourorg/snowflake-etl-pipeline/wiki",
        "Source Code": "https://github.com/yourorg/snowflake-etl-pipeline",
        "Changelog": "https://github.com/yourorg/snowflake-etl-pipeline/blob/main/CHANGELOG.md",
    },
    
    # Package configuration
    packages=find_packages(exclude=["tests*", "docs*", "scripts*", "*.tests", "*.tests.*"]),
    package_data={
        "snowflake_etl": ["py.typed"],
    },
    include_package_data=True,
    zip_safe=False,
    
    # Python version requirement
    python_requires=">=3.8",
    
    # Core dependencies
    install_requires=INSTALL_REQUIRES,
    
    # Optional dependencies - granular for CI/CD, comprehensive for developers
    extras_require={
        # Individual groups for targeted installations
        "test": TEST_REQUIRES,
        "docs": DOCS_REQUIRES,
        "lint": LINT_REQUIRES,
        "performance": PERFORMANCE_REQUIRES,
        
        # Comprehensive development environment
        "dev": (
            TEST_REQUIRES +
            DOCS_REQUIRES +
            LINT_REQUIRES +
            PERFORMANCE_REQUIRES +
            DEV_TOOLS_REQUIRES
        ),
        
        # Minimal CI/CD configurations
        "ci": TEST_REQUIRES + LINT_REQUIRES,
        "ci-minimal": TEST_REQUIRES,
    },
    
    # Entry points for CLI
    entry_points={
        "console_scripts": [
            "snowflake-etl=snowflake_etl.__main__:main",
            "sfe=snowflake_etl.__main__:main",  # Short alias
        ],
    },
    
    # Classifiers for PyPI
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Archiving",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Environment :: Console",
        "Natural Language :: English",
        "Typing :: Typed",  # We include py.typed
    ],
    
    keywords="snowflake etl pipeline tsv data-engineering bulk-loading data-warehouse",
    platforms=["any"],
    license="MIT",
)