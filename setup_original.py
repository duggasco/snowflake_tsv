#!/usr/bin/env python3
"""
Setup script for Snowflake ETL Pipeline Package
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the version from __init__.py
def get_version():
    """Extract version from __init__.py"""
    init_file = Path(__file__).parent / "snowflake_etl" / "__init__.py"
    with open(init_file, "r") as f:
        for line in f:
            if line.startswith("__version__"):
                # Extract version string
                return line.split("=")[1].strip().strip('"').strip("'")
    return "0.0.0"

# Read README for long description
def get_long_description():
    """Read README.md for package description"""
    readme_file = Path(__file__).parent / "README.md"
    if readme_file.exists():
        with open(readme_file, "r", encoding="utf-8") as f:
            return f.read()
    return "Snowflake ETL Pipeline - Enterprise-grade ETL solution for large TSV files"

# Define package metadata
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
    },
    
    # Package configuration
    packages=find_packages(exclude=["tests*", "docs*", "scripts*"]),
    package_data={
        "snowflake_etl": ["py.typed"],  # Include type hints
    },
    include_package_data=True,
    zip_safe=False,  # Don't install as zip for better debugging
    
    # Python version requirement
    python_requires=">=3.8",
    
    # Dependencies
    install_requires=[
        "snowflake-connector-python>=3.0.0",
        "pandas>=1.5.0",
        "numpy>=1.20.0",
        "jsonschema>=4.0.0",
        "chardet>=4.0.0",
        "psutil>=5.8.0",  # For system resource monitoring
        "tqdm>=4.60.0",  # For progress bars
    ],
    
    # Optional dependencies for different use cases
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "pytest-mock>=3.6.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.950",
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "pytest-mock>=3.6.0",
            "faker>=15.0.0",  # For generating test data
        ],
        "performance": [
            "memory-profiler>=0.60.0",
            "py-spy>=0.3.0",
        ],
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
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
        "Environment :: Console",
    ],
    
    # Additional metadata
    keywords="snowflake etl pipeline tsv data-engineering bulk-loading",
    platforms=["any"],
    license="MIT",
)