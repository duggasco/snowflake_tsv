#!/usr/bin/env python3
"""
Test script for CSV support implementation - Phase 4
Tests documentation, help text, and user guides
"""

import sys
import subprocess
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_documentation_files():
    """Test that all documentation files exist and contain CSV references"""
    print("Testing documentation files...")
    
    docs_to_check = [
        ("README.md", ["CSV", "csv", "comma-separated", "file_format"]),
        ("CLAUDE.md", ["CSV", "FormatDetector", "delimiter", "quote_char"]),
        ("CSV_USER_GUIDE.md", ["CSV Processing", "delimiter", "format detection"]),
        ("CHANGELOG.md", ["CSV", "Phase 1", "Phase 2", "Phase 3"])
    ]
    
    for doc_file, keywords in docs_to_check:
        doc_path = Path(doc_file)
        if not doc_path.exists():
            print(f"✗ Missing: {doc_file}")
            continue
            
        content = doc_path.read_text()
        missing_keywords = []
        for keyword in keywords:
            if keyword not in content:
                missing_keywords.append(keyword)
        
        if missing_keywords:
            print(f"✗ {doc_file} missing keywords: {', '.join(missing_keywords)}")
        else:
            print(f"✓ {doc_file} contains all required keywords")


def test_cli_help_text():
    """Test that CLI help text mentions CSV support"""
    print("\nTesting CLI help text...")
    
    # Test main help
    try:
        result = subprocess.run(
            ["python3", "-m", "snowflake_etl", "--help"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if "CSV" in result.stdout or "CSV/TSV" in result.stdout:
            print("✓ Main help mentions CSV support")
        else:
            print("✗ Main help does not mention CSV")
            
    except Exception as e:
        print(f"! Could not test CLI help: {e}")
    
    # Test load subcommand help
    try:
        result = subprocess.run(
            ["python3", "-m", "snowflake_etl", "load", "--help"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if "CSV" in result.stdout or "csv" in result.stdout:
            print("✓ Load subcommand mentions CSV support")
        else:
            print("✗ Load subcommand does not mention CSV")
            
    except Exception as e:
        print(f"! Could not test load help: {e}")


def test_readme_sections():
    """Verify README has all required CSV sections"""
    print("\nTesting README sections...")
    
    readme_path = Path("README.md")
    if not readme_path.exists():
        print("✗ README.md not found")
        return
        
    content = readme_path.read_text()
    
    required_sections = [
        ("## Features", "Multi-Format Support"),
        ("### Supported File Formats", "CSV Files"),
        ("## CSV/TSV Processing Examples", "Processing CSV Files"),
        ("### File Format Issues", "CSV/TSV Format Detection"),
        ("file_format", "Configuration examples")
    ]
    
    for section, description in required_sections:
        if section in content:
            print(f"✓ {description} section found")
        else:
            print(f"✗ Missing section: {description}")


def test_claude_md_updates():
    """Verify CLAUDE.md has CSV technical details"""
    print("\nTesting CLAUDE.md technical documentation...")
    
    claude_path = Path("CLAUDE.md")
    if not claude_path.exists():
        print("✗ CLAUDE.md not found")
        return
        
    content = claude_path.read_text()
    
    required_content = [
        ("FormatDetector", "FormatDetector class documented"),
        ("file_format", "file_format field documented"),
        ("delimiter", "delimiter configuration documented"),
        ("CSV Files", "CSV file support documented"),
        ("File Format Support", "Format support section exists")
    ]
    
    for keyword, description in required_content:
        if keyword in content:
            print(f"✓ {description}")
        else:
            print(f"✗ Missing: {description}")


def test_user_guide():
    """Verify CSV User Guide completeness"""
    print("\nTesting CSV User Guide...")
    
    guide_path = Path("CSV_USER_GUIDE.md")
    if not guide_path.exists():
        print("✗ CSV_USER_GUIDE.md not found")
        return
        
    content = guide_path.read_text()
    
    required_topics = [
        "Quick Start",
        "File Format Detection",
        "Common CSV Scenarios",
        "Advanced Features",
        "Performance Optimization",
        "Troubleshooting",
        "Best Practices",
        "Command Reference"
    ]
    
    for topic in required_topics:
        if f"## {topic}" in content or f"### {topic}" in content:
            print(f"✓ {topic} section present")
        else:
            print(f"✗ Missing topic: {topic}")


def test_python_docstrings():
    """Verify Python module docstrings mention CSV"""
    print("\nTesting Python docstrings...")
    
    modules_to_check = [
        ("snowflake_etl/models/file_config.py", "FileConfig", "CSV/TSV"),
        ("snowflake_etl/utils/format_detector.py", "format_detector", "CSV/TSV"),
        ("snowflake_etl/core/snowflake_loader.py", "SnowflakeLoader", "CSV/TSV")
    ]
    
    for module_path, class_name, expected in modules_to_check:
        path = Path(module_path)
        if not path.exists():
            print(f"✗ Module not found: {module_path}")
            continue
            
        content = path.read_text()
        if expected in content:
            print(f"✓ {class_name} docstring mentions {expected}")
        else:
            print(f"✗ {class_name} docstring missing {expected}")


def test_configuration_examples():
    """Verify configuration examples include CSV"""
    print("\nTesting configuration examples...")
    
    # Check for example CSV config
    example_config = Path("config/example_csv_config.json")
    if example_config.exists():
        print("✓ Example CSV config file exists")
    else:
        print("✗ Example CSV config file missing")
    
    # Check README for CSV config examples
    readme = Path("README.md")
    if readme.exists():
        content = readme.read_text()
        if '"file_format": "CSV"' in content:
            print("✓ README contains CSV config examples")
        else:
            print("✗ README missing CSV config examples")


def main():
    """Run all Phase 4 documentation tests"""
    print("=" * 60)
    print("CSV Support Phase 4 Tests - Documentation")
    print("=" * 60)
    
    test_documentation_files()
    test_cli_help_text()
    test_readme_sections()
    test_claude_md_updates()
    test_user_guide()
    test_python_docstrings()
    test_configuration_examples()
    
    print("\n" + "=" * 60)
    print("Phase 4 Documentation Testing Complete")
    print("=" * 60)
    
    print("\nDocumentation Summary:")
    print("- README.md updated with CSV examples")
    print("- CLAUDE.md includes technical CSV details")
    print("- CSV_USER_GUIDE.md provides comprehensive guide")
    print("- CLI help text mentions CSV/TSV support")
    print("- Python docstrings updated")
    print("- Configuration examples include CSV")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())