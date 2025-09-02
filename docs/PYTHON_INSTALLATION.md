# Python 3.11 Automatic Installation

*Added in v3.4.3*

## Overview

The Snowflake ETL Pipeline now offers automatic Python 3.11 installation when not found on the system. This ensures optimal compatibility and performance with user-configurable installation paths.

## Features

### Automatic Detection
- Checks for Python 3.11 first (preferred version)
- Falls back to any Python 3.x if 3.11 not found
- Offers to install Python 3.11 if missing or using older version

### Installation Methods

#### 1. Source Installation (Recommended for Custom Paths)
- Downloads Python 3.11.9 from python.org
- Compiles with optimizations
- Installs to user-specified directory
- No sudo required for user directories
- Complete control over installation

#### 2. System Package Manager
- Uses OS-specific package managers
- Faster installation
- May require sudo privileges
- Provides installation commands for manual execution

## Installation Flow

```
Start Script
    ↓
Check for Python 3.11
    ↓
Found? → Continue
    ↓ (No)
Found Python 3.x? → Offer upgrade
    ↓ (No)
No Python? → Require installation
    ↓
Choose Installation Method
    ├─ Source Installation
    │   ├─ Choose install path
    │   ├─ Check build tools
    │   ├─ Download Python
    │   ├─ Compile & Install
    │   └─ Save path
    └─ Package Manager
        ├─ Detect OS
        ├─ Show commands
        └─ User runs manually
```

## Custom Installation Paths

### Default Path
```
$HOME/.local
```
No sudo required, user-owned directory.

### Common Alternative Paths
- `$HOME/opt` - User's optional software
- `$HOME/.python` - Hidden Python installation
- `/opt/python3.11` - System-wide (requires sudo)
- `/usr/local` - Traditional Unix location (requires sudo)

### Path Selection
During source installation, you'll be prompted:
```
Enter installation base path
(press Enter for default: /home/user/.local):
```

You can enter:
- Absolute path: `/home/user/my-python`
- Relative path: `./python` (converted to absolute)
- Tilde path: `~/software/python` (expanded)

## Build Requirements

### For Source Installation
Required tools:
- `gcc` - C compiler
- `make` - Build automation
- `wget` - Download Python source
- `tar` - Extract archives

Install on different systems:

**Ubuntu/Debian:**
```bash
sudo apt-get install build-essential wget
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum groupinstall 'Development Tools'
sudo yum install wget
```

**macOS:**
```bash
# Install Xcode Command Line Tools
xcode-select --install
```

## OS-Specific Installation

### Ubuntu/Debian
```bash
# Add repository with Python 3.11
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt-get update
sudo apt-get install python3.11 python3.11-venv python3.11-dev -y
```

### RHEL/CentOS/Fedora
```bash
sudo dnf install python3.11 python3.11-devel -y
```

### macOS (Homebrew)
```bash
brew install python@3.11
```

### Arch Linux
```bash
sudo pacman -S python python-pip
```

## Compilation Options

The source installation uses these optimizations:

### With Optimizations (Default)
```bash
./configure --prefix="$install_path" \
    --enable-optimizations \      # PGO optimization
    --with-ensurepip=install \    # Include pip
    --enable-shared \              # Shared libraries
    LDFLAGS="-Wl,-rpath,$install_path/lib"
```
Takes 5-10 minutes, ~10% performance improvement.

### Without Optimizations (Fallback)
```bash
./configure --prefix="$install_path" \
    --with-ensurepip=install \
    --enable-shared \
    LDFLAGS="-Wl,-rpath,$install_path/lib"
```
Takes 2-3 minutes, standard performance.

## Path Management

### Automatic Path Loading
The script saves custom Python installations to:
```
.etl_state/.python311_path
```

This path is automatically loaded on each run.

### Manual PATH Configuration
To make the installation permanent, add to your shell profile:

**Bash (~/.bashrc):**
```bash
export PATH="$HOME/.local/bin:$PATH"
```

**Zsh (~/.zshrc):**
```bash
export PATH="$HOME/.local/bin:$PATH"
```

**Fish (~/.config/fish/config.fish):**
```fish
set -gx PATH $HOME/.local/bin $PATH
```

## Testing Installation

### Check Python Version
```bash
python3.11 --version
```

### Check Installation Location
```bash
which python3.11
```

### Test Script
```bash
./test_scripts/test_python_install.sh
```

## Troubleshooting

### Issue: "Configuration failed"
**Causes:**
- Missing development headers
- Insufficient disk space
- Missing SSL development files

**Solution:**
```bash
# Install development packages
sudo apt-get install libssl-dev libffi-dev python3-dev
```

### Issue: "Build failed"
**Causes:**
- Insufficient memory
- Compiler errors

**Solution:**
```bash
# Use single-core build (slower but uses less memory)
make -j1
```

### Issue: "Cannot download Python source"
**Causes:**
- Network restrictions
- Proxy requirements

**Solution:**
```bash
# Set proxy before running
export https_proxy=http://proxy:8080
./snowflake_etl.sh
```

### Issue: "python3.11: command not found" after installation
**Causes:**
- PATH not updated
- Installation in non-standard location

**Solution:**
```bash
# Find the installation
find $HOME -name python3.11 2>/dev/null

# Add to PATH
export PATH="/path/to/python/bin:$PATH"
```

## Security Considerations

### Verification
Python source is downloaded from official python.org servers over HTTPS.

### Permissions
- User directory installations don't require elevated privileges
- System installations require sudo
- Custom paths inherit parent directory permissions

### Best Practices
1. Install in user directory when possible
2. Avoid running compilation as root
3. Verify checksums for production systems (manual)

## Advanced Options

### Custom Compilation Flags
Edit the script to add flags:
```bash
./configure --prefix="$python_prefix" \
    --enable-optimizations \
    --with-lto \              # Link-time optimization
    --enable-ipv6 \           # IPv6 support
    --with-computed-gotos     # Faster interpreter
```

### Multiple Python Versions
The script supports side-by-side installations:
```
~/.local/
├── bin/
│   ├── python3.11
│   ├── python3.12
│   └── python3.10
```

### Uninstalling
To remove a custom installation:
```bash
# Remove the installation directory
rm -rf $HOME/.local/lib/python3.11
rm -rf $HOME/.local/bin/python3.11*

# Clear saved path
rm .etl_state/.python311_path
```

## Summary

The automatic Python 3.11 installation feature ensures:
- ✅ Consistent Python version across environments
- ✅ User-controlled installation paths
- ✅ No sudo required for user directories
- ✅ Automatic detection and path management
- ✅ Support for all major operating systems
- ✅ Optimized builds for better performance

This makes deployment easier in environments where Python 3.11 isn't pre-installed or where users need specific Python installations.