# Proxy Configuration for Restricted Environments

*Added in v3.4.2*

## Overview

The Snowflake ETL Pipeline now automatically detects restricted network environments and helps configure proxy settings for package installation. This is essential for corporate environments with firewalls or proxy requirements.

## How It Works

### 1. Automatic Detection
When setting up the virtual environment for the first time, the script:
1. Tests direct connectivity to PyPI (https://pypi.org)
2. Checks for existing proxy environment variables
3. Prompts for proxy configuration if needed
4. Saves working proxy settings for future use

### 2. Connectivity Test Methods
The script tests connectivity using multiple methods in order:
- **curl** - Most common and reliable
- **wget** - Fallback option
- **Python urllib** - Last resort

### 3. Proxy Detection Flow

```
Start Setup
    ↓
Test Direct Connection
    ↓
Success? → Continue normally
    ↓ (No)
Check Environment Variables
    ↓
Found proxy? → Test it
    ↓
Works? → Save and continue
    ↓ (No)
Interactive Configuration
    ↓
Test User Proxy
    ↓
Works? → Save and continue
    ↓ (No)
Retry or Skip
```

## Configuration Options

### Automatic Detection
If you have proxy environment variables set, they will be detected automatically:
```bash
export https_proxy=http://proxy.company.com:8080
export http_proxy=http://proxy.company.com:8080
./snowflake_etl.sh
```

### Interactive Configuration
If no working proxy is found, you'll be prompted:
```
=== Proxy Configuration Required ===
Please enter your proxy server details.
Common formats:
  - http://proxy.company.com:8080
  - http://username:password@proxy.company.com:8080
  - socks5://proxy.company.com:1080

Enter proxy URL (or 'skip' to proceed without proxy): 
```

### Saved Configuration
Once configured, proxy settings are saved to:
```
.etl_state/.proxy_config
```

This file is automatically loaded on subsequent runs.

## Proxy Formats

### Basic HTTP Proxy
```
http://proxy.company.com:8080
```

### Authenticated Proxy
```
http://username:password@proxy.company.com:8080
```

### SOCKS Proxy
```
socks5://proxy.company.com:1080
```

### HTTPS Proxy
```
https://secure-proxy.company.com:8443
```

## Managing Proxy Settings

### View Current Settings
```bash
cat .etl_state/.proxy_config
```

### Clear Proxy Configuration
```bash
rm .etl_state/.proxy_config
```

### Test Connectivity
```bash
./test_scripts/test_proxy_config.sh
```

### Force Reconfiguration
```bash
# Remove both venv and proxy config
rm -rf etl_venv
rm -f .etl_state/.venv_setup_complete
rm -f .etl_state/.proxy_config
./snowflake_etl.sh
```

## Environment Variables

The script recognizes these standard proxy variables:
- `http_proxy` / `HTTP_PROXY`
- `https_proxy` / `HTTPS_PROXY`
- `no_proxy` / `NO_PROXY` (not currently used)

## Troubleshooting

### Issue: "Cannot connect to PyPI directly"
**Solution**: This is expected in restricted environments. Enter your proxy when prompted.

### Issue: "Proxy connection failed"
**Possible causes**:
1. Incorrect proxy URL format
2. Proxy requires authentication
3. Proxy blocks PyPI access
4. Firewall rules

**Solutions**:
- Verify proxy URL with your IT department
- Include credentials if required: `http://user:pass@proxy:8080`
- Check if proxy allows HTTPS to pypi.org
- Try different proxy servers if available

### Issue: "Package installation fails despite proxy"
**Solutions**:
1. Check proxy allows access to:
   - pypi.org
   - files.pythonhosted.org
   - pypa.io

2. Try manual installation with verbose output:
```bash
source etl_venv/bin/activate
pip install --proxy http://proxy:8080 -v snowflake-connector-python
```

3. Use alternative index:
```bash
pip install --proxy http://proxy:8080 --index-url https://pypi.org/simple/ package_name
```

### Issue: "SSL Certificate verification failed"
**Solution** (use with caution):
```bash
pip install --proxy http://proxy:8080 --trusted-host pypi.org --trusted-host files.pythonhosted.org package_name
```

## Corporate Environment Tips

### 1. Get Proxy Details from IT
Ask your IT department for:
- Proxy server address and port
- Authentication requirements
- Any required certificates
- Allowed domains/URLs

### 2. Check Browser Settings
Often the same proxy used by your browser:
- Chrome: Settings → Advanced → System → Proxy
- Firefox: Settings → Network Settings
- Edge: Settings → System → Proxy

### 3. Use System Proxy
On Linux/Mac, check system proxy:
```bash
echo $https_proxy
env | grep -i proxy
```

### 4. Windows Proxy
On Windows (WSL), get proxy from PowerShell:
```powershell
netsh winhttp show proxy
```

## Security Considerations

### Password Security
⚠️ **Warning**: Proxy passwords are stored in plain text in `.etl_state/.proxy_config`

**Recommendations**:
1. Use proxy without authentication if possible
2. Use environment variables instead of saved config
3. Restrict file permissions:
```bash
chmod 600 .etl_state/.proxy_config
```

### Certificate Validation
- The script maintains SSL certificate validation by default
- Only disable certificate checks if absolutely necessary
- Consider adding corporate certificates to system trust store

## Examples

### Example 1: Simple Corporate Proxy
```
Enter proxy URL: proxy.acme.com:8080
Testing proxy connection...
✓ Proxy connection successful!
```

### Example 2: Authenticated Proxy
```
Enter proxy URL: http://john.doe:secretpass@proxy.acme.com:8080
Testing proxy connection...
✓ Proxy connection successful!
```

### Example 3: Environment Variable Setup
```bash
# In .bashrc or .zshrc
export https_proxy=http://proxy.acme.com:8080
export http_proxy=http://proxy.acme.com:8080

# Then run
./snowflake_etl.sh
```

## Technical Details

### Functions Added
- `test_pypi_connectivity()` - Tests connection to PyPI
- `configure_proxy()` - Interactive proxy configuration
- `clear_proxy_config()` - Utility to clear saved proxy

### Files Created
- `.etl_state/.proxy_config` - Saved proxy URL

### pip Integration
All pip commands automatically use the configured proxy:
```bash
pip install $pip_args package_name
```
Where `$pip_args` contains `--proxy $https_proxy` when configured.

## Summary

The proxy configuration feature ensures the Snowflake ETL Pipeline can be set up in any environment, whether:
- Direct internet access (no configuration needed)
- Corporate proxy (automatic or manual configuration)
- Authenticated proxy (with credentials)
- Restricted environment (with proper proxy)

This makes the tool truly enterprise-ready for deployment in secured corporate networks.