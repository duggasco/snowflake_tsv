# TODO.md - Task List
*Last Updated: 2025-09-03*
*Version: 3.4.14*

## ✅ Recently Completed (2025-09-03)

### Cross-Environment Support
- [x] Added standalone file compression functionality (v3.4.14)
- [x] Created compress_tsv.py utility for CLI compression
- [x] Added "Compress TSV File (No Upload)" to File Tools menu
- [x] Implemented progress tracking and batch compression support

### Script Stability Fixes
- [x] Fixed silent failure when run non-interactively (v3.4.4-3.4.7)
- [x] Fixed unbound variable errors with proxy detection
- [x] Fixed eval command issues with special characters in proxy URLs
- [x] Added proper error handling for non-TTY environments

### Proxy & Network Enhancements  
- [x] Added proxy support for PyPI package installation (v3.4.2)
- [x] Added proxy support for Python source downloads (v3.4.6-3.4.8)
- [x] Added HTTP fallback for proxy tunneling failures
- [x] Support for pre-downloaded Python packages (v3.4.9)
- [x] Added proxy support for Snowflake connections (v3.4.11)
- [x] Fixed SSL handshake errors with corporate proxies (v3.4.12)
- [x] Enhanced proxy routing for all HTTP/HTTPS traffic (v3.4.13)

### Compatibility Updates
- [x] Relaxed Snowflake connector version to 2.7.5 (v3.4.10)
- [x] Added support for older package versions in restricted environments

## 🚀 High Priority - Next Session

### Snowflake Connection Issues
- [ ] Test proxy configuration with actual corporate Snowflake instance
- [ ] Verify SSL/TLS settings work in production
- [ ] Document proxy troubleshooting steps
- [ ] Add retry logic for connection failures

### Testing & Validation  
- [ ] Full integration test with proxy environment
- [ ] Test with restricted package repositories
- [ ] Verify Python 3.11 installation process
- [ ] Test all fallback mechanisms

### Documentation
- [ ] Create comprehensive proxy setup guide
- [ ] Document SSL/TLS configuration options
- [ ] Add troubleshooting flowchart
- [ ] Update README with proxy instructions

## 📋 Pending Features

### Performance Optimization
- [ ] Implement connection retry with exponential backoff
- [ ] Add connection pool monitoring
- [ ] Optimize proxy connection reuse
- [ ] Add metrics for proxy performance

### Error Handling
- [ ] Better error messages for proxy failures
- [ ] Automatic fallback mechanisms
- [ ] Connection diagnostic tools
- [ ] Proxy validation before operations

## 🔍 Known Issues

### Proxy-Related
- Some corporate proxies may require additional authentication methods
- NTLM authentication not yet supported
- Certificate pinning may cause issues with SSL interception

### Compatibility
- Snowflake connector 2.7.5 works but newer versions preferred
- Python 3.11 recommended but 3.7+ should work
- Some package versions may be unavailable in restricted repos

## 📝 Notes

- All proxy settings are now unified across PyPI, Python downloads, and Snowflake
- Insecure mode should only be used in trusted environments
- The test_snowflake_proxy.py script helps diagnose connection issues
- Version 3.4.13 represents significant improvements in proxy handling