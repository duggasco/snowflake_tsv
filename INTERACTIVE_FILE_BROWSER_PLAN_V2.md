# Interactive File Browser Implementation Plan V2
*Revised: 2025-01-22 - Addressing Critical Feedback*

## Overview
Enhanced plan for interactive TSV file selection with config validation, addressing performance, security, and usability concerns identified in review.

## Critical Design Changes Based on Feedback

### 1. Performance-First Architecture
- **Single Python Process**: One Python script handles all file browsing and config validation
- **Efficient Directory Reading**: Use `os.scandir()` or `pathlib` for fast directory traversal
- **Config Caching**: Load all configs once at startup, not per-file
- **Lazy Loading**: Only load visible items, implement real pagination
- **Background Processing**: Pre-fetch directory contents while user navigates

### 2. Robust File Handling
- **Special Characters**: All paths properly escaped and quoted
- **Symlink Awareness**: Explicitly handle and display symlinks differently
- **Error Propagation**: Never suppress errors silently
- **Path Validation**: Sanitize and validate all user inputs

### 3. Enhanced User Experience
- **Search/Filter**: Real-time filtering as user types
- **Multi-Select**: Support batch file selection
- **Preview Hotkey**: Preview files before selection (e.g., 'p' key)
- **Sort Options**: Sort by name, size, date, type
- **Breadcrumb Navigation**: Show full path with clickable components

## Revised Technical Architecture

### Option 1: Python-Based TUI (Recommended)
Use Python with `curses` or `rich` library for the entire browser:

```python
# tsv_file_browser.py
import os
import json
import curses
import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from functools import lru_cache

@dataclass
class FileItem:
    path: Path
    name: str
    is_dir: bool
    is_symlink: bool
    size: int
    mtime: float
    
    def display_name(self) -> str:
        if self.is_symlink:
            return f"{self.name} -> {os.readlink(self.path)}"
        elif self.is_dir:
            return f"{self.name}/"
        return self.name
    
    def display_size(self) -> str:
        if self.is_dir:
            return "[DIR]"
        units = ['B', 'KB', 'MB', 'GB']
        size = self.size
        for unit in units:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}TB"

class ConfigMatcher:
    """Efficient config matching with caching"""
    
    def __init__(self, config_dir: str):
        self.configs = {}
        self.patterns = {}
        self._load_all_configs(config_dir)
    
    def _load_all_configs(self, config_dir: str):
        """Load all configs once at startup"""
        for config_file in Path(config_dir).glob("*.json"):
            try:
                with open(config_file) as f:
                    config = json.load(f)
                    self.configs[str(config_file)] = config
                    
                    # Pre-compile patterns
                    patterns = []
                    for file_config in config.get('files', []):
                        pattern = file_config.get('file_pattern', '')
                        regex = self._pattern_to_regex(pattern)
                        patterns.append((re.compile(regex), file_config))
                    self.patterns[str(config_file)] = patterns
            except Exception as e:
                # Log error but continue
                print(f"Error loading {config_file}: {e}")
    
    @lru_cache(maxsize=1000)
    def _pattern_to_regex(self, pattern: str) -> str:
        """Convert file pattern to regex with caching"""
        # Properly escape special characters
        pattern = re.escape(pattern)
        pattern = pattern.replace(r'\{date_range\}', r'(\d{8}-\d{8})')
        pattern = pattern.replace(r'\{month\}', r'(\d{4}-\d{2})')
        return f"^{pattern}$"
    
    def find_matching_configs(self, filepath: str) -> List[Tuple[str, Dict]]:
        """Find all configs matching the file"""
        filename = os.path.basename(filepath)
        matches = []
        
        for config_path, patterns in self.patterns.items():
            for regex, file_config in patterns:
                if regex.match(filename):
                    matches.append((config_path, file_config))
                    break
        
        return matches

class TSVFileBrowser:
    """Main file browser with search and navigation"""
    
    def __init__(self, start_dir: str = ".", config_dir: str = "config"):
        self.current_dir = Path(start_dir).resolve()
        self.config_matcher = ConfigMatcher(config_dir)
        self.selected_files = []
        self.filter_text = ""
        self.sort_by = "name"  # name, size, date
        self.show_hidden = False
        self.preview_lines = 10
        
        # Performance: cache directory contents
        self.dir_cache = {}
        self.cache_size_limit = 100
    
    def _get_directory_contents(self, path: Path) -> List[FileItem]:
        """Efficiently read directory with caching"""
        cache_key = str(path)
        
        # Check cache
        if cache_key in self.dir_cache:
            return self.dir_cache[cache_key]
        
        items = []
        try:
            # Use scandir for efficiency
            with os.scandir(path) as entries:
                for entry in entries:
                    # Skip hidden files unless requested
                    if not self.show_hidden and entry.name.startswith('.'):
                        continue
                    
                    # Only include directories and TSV files
                    if entry.is_dir(follow_symlinks=False) or entry.name.endswith('.tsv'):
                        try:
                            stat = entry.stat(follow_symlinks=False)
                            items.append(FileItem(
                                path=Path(entry.path),
                                name=entry.name,
                                is_dir=entry.is_dir(follow_symlinks=False),
                                is_symlink=entry.is_symlink(),
                                size=stat.st_size,
                                mtime=stat.st_mtime
                            ))
                        except (OSError, PermissionError) as e:
                            # Log but continue
                            print(f"Cannot access {entry.path}: {e}")
        
        except (OSError, PermissionError) as e:
            print(f"Cannot read directory {path}: {e}")
            return []
        
        # Sort items
        items = self._sort_items(items)
        
        # Cache management
        if len(self.dir_cache) >= self.cache_size_limit:
            # Remove oldest cache entry
            self.dir_cache.pop(next(iter(self.dir_cache)))
        
        self.dir_cache[cache_key] = items
        return items
    
    def _sort_items(self, items: List[FileItem]) -> List[FileItem]:
        """Sort items based on current sort setting"""
        # Directories first, then files
        dirs = [i for i in items if i.is_dir]
        files = [i for i in items if not i.is_dir]
        
        if self.sort_by == "name":
            dirs.sort(key=lambda x: x.name.lower())
            files.sort(key=lambda x: x.name.lower())
        elif self.sort_by == "size":
            dirs.sort(key=lambda x: x.name.lower())
            files.sort(key=lambda x: x.size, reverse=True)
        elif self.sort_by == "date":
            dirs.sort(key=lambda x: x.mtime, reverse=True)
            files.sort(key=lambda x: x.mtime, reverse=True)
        
        return dirs + files
    
    def _apply_filter(self, items: List[FileItem]) -> List[FileItem]:
        """Apply search filter to items"""
        if not self.filter_text:
            return items
        
        filter_lower = self.filter_text.lower()
        return [i for i in items if filter_lower in i.name.lower()]
    
    def preview_file(self, filepath: Path) -> List[str]:
        """Generate preview of TSV file"""
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                lines = []
                for i, line in enumerate(f):
                    if i >= self.preview_lines:
                        break
                    # Truncate long lines
                    if len(line) > 200:
                        line = line[:200] + "..."
                    lines.append(line.rstrip())
                
                # Add file stats
                stat = filepath.stat()
                lines.insert(0, f"File: {filepath.name}")
                lines.insert(1, f"Size: {stat.st_size:,} bytes")
                lines.insert(2, f"Lines: ~{stat.st_size // 100:,}")  # Rough estimate
                lines.insert(3, "-" * 40)
                
                return lines
        except Exception as e:
            return [f"Error previewing file: {e}"]
    
    def run_interactive(self, stdscr) -> Optional[List[str]]:
        """Main interactive loop using curses"""
        curses.curs_set(0)  # Hide cursor
        current_index = 0
        page_size = curses.LINES - 5  # Leave room for header/footer
        
        while True:
            stdscr.clear()
            
            # Header
            stdscr.addstr(0, 0, f"Directory: {self.current_dir}", curses.A_BOLD)
            stdscr.addstr(1, 0, f"Filter: {self.filter_text or '(none)'}")
            stdscr.addstr(2, 0, "-" * curses.COLS)
            
            # Get and filter items
            items = self._get_directory_contents(self.current_dir)
            filtered_items = self._apply_filter(items)
            
            # Add parent directory option
            if self.current_dir.parent != self.current_dir:
                parent_item = FileItem(
                    path=self.current_dir.parent,
                    name="..",
                    is_dir=True,
                    is_symlink=False,
                    size=0,
                    mtime=0
                )
                filtered_items.insert(0, parent_item)
            
            # Pagination
            start_idx = max(0, current_index - page_size // 2)
            end_idx = min(len(filtered_items), start_idx + page_size)
            
            # Display items
            for i, item in enumerate(filtered_items[start_idx:end_idx]):
                y = i + 3
                if y >= curses.LINES - 2:
                    break
                
                # Highlight current selection
                attr = curses.A_REVERSE if start_idx + i == current_index else 0
                
                # Multi-select indicator
                prefix = "[x] " if item.path in self.selected_files else "[ ] "
                
                # Display line
                line = f"{prefix}{item.display_name():40} {item.display_size():>10}"
                stdscr.addstr(y, 0, line[:curses.COLS-1], attr)
            
            # Footer
            footer = f"[↑↓] Navigate [Enter] Select [Space] Multi [p] Preview [/] Search [s] Sort [q] Quit"
            stdscr.addstr(curses.LINES - 1, 0, footer[:curses.COLS-1], curses.A_BOLD)
            
            # Handle input
            key = stdscr.getch()
            
            if key == ord('q'):  # Quit
                return None
            elif key == curses.KEY_UP:
                current_index = max(0, current_index - 1)
            elif key == curses.KEY_DOWN:
                current_index = min(len(filtered_items) - 1, current_index + 1)
            elif key == ord('\n'):  # Enter - select
                if current_index < len(filtered_items):
                    item = filtered_items[current_index]
                    if item.name == "..":
                        self.current_dir = self.current_dir.parent
                        current_index = 0
                    elif item.is_dir:
                        self.current_dir = item.path
                        current_index = 0
                    else:
                        # File selected
                        if self.selected_files:
                            self.selected_files.append(item.path)
                            return [str(p) for p in self.selected_files]
                        else:
                            return [str(item.path)]
            elif key == ord(' '):  # Space - multi-select
                if current_index < len(filtered_items):
                    item = filtered_items[current_index]
                    if not item.is_dir and item.name != "..":
                        if item.path in self.selected_files:
                            self.selected_files.remove(item.path)
                        else:
                            self.selected_files.append(item.path)
            elif key == ord('p'):  # Preview
                if current_index < len(filtered_items):
                    item = filtered_items[current_index]
                    if not item.is_dir and item.name.endswith('.tsv'):
                        preview = self.preview_file(item.path)
                        # Show preview in a subwindow
                        self._show_preview(stdscr, preview)
            elif key == ord('/'):  # Search
                self.filter_text = self._get_search_input(stdscr)
                current_index = 0
            elif key == ord('s'):  # Sort
                sort_options = ["name", "size", "date"]
                current = sort_options.index(self.sort_by)
                self.sort_by = sort_options[(current + 1) % len(sort_options)]
                # Clear cache to force re-sort
                self.dir_cache.clear()
    
    def _show_preview(self, stdscr, lines: List[str]):
        """Show file preview in a popup window"""
        height = min(len(lines) + 2, curses.LINES - 4)
        width = min(max(len(line) for line in lines) + 4, curses.COLS - 4)
        
        # Center the window
        y = (curses.LINES - height) // 2
        x = (curses.COLS - width) // 2
        
        # Create window
        win = curses.newwin(height, width, y, x)
        win.box()
        
        # Display content
        for i, line in enumerate(lines[:height-2]):
            win.addstr(i + 1, 2, line[:width-4])
        
        win.addstr(height - 1, 2, "Press any key to close")
        win.refresh()
        win.getch()
    
    def _get_search_input(self, stdscr) -> str:
        """Get search input from user"""
        curses.echo()
        stdscr.addstr(curses.LINES - 2, 0, "Search: ")
        search = stdscr.getstr(curses.LINES - 2, 8, 50).decode('utf-8')
        curses.noecho()
        return search
```

### Option 2: Enhanced Bash with Single Python Helper
If we must use bash, create a single Python script that does all heavy lifting:

```python
# tsv_browser_helper.py
#!/usr/bin/env python3
import sys
import json
import os
from pathlib import Path
import argparse

def list_directory(path, show_hidden=False):
    """List directory contents efficiently"""
    result = {
        'dirs': [],
        'files': [],
        'error': None
    }
    
    try:
        path = Path(path)
        entries = []
        
        # Single pass with scandir
        with os.scandir(path) as it:
            for entry in it:
                if not show_hidden and entry.name.startswith('.'):
                    continue
                    
                try:
                    stat = entry.stat(follow_symlinks=False)
                    if entry.is_dir(follow_symlinks=False):
                        result['dirs'].append({
                            'name': entry.name,
                            'path': entry.path,
                            'symlink': entry.is_symlink()
                        })
                    elif entry.name.endswith('.tsv'):
                        result['files'].append({
                            'name': entry.name,
                            'path': entry.path,
                            'size': stat.st_size,
                            'mtime': stat.st_mtime,
                            'symlink': entry.is_symlink()
                        })
                except (OSError, PermissionError) as e:
                    # Include permission errors in output
                    if 'errors' not in result:
                        result['errors'] = []
                    result['errors'].append(f"{entry.name}: {str(e)}")
        
        # Sort
        result['dirs'].sort(key=lambda x: x['name'].lower())
        result['files'].sort(key=lambda x: x['name'].lower())
        
    except Exception as e:
        result['error'] = str(e)
    
    return json.dumps(result)

def validate_configs(filepath, config_dir):
    """Check all configs for matches"""
    result = {
        'current_match': False,
        'matching_configs': [],
        'error': None
    }
    
    try:
        filename = os.path.basename(filepath)
        
        # Load and check all configs in one pass
        for config_file in Path(config_dir).glob('*.json'):
            try:
                with open(config_file) as f:
                    config = json.load(f)
                
                for file_config in config.get('files', []):
                    pattern = file_config.get('file_pattern', '')
                    # Convert to regex and test
                    import re
                    regex_pattern = pattern.replace('{date_range}', r'\d{8}-\d{8}')
                    regex_pattern = regex_pattern.replace('{month}', r'\d{4}-\d{2}')
                    
                    if re.match(f"^{re.escape(regex_pattern)}$", filename):
                        result['matching_configs'].append({
                            'path': str(config_file),
                            'table': file_config.get('table_name'),
                            'pattern': pattern
                        })
                        break
            
            except Exception as e:
                if 'errors' not in result:
                    result['errors'] = []
                result['errors'].append(f"{config_file}: {str(e)}")
    
    except Exception as e:
        result['error'] = str(e)
    
    return json.dumps(result)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['list', 'validate', 'preview'])
    parser.add_argument('--path', required=True)
    parser.add_argument('--config-dir', default='config')
    parser.add_argument('--show-hidden', action='store_true')
    parser.add_argument('--preview-lines', type=int, default=10)
    
    args = parser.parse_args()
    
    if args.command == 'list':
        print(list_directory(args.path, args.show_hidden))
    elif args.command == 'validate':
        print(validate_configs(args.path, args.config_dir))
    elif args.command == 'preview':
        # Preview implementation
        pass
```

Then the bash script becomes much simpler:

```bash
browse_for_tsv_files() {
    local current_dir="${1:-$(pwd)}"
    local helper="python3 tsv_browser_helper.py"
    
    while true; do
        # Get directory listing from Python (single call)
        local listing=$($helper list --path "$current_dir")
        
        # Check for errors
        local error=$(echo "$listing" | jq -r '.error // empty')
        if [[ -n "$error" ]]; then
            dialog --msgbox "Error: $error" 10 50
            return 1
        fi
        
        # Build menu from JSON
        local items=()
        items+=(".." "[Parent]")
        
        # Add directories
        while IFS= read -r dir; do
            items+=("$dir" "[DIR]")
        done < <(echo "$listing" | jq -r '.dirs[].name')
        
        # Add files with size
        while IFS= read -r line; do
            local name=$(echo "$line" | jq -r '.name')
            local size=$(echo "$line" | jq -r '.size')
            local size_fmt=$(numfmt --to=iec-i --suffix=B "$size" 2>/dev/null || echo "$size")
            items+=("$name" "[$size_fmt]")
        done < <(echo "$listing" | jq -c '.files[]')
        
        # Show dialog
        local choice
        choice=$(dialog --title "TSV Browser" \
                       --menu "$current_dir" \
                       25 80 18 \
                       "${items[@]}" \
                       2>&1 >/dev/tty) || return 1
        
        # Handle choice
        case "$choice" in
            "..")
                current_dir=$(dirname "$current_dir")
                ;;
            *.tsv)
                echo "$current_dir/$choice"
                return 0
                ;;
            *)
                current_dir="$current_dir/$choice"
                ;;
        esac
    done
}
```

## Key Improvements in V2

1. **Performance**
   - Single directory scan with `os.scandir()`
   - Config files loaded once, patterns pre-compiled
   - Directory contents cached
   - Efficient sorting and filtering

2. **Safety**
   - Proper handling of special characters
   - No silent error suppression
   - Explicit symlink handling
   - Path validation and sanitization

3. **Usability**
   - Real-time search/filter
   - Multi-file selection
   - Preview before selection
   - Multiple sort options
   - Breadcrumb navigation

4. **Scalability**
   - Pagination for large directories
   - Lazy loading of items
   - Memory-efficient caching
   - Background pre-fetching option

## Implementation Timeline (Realistic)

### Phase 1: Core Infrastructure (1-2 days)
- Python browser with basic navigation
- Config loading and caching system
- Error handling framework

### Phase 2: Features (1-2 days)
- Search/filter implementation
- Multi-select support
- Preview functionality
- Sort options

### Phase 3: Integration (1 day)
- Bash wrapper integration
- Config validation flow
- Job system integration

### Phase 4: Testing & Optimization (1-2 days)
- Performance testing with 10K+ files
- Special character testing
- Error scenario testing
- User acceptance testing

Total: 4-7 days for production-ready implementation

## Success Metrics
- Handle 10,000+ files without lag
- Sub-second response for all operations
- Zero crashes from special characters
- 100% config validation accuracy
- User satisfaction scores >4/5