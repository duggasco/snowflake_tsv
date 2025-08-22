#!/usr/bin/env python3
"""
Interactive TSV File Browser with Config Validation
Efficient implementation for browsing and selecting TSV files with automatic config matching
"""

import os
import json
import curses
import re
import sys
import time
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
from dataclasses import dataclass, field
from functools import lru_cache
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='/tmp/tsv_browser.log'
)
logger = logging.getLogger(__name__)

@dataclass
class FileItem:
    """Represents a file or directory in the browser"""
    path: Path
    name: str
    is_dir: bool
    is_symlink: bool
    size: int
    mtime: float
    
    def display_name(self) -> str:
        """Get display name with type indicators"""
        if self.is_symlink:
            try:
                target = os.readlink(self.path)
                return f"{self.name} -> {target}"
            except:
                return f"{self.name} -> [broken]"
        elif self.is_dir:
            return f"{self.name}/"
        return self.name
    
    def display_size(self) -> str:
        """Format size for display"""
        if self.is_dir:
            return "[DIR]"
        
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        size = float(self.size)
        for unit in units[:-1]:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}{units[-1]}"
    
    def display_time(self) -> str:
        """Format modification time for display"""
        try:
            dt = datetime.fromtimestamp(self.mtime)
            # If file is from today, show time
            if dt.date() == datetime.now().date():
                return dt.strftime("%H:%M")
            # If from this year, show month and day
            elif dt.year == datetime.now().year:
                return dt.strftime("%b %d")
            # Otherwise show year
            else:
                return dt.strftime("%Y")
        except:
            return "----"


@dataclass
class ConfigMatch:
    """Represents a config file match for a TSV file"""
    config_path: str
    table_name: str
    pattern: str
    date_column: str
    confidence: float = 1.0  # For future fuzzy matching


class ConfigMatcher:
    """Efficient config matching with caching and pre-compilation"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.configs: Dict[str, dict] = {}
        self.patterns: Dict[str, List[Tuple[re.Pattern, dict]]] = {}
        self.load_time = 0
        self._load_all_configs()
    
    def _load_all_configs(self):
        """Load all configs once at startup"""
        start_time = time.time()
        
        if not self.config_dir.exists():
            logger.warning(f"Config directory {self.config_dir} does not exist")
            return
        
        for config_file in self.config_dir.glob("*.json"):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    self.configs[str(config_file)] = config
                    
                    # Pre-compile patterns for this config
                    patterns = []
                    for file_config in config.get('files', []):
                        pattern_str = file_config.get('file_pattern', '')
                        if pattern_str:
                            try:
                                regex = self._pattern_to_regex(pattern_str)
                                compiled = re.compile(regex)
                                patterns.append((compiled, file_config))
                            except re.error as e:
                                logger.error(f"Invalid pattern {pattern_str}: {e}")
                    
                    self.patterns[str(config_file)] = patterns
                    logger.info(f"Loaded config {config_file} with {len(patterns)} patterns")
                    
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {config_file}: {e}")
            except Exception as e:
                logger.error(f"Error loading {config_file}: {e}")
        
        self.load_time = time.time() - start_time
        logger.info(f"Loaded {len(self.configs)} configs in {self.load_time:.2f}s")
    
    @lru_cache(maxsize=256)
    def _pattern_to_regex(self, pattern: str) -> str:
        """Convert file pattern to regex with caching"""
        # First, escape special regex characters except our placeholders
        # We need to be careful not to escape the placeholders themselves
        
        # Temporarily replace placeholders with unique markers
        marker1 = "<<<DATE_RANGE_MARKER>>>"
        marker2 = "<<<MONTH_MARKER>>>"
        
        pattern = pattern.replace("{date_range}", marker1)
        pattern = pattern.replace("{month}", marker2)
        
        # Now escape the pattern
        pattern = re.escape(pattern)
        
        # Replace markers with actual regex
        pattern = pattern.replace(marker1, r'(\d{8}-\d{8})')
        pattern = pattern.replace(marker2, r'(\d{4}-\d{2})')
        
        return f"^{pattern}$"
    
    def find_matching_configs(self, filepath: str) -> List[ConfigMatch]:
        """Find all configs matching the given file"""
        filename = os.path.basename(filepath)
        matches = []
        
        for config_path, pattern_list in self.patterns.items():
            for regex, file_config in pattern_list:
                if regex.match(filename):
                    match = ConfigMatch(
                        config_path=config_path,
                        table_name=file_config.get('table_name', ''),
                        pattern=file_config.get('file_pattern', ''),
                        date_column=file_config.get('date_column', '')
                    )
                    matches.append(match)
                    break  # Only one match per config file
        
        return matches
    
    def get_config_details(self, config_path: str) -> dict:
        """Get full config details for a given path"""
        return self.configs.get(config_path, {})


class TSVFileBrowser:
    """Main file browser with efficient scanning and navigation"""
    
    def __init__(self, start_dir: str = ".", config_dir: str = "config"):
        self.current_dir = Path(start_dir).resolve()
        self.config_matcher = ConfigMatcher(config_dir)
        self.selected_files: List[Path] = []
        self.filter_text = ""
        self.sort_by = "name"  # name, size, date, type
        self.show_hidden = False
        self.preview_lines = 20
        self.reverse_sort = False
        
        # Performance: cache directory contents
        self.dir_cache: Dict[str, Tuple[float, List[FileItem]]] = {}
        self.cache_ttl = 60  # Cache for 60 seconds
        self.cache_size_limit = 100
        
        # State for UI
        self.current_index = 0
        self.scroll_offset = 0
        self.message = ""
        self.message_time = 0
        
        # Recent directories for quick navigation
        self.recent_dirs: List[Path] = []
        self.max_recent = 10
    
    def _get_directory_contents(self, path: Path) -> List[FileItem]:
        """Efficiently read directory with caching"""
        cache_key = str(path)
        current_time = time.time()
        
        # Check cache (with TTL)
        if cache_key in self.dir_cache:
            cache_time, cached_items = self.dir_cache[cache_key]
            if current_time - cache_time < self.cache_ttl:
                logger.debug(f"Using cached contents for {path}")
                return cached_items
        
        items = []
        errors = []
        
        try:
            # Use scandir for efficiency - single system call
            with os.scandir(path) as entries:
                for entry in entries:
                    try:
                        # Skip hidden files unless requested
                        if not self.show_hidden and entry.name.startswith('.'):
                            continue
                        
                        # For TSV browser, only show directories and TSV files
                        is_dir = entry.is_dir(follow_symlinks=False)
                        is_tsv = entry.name.lower().endswith('.tsv')
                        
                        if is_dir or is_tsv:
                            stat = entry.stat(follow_symlinks=False)
                            items.append(FileItem(
                                path=Path(entry.path),
                                name=entry.name,
                                is_dir=is_dir,
                                is_symlink=entry.is_symlink(),
                                size=stat.st_size if not is_dir else 0,
                                mtime=stat.st_mtime
                            ))
                    except (OSError, PermissionError) as e:
                        errors.append(f"{entry.name}: {str(e)}")
                        logger.warning(f"Cannot access {entry.path}: {e}")
        
        except (OSError, PermissionError) as e:
            logger.error(f"Cannot read directory {path}: {e}")
            self.message = f"Error: {str(e)}"
            self.message_time = time.time()
            return []
        
        # Log any errors encountered
        if errors:
            logger.info(f"Encountered {len(errors)} errors reading {path}")
        
        # Sort items
        items = self._sort_items(items)
        
        # Update cache (with cache size management)
        if len(self.dir_cache) >= self.cache_size_limit:
            # Remove oldest cache entry
            oldest_key = min(self.dir_cache.keys(), 
                           key=lambda k: self.dir_cache[k][0])
            del self.dir_cache[oldest_key]
        
        self.dir_cache[cache_key] = (current_time, items)
        logger.info(f"Cached {len(items)} items for {path}")
        
        return items
    
    def _sort_items(self, items: List[FileItem]) -> List[FileItem]:
        """Sort items based on current sort setting"""
        # Always separate directories and files
        dirs = [i for i in items if i.is_dir]
        files = [i for i in items if not i.is_dir]
        
        # Define sort key functions
        sort_keys = {
            "name": lambda x: x.name.lower(),
            "size": lambda x: x.size,
            "date": lambda x: x.mtime,
            "type": lambda x: (os.path.splitext(x.name)[1].lower(), x.name.lower())
        }
        
        key_func = sort_keys.get(self.sort_by, sort_keys["name"])
        
        # Sort with reverse option
        dirs.sort(key=key_func, reverse=self.reverse_sort)
        files.sort(key=key_func, reverse=self.reverse_sort)
        
        return dirs + files
    
    def _apply_filter(self, items: List[FileItem]) -> List[FileItem]:
        """Apply search filter to items"""
        if not self.filter_text:
            # Return a copy to avoid modifying the cached list
            return items.copy()
        
        # Case-insensitive filtering
        filter_lower = self.filter_text.lower()
        
        # Support multiple search terms (space-separated)
        terms = filter_lower.split()
        
        filtered = []
        for item in items:
            name_lower = item.name.lower()
            # All terms must match
            if all(term in name_lower for term in terms):
                filtered.append(item)
        
        return filtered
    
    def invalidate_cache(self, path: Optional[Path] = None):
        """Invalidate cache for a specific path or all paths"""
        if path:
            cache_key = str(path)
            if cache_key in self.dir_cache:
                del self.dir_cache[cache_key]
                logger.debug(f"Invalidated cache for {path}")
        else:
            self.dir_cache.clear()
            logger.debug("Cleared entire directory cache")
    
    def preview_file(self, filepath: Path) -> List[str]:
        """Generate preview of TSV file"""
        lines = []
        
        try:
            # Get file stats first
            stat = filepath.stat()
            lines.append(f"File: {filepath.name}")
            lines.append(f"Size: {stat.st_size:,} bytes")
            lines.append(f"Modified: {datetime.fromtimestamp(stat.st_mtime)}")
            
            # Try to count actual lines (fast method)
            with open(filepath, 'rb') as f:
                line_count = sum(1 for _ in f)
            lines.append(f"Lines: {line_count:,}")
            
            # Check config matches
            matches = self.config_matcher.find_matching_configs(str(filepath))
            if matches:
                lines.append(f"Matching configs: {len(matches)}")
                for match in matches[:3]:  # Show first 3 matches
                    config_name = os.path.basename(match.config_path)
                    lines.append(f"  - {config_name}: {match.table_name}")
            else:
                lines.append("No matching configs found")
            
            lines.append("-" * 50)
            
            # Show file content preview
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                for i, line in enumerate(f):
                    if i >= self.preview_lines:
                        lines.append(f"... ({line_count - i} more lines)")
                        break
                    
                    # Truncate long lines
                    if len(line) > 200:
                        line = line[:197] + "..."
                    lines.append(line.rstrip())
            
        except Exception as e:
            lines.append(f"Error reading file: {e}")
            logger.error(f"Error previewing {filepath}: {e}")
        
        return lines
    
    def add_to_recent(self, path: Path):
        """Add directory to recent list"""
        if path in self.recent_dirs:
            self.recent_dirs.remove(path)
        self.recent_dirs.insert(0, path)
        if len(self.recent_dirs) > self.max_recent:
            self.recent_dirs.pop()
    
    def get_selected_files(self) -> List[str]:
        """Get list of selected file paths as strings"""
        return [str(f) for f in self.selected_files]


class CursesUI:
    """Curses-based UI for the file browser"""
    
    def __init__(self, browser: TSVFileBrowser):
        self.browser = browser
        self.stdscr = None
        self.height = 0
        self.width = 0
        
        # UI state
        self.show_help = False
        self.show_preview = False
        self.preview_content = []
        
        # Colors
        self.colors_initialized = False
    
    def init_colors(self):
        """Initialize color pairs for the UI"""
        if not curses.has_colors():
            return
        
        try:
            curses.start_color()
            curses.use_default_colors()
            
            # Define color pairs
            curses.init_pair(1, curses.COLOR_CYAN, -1)    # Directories
            curses.init_pair(2, curses.COLOR_GREEN, -1)   # Selected
            curses.init_pair(3, curses.COLOR_YELLOW, -1)  # Symlinks
            curses.init_pair(4, curses.COLOR_RED, -1)      # Errors
            curses.init_pair(5, curses.COLOR_MAGENTA, -1) # Headers
            curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_BLUE)  # Highlight
            
            self.colors_initialized = True
        except:
            self.colors_initialized = False
    
    def draw_header(self):
        """Draw the header with current directory and filter"""
        # Line 1: Current directory
        dir_str = str(self.browser.current_dir)
        if len(dir_str) > self.width - 2:
            # Truncate long paths, keeping the end
            dir_str = "..." + dir_str[-(self.width - 5):]
        
        self.stdscr.addstr(0, 0, f"Directory: {dir_str}", 
                          curses.A_BOLD | curses.color_pair(5))
        
        # Line 2: Filter and sort info
        filter_info = f"Filter: {self.browser.filter_text}" if self.browser.filter_text else "No filter"
        sort_info = f"Sort: {self.browser.sort_by}"
        if self.browser.reverse_sort:
            sort_info += " [rev]"
        
        info_line = f"{filter_info} | {sort_info}"
        self.stdscr.addstr(1, 0, info_line[:self.width-1])
        
        # Line 3: Separator
        self.stdscr.addstr(2, 0, "-" * (self.width - 1))
    
    def draw_footer(self):
        """Draw the footer with help text"""
        # Show message if there is one
        if self.browser.message and (time.time() - self.browser.message_time < 3):
            self.stdscr.addstr(self.height - 2, 0, 
                              self.browser.message[:self.width-1],
                              curses.color_pair(4))
        
        # Help line
        if self.show_help:
            help_text = "[q]uit [h]ide help [Enter]select [Space]multi [p]review [/]search [s]ort [r]everse [.]hidden"
        else:
            help_text = "[h]elp [q]uit | Selected: {} files".format(len(self.browser.selected_files))
        
        self.stdscr.addstr(self.height - 1, 0, help_text[:self.width-1], 
                          curses.A_BOLD)
    
    def draw_file_list(self, items: List[FileItem], start_y: int = 3):
        """Draw the main file list"""
        # Calculate visible area
        visible_height = self.height - start_y - 2  # Leave room for footer
        
        # Adjust scroll offset if needed
        if self.browser.current_index < self.browser.scroll_offset:
            self.browser.scroll_offset = self.browser.current_index
        elif self.browser.current_index >= self.browser.scroll_offset + visible_height:
            self.browser.scroll_offset = self.browser.current_index - visible_height + 1
        
        # Draw visible items
        for i in range(visible_height):
            item_idx = self.browser.scroll_offset + i
            if item_idx >= len(items):
                break
            
            item = items[item_idx]
            y = start_y + i
            
            # Determine attributes
            attr = 0
            if item_idx == self.browser.current_index:
                attr |= curses.A_REVERSE
            
            # Color based on type
            if self.colors_initialized:
                if item.is_dir:
                    attr |= curses.color_pair(1)
                elif item.is_symlink:
                    attr |= curses.color_pair(3)
            
            # Selection indicator
            selected = item.path in self.browser.selected_files
            prefix = "[*] " if selected else "[ ] "
            
            # Format the line
            name = item.display_name()
            if len(name) > 40:
                name = name[:37] + "..."
            
            size = item.display_size()
            mtime = item.display_time()
            
            # Build the line
            line = f"{prefix}{name:<40} {size:>10} {mtime:>8}"
            
            # Draw the line (truncate if needed)
            if len(line) > self.width - 1:
                line = line[:self.width - 1]
            
            try:
                self.stdscr.addstr(y, 0, line, attr)
            except curses.error:
                # Ignore errors at screen edge
                pass
        
        # Draw scrollbar if needed
        if len(items) > visible_height:
            self.draw_scrollbar(start_y, visible_height, len(items))
    
    def draw_scrollbar(self, start_y: int, visible_height: int, total_items: int):
        """Draw a simple scrollbar"""
        if total_items == 0:
            return
        
        # Calculate scrollbar position and size
        bar_height = max(1, int(visible_height * visible_height / total_items))
        bar_pos = int(self.browser.scroll_offset * visible_height / total_items)
        
        x = self.width - 1
        
        for i in range(visible_height):
            y = start_y + i
            if i >= bar_pos and i < bar_pos + bar_height:
                char = '█'
            else:
                char = '│'
            
            try:
                self.stdscr.addstr(y, x, char)
            except curses.error:
                pass
    
    def show_preview_window(self):
        """Show file preview in a popup window"""
        if not self.preview_content:
            return
        
        # Calculate window size
        height = min(len(self.preview_content) + 4, self.height - 4)
        width = min(80, self.width - 4)
        
        # Center the window
        y = (self.height - height) // 2
        x = (self.width - width) // 2
        
        # Create window
        win = curses.newwin(height, width, y, x)
        win.box()
        
        # Title
        title = " File Preview "
        win.addstr(0, (width - len(title)) // 2, title)
        
        # Content
        for i, line in enumerate(self.preview_content[:height-3]):
            if len(line) > width - 4:
                line = line[:width-7] + "..."
            try:
                win.addstr(i + 2, 2, line)
            except:
                pass
        
        # Footer
        win.addstr(height - 1, 2, "[Press any key to close]")
        
        win.refresh()
        win.getch()
    
    def get_search_input(self) -> str:
        """Get search input from user"""
        # Clear the message area
        self.stdscr.move(self.height - 2, 0)
        self.stdscr.clrtoeol()
        self.stdscr.addstr(self.height - 2, 0, "Search: ")
        
        curses.echo()
        curses.curs_set(1)  # Show cursor
        
        try:
            search = self.stdscr.getstr(self.height - 2, 8, 50).decode('utf-8')
        except:
            search = self.browser.filter_text  # Keep existing filter on error
        
        curses.noecho()
        curses.curs_set(0)  # Hide cursor
        
        return search
    
    def run(self, stdscr) -> Optional[List[str]]:
        """Main UI loop"""
        self.stdscr = stdscr
        self.height, self.width = stdscr.getmaxyx()
        
        # Initialize
        curses.curs_set(0)  # Hide cursor
        self.init_colors()
        
        # Main loop
        while True:
            # Clear screen
            stdscr.clear()
            
            # Get current directory contents
            items = self.browser._get_directory_contents(self.browser.current_dir)
            
            # Apply filter
            filtered_items = self.browser._apply_filter(items)
            
            # Add parent directory option if not at root
            if self.browser.current_dir.parent != self.browser.current_dir:
                parent_item = FileItem(
                    path=self.browser.current_dir.parent,
                    name="..",
                    is_dir=True,
                    is_symlink=False,
                    size=0,
                    mtime=0
                )
                filtered_items.insert(0, parent_item)
            
            # Ensure current index is valid
            if self.browser.current_index >= len(filtered_items):
                self.browser.current_index = max(0, len(filtered_items) - 1)
            
            # Draw UI components
            self.draw_header()
            self.draw_file_list(filtered_items)
            self.draw_footer()
            
            # Show preview if requested
            if self.show_preview and self.preview_content:
                self.show_preview_window()
                self.show_preview = False
                self.preview_content = []
                continue
            
            # Refresh and get input
            stdscr.refresh()
            
            try:
                key = stdscr.getch()
            except KeyboardInterrupt:
                return None
            
            # Handle input
            if key == ord('q') or key == 27:  # q or ESC
                return None
            
            elif key == ord('h'):  # Toggle help
                self.show_help = not self.show_help
            
            elif key == curses.KEY_UP or key == ord('k'):
                if self.browser.current_index > 0:
                    self.browser.current_index -= 1
            
            elif key == curses.KEY_DOWN or key == ord('j'):
                if self.browser.current_index < len(filtered_items) - 1:
                    self.browser.current_index += 1
            
            elif key == curses.KEY_PPAGE:  # Page up
                self.browser.current_index = max(0, 
                    self.browser.current_index - (self.height - 5))
            
            elif key == curses.KEY_NPAGE:  # Page down
                self.browser.current_index = min(len(filtered_items) - 1,
                    self.browser.current_index + (self.height - 5))
            
            elif key == curses.KEY_HOME:
                self.browser.current_index = 0
            
            elif key == curses.KEY_END:
                self.browser.current_index = len(filtered_items) - 1
            
            elif key == ord('\n') or key == curses.KEY_RIGHT:  # Enter
                if filtered_items and self.browser.current_index < len(filtered_items):
                    item = filtered_items[self.browser.current_index]
                    
                    if item.name == "..":
                        # Go to parent directory
                        self.browser.add_to_recent(self.browser.current_dir)
                        self.browser.current_dir = self.browser.current_dir.parent
                        self.browser.current_index = 0
                        self.browser.scroll_offset = 0
                    elif item.is_dir:
                        # Enter directory
                        self.browser.add_to_recent(self.browser.current_dir)
                        self.browser.current_dir = item.path
                        self.browser.current_index = 0
                        self.browser.scroll_offset = 0
                    else:
                        # File selected - return if we have selections
                        if self.browser.selected_files:
                            # Add current file if not already selected
                            if item.path not in self.browser.selected_files:
                                self.browser.selected_files.append(item.path)
                            return self.browser.get_selected_files()
                        else:
                            # Single file selection
                            return [str(item.path)]
            
            elif key == ord(' '):  # Space - toggle selection
                if filtered_items and self.browser.current_index < len(filtered_items):
                    item = filtered_items[self.browser.current_index]
                    if not item.is_dir and item.name != "..":
                        if item.path in self.browser.selected_files:
                            self.browser.selected_files.remove(item.path)
                        else:
                            self.browser.selected_files.append(item.path)
                        
                        # Move to next item
                        if self.browser.current_index < len(filtered_items) - 1:
                            self.browser.current_index += 1
            
            elif key == ord('a'):  # Select all
                for item in filtered_items:
                    if not item.is_dir and item.name != ".." and item.name.endswith('.tsv'):
                        if item.path not in self.browser.selected_files:
                            self.browser.selected_files.append(item.path)
            
            elif key == ord('A'):  # Deselect all
                self.browser.selected_files.clear()
            
            elif key == ord('p'):  # Preview
                if filtered_items and self.browser.current_index < len(filtered_items):
                    item = filtered_items[self.browser.current_index]
                    if not item.is_dir and item.name != ".." and item.name.endswith('.tsv'):
                        self.preview_content = self.browser.preview_file(item.path)
                        self.show_preview = True
            
            elif key == ord('/'):  # Search
                search = self.get_search_input()
                self.browser.filter_text = search
                self.browser.current_index = 0
                self.browser.scroll_offset = 0
            
            elif key == ord('c'):  # Clear filter
                self.browser.filter_text = ""
                self.browser.current_index = 0
                self.browser.scroll_offset = 0
            
            elif key == ord('s'):  # Cycle sort modes
                sort_modes = ["name", "size", "date", "type"]
                current = sort_modes.index(self.browser.sort_by)
                self.browser.sort_by = sort_modes[(current + 1) % len(sort_modes)]
                self.browser.invalidate_cache(self.browser.current_dir)
            
            elif key == ord('r'):  # Toggle reverse sort
                self.browser.reverse_sort = not self.browser.reverse_sort
                self.browser.invalidate_cache(self.browser.current_dir)
            
            elif key == ord('.'):  # Toggle hidden files
                self.browser.show_hidden = not self.browser.show_hidden
                self.browser.invalidate_cache(self.browser.current_dir)
                self.browser.current_index = 0
                self.browser.scroll_offset = 0
            
            elif key == curses.KEY_LEFT:  # Go back
                if self.browser.current_dir.parent != self.browser.current_dir:
                    self.browser.add_to_recent(self.browser.current_dir)
                    self.browser.current_dir = self.browser.current_dir.parent
                    self.browser.current_index = 0
                    self.browser.scroll_offset = 0
            
            elif key == ord('R'):  # Refresh
                self.browser.invalidate_cache()
                self.browser.message = "Cache cleared"
                self.browser.message_time = time.time()


def main():
    """Main entry point for the file browser"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Interactive TSV File Browser')
    parser.add_argument('--start-dir', default='.', 
                       help='Starting directory (default: current)')
    parser.add_argument('--config-dir', default='config',
                       help='Configuration directory (default: config)')
    parser.add_argument('--output', '-o', 
                       help='Output selected files to this file')
    
    args = parser.parse_args()
    
    # Create browser
    browser = TSVFileBrowser(
        start_dir=args.start_dir,
        config_dir=args.config_dir
    )
    
    # Create and run UI
    ui = CursesUI(browser)
    
    try:
        selected = curses.wrapper(ui.run)
        
        if selected:
            if args.output:
                # Write to file
                with open(args.output, 'w') as f:
                    for filepath in selected:
                        f.write(filepath + '\n')
                print(f"Selected {len(selected)} file(s) written to {args.output}")
            else:
                # Print to stdout
                for filepath in selected:
                    print(filepath)
            return 0
        else:
            print("No files selected", file=sys.stderr)
            return 1
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.exception("Unhandled exception in main")
        return 1


if __name__ == "__main__":
    sys.exit(main())