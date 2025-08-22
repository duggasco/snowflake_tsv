# Interactive File Browser Implementation Plan
*Created: 2025-01-22*

## Overview
Enhance the Snowflake ETL pipeline's Load Data functionality to support interactive file browsing with automatic config validation and suggestion.

## Requirements
1. **Interactive Directory Navigation**
   - Browse directories visually
   - Navigate with arrow keys and Enter
   - Use ".." to move up directories
   - Show current path context
   - Distinguish directories from files

2. **Config Validation**
   - Validate selected files against current config
   - Automatically suggest matching configs if current doesn't match
   - Allow config switching inline
   - Generate new config if no matches found

3. **User Experience**
   - Maintain backward compatibility with base path option
   - Remember last browsed directory
   - Show file details (size, date modified)
   - Preview capability for file headers

## Technical Architecture

### 1. File Browser Component

#### Implementation Options
**Option A: Dialog/Whiptail** (Recommended)
```bash
browse_for_tsv_files() {
    local current_dir="${1:-$(pwd)}"
    local selected_file=""
    local last_dir_file="/tmp/.tsv_loader_last_dir"
    
    # Load last directory if exists
    [[ -f "$last_dir_file" ]] && current_dir=$(cat "$last_dir_file")
    
    while true; do
        # Build menu items array
        local items=()
        
        # Add parent directory option
        items+=(".." "[Parent Directory]")
        
        # Add directories (sorted)
        while IFS= read -r dir; do
            [[ -d "$dir" ]] && items+=("$(basename "$dir")/" "[DIR]")
        done < <(find "$current_dir" -maxdepth 1 -type d ! -path "$current_dir" | sort)
        
        # Add TSV files with size info
        while IFS= read -r file; do
            if [[ -f "$file" ]]; then
                local size=$(du -h "$file" | cut -f1)
                local name=$(basename "$file")
                items+=("$name" "[$size]")
            fi
        done < <(find "$current_dir" -maxdepth 1 -name "*.tsv" -type f | sort)
        
        # Show dialog menu
        local choice
        choice=$(dialog --title "TSV File Browser" \
                       --menu "Current: ${current_dir##*/}" \
                       25 80 18 \
                       "${items[@]}" \
                       2>&1 >/dev/tty) || return 1
        
        # Handle selection
        case "$choice" in
            "..")
                current_dir="$(dirname "$current_dir")"
                ;;
            */)
                current_dir="$current_dir/${choice%/}"
                ;;
            *.tsv)
                selected_file="$current_dir/$choice"
                echo "$current_dir" > "$last_dir_file"
                echo "$selected_file"
                return 0
                ;;
        esac
    done
}
```

**Option B: Pure Bash with Select** (Fallback)
```bash
browse_for_tsv_files_pure() {
    local current_dir="${1:-$(pwd)}"
    
    while true; do
        echo -e "\n${BLUE}Current Directory: $current_dir${NC}"
        echo "----------------------------------------"
        
        local options=()
        options+=("..")
        
        # Add directories
        for item in "$current_dir"/*; do
            [[ -d "$item" ]] && options+=("$(basename "$item")/")
        done
        
        # Add TSV files
        for item in "$current_dir"/*.tsv; do
            [[ -f "$item" ]] && options+=("$(basename "$item")")
        done
        
        PS3="Select file or directory: "
        select choice in "${options[@]}" "Cancel"; do
            case "$choice" in
                "..")
                    current_dir="$(dirname "$current_dir")"
                    break
                    ;;
                */)
                    current_dir="$current_dir/${choice%/}"
                    break
                    ;;
                *.tsv)
                    echo "$current_dir/$choice"
                    return 0
                    ;;
                "Cancel")
                    return 1
                    ;;
            esac
        done
    done
}
```

### 2. Config Pattern Matching System

#### Pattern Extractor
```python
# extract_patterns.py
import json
import re
import sys

def pattern_to_regex(pattern):
    """Convert file pattern to regex for matching"""
    # Escape special regex characters except our placeholders
    pattern = re.escape(pattern)
    # Replace our placeholders with regex
    pattern = pattern.replace(r'\{date_range\}', r'(\d{8}-\d{8})')
    pattern = pattern.replace(r'\{month\}', r'(\d{4}-\d{2})')
    return f"^{pattern}$"

def extract_table_from_filename(filename, pattern):
    """Extract table name from filename based on pattern"""
    # Remove date components to get base name
    base = re.sub(r'_\d{8}-\d{8}', '', filename)
    base = re.sub(r'_\d{4}-\d{2}', '', base)
    base = re.sub(r'\.tsv$', '', base)
    return base.upper()

def check_file_matches_config(filepath, config_path):
    """Check if file matches any pattern in config"""
    filename = os.path.basename(filepath)
    
    with open(config_path) as f:
        config = json.load(f)
    
    for file_config in config.get('files', []):
        pattern = file_config.get('file_pattern', '')
        regex = pattern_to_regex(pattern)
        if re.match(regex, filename):
            return {
                'matches': True,
                'pattern': pattern,
                'table': file_config.get('table_name'),
                'date_column': file_config.get('date_column')
            }
    
    return {'matches': False}
```

#### Bash Integration
```bash
find_matching_configs() {
    local file_path="$1"
    local filename=$(basename "$file_path")
    local matching_configs=""
    
    for config_file in config/*.json; do
        [[ -f "$config_file" ]] || continue
        
        # Use Python helper to check match
        local result=$(python3 -c "
import json, re, os, sys
sys.path.insert(0, '.')
from extract_patterns import check_file_matches_config
result = check_file_matches_config('$file_path', '$config_file')
if result['matches']:
    print('$config_file')
        " 2>/dev/null)
        
        [[ -n "$result" ]] && matching_configs+="$result "
    done
    
    echo "$matching_configs"
}
```

### 3. Config Validation and Suggestion Flow

```bash
validate_and_suggest_config() {
    local selected_file="$1"
    local current_config="${CONFIG_FILE}"
    local filename=$(basename "$selected_file")
    
    # Check current config first
    local matches=$(python3 -c "
from extract_patterns import check_file_matches_config
result = check_file_matches_config('$selected_file', '$current_config')
print('yes' if result['matches'] else 'no')
    ")
    
    if [[ "$matches" == "yes" ]]; then
        echo -e "${GREEN}File matches current config${NC}"
        return 0
    fi
    
    # Find alternative configs
    echo -e "${YELLOW}File doesn't match current config. Searching alternatives...${NC}"
    local matching_configs=($(find_matching_configs "$selected_file"))
    
    if [[ ${#matching_configs[@]} -gt 0 ]]; then
        echo -e "${BLUE}Found ${#matching_configs[@]} matching config(s):${NC}"
        
        # Build menu
        local config_options=()
        for cfg in "${matching_configs[@]}"; do
            local cfg_name=$(basename "$cfg" .json)
            local table_info=$(python3 -c "
import json
with open('$cfg') as f:
    config = json.load(f)
    tables = [f['table_name'] for f in config.get('files', [])]
    print(', '.join(set(tables[:3])))
            ")
            config_options+=("$cfg" "$cfg_name - Tables: $table_info")
        done
        config_options+=("current" "Keep current config (may fail)")
        config_options+=("generate" "Generate new config for this file")
        
        local choice=$(dialog --title "Config Selection" \
                            --menu "Select configuration for $filename" \
                            20 70 10 \
                            "${config_options[@]}" \
                            2>&1 >/dev/tty)
        
        case "$choice" in
            current)
                echo -e "${YELLOW}Proceeding with current config (validation may fail)${NC}"
                return 0
                ;;
            generate)
                generate_config_for_file "$selected_file"
                ;;
            *)
                export CONFIG_FILE="$choice"
                echo -e "${GREEN}Switched to config: $(basename "$choice")${NC}"
                return 0
                ;;
        esac
    else
        # No matching configs found
        dialog --title "No Matching Config" \
               --menu "No matching configuration found for $filename" \
               15 60 5 \
               "generate" "Generate new config" \
               "current" "Use current config anyway" \
               "browse" "Browse for different file" \
               "cancel" "Cancel operation" \
               2>&1 >/dev/tty
    fi
}
```

### 4. Integration with Existing Menu

```bash
menu_load_data() {
    while true; do
        local choice=$(dialog --title "Load Data" \
                            --menu "Select data loading method:" \
                            15 60 4 \
                            "1" "Specify base path and month" \
                            "2" "Browse for TSV files" \
                            "3" "Recent files" \
                            "4" "Back to main menu" \
                            2>&1 >/dev/tty)
        
        case $choice in
            1)
                # Existing base path flow
                load_with_base_path
                ;;
            2)
                # New interactive browser
                local selected_file=$(browse_for_tsv_files)
                if [[ -n "$selected_file" ]]; then
                    # Validate config
                    if validate_and_suggest_config "$selected_file"; then
                        # Extract month from filename if needed
                        local month=$(extract_month_from_file "$selected_file")
                        # Process file
                        process_direct_file "$selected_file" "$month"
                    fi
                fi
                ;;
            3)
                # Show recently used files
                show_recent_files
                ;;
            4)
                return
                ;;
        esac
    done
}
```

### 5. Additional Features

#### File Preview
```bash
preview_tsv_file() {
    local file="$1"
    local preview=$(head -n 5 "$file" | column -t -s $'\t' | head -c 1000)
    local row_count=$(wc -l < "$file")
    local file_size=$(du -h "$file" | cut -f1)
    
    dialog --title "File Preview: $(basename "$file")" \
           --msgbox "Size: $file_size | Rows: $row_count\n\n$preview" \
           20 80
}
```

#### Recent Files Tracking
```bash
RECENT_FILES_LOG="$HOME/.tsv_loader_recent"

add_to_recent() {
    local file="$1"
    # Add to top of recent files (max 10)
    echo "$file" | cat - "$RECENT_FILES_LOG" 2>/dev/null | \
        head -n 10 | uniq > "$RECENT_FILES_LOG.tmp"
    mv "$RECENT_FILES_LOG.tmp" "$RECENT_FILES_LOG"
}

show_recent_files() {
    [[ -f "$RECENT_FILES_LOG" ]] || return
    
    local items=()
    while IFS= read -r file; do
        if [[ -f "$file" ]]; then
            local size=$(du -h "$file" | cut -f1)
            items+=("$file" "[$size]")
        fi
    done < "$RECENT_FILES_LOG"
    
    dialog --title "Recent Files" \
           --menu "Select a recent file:" \
           20 80 10 \
           "${items[@]}" \
           2>&1 >/dev/tty
}
```

## Implementation Phases

### Phase 1: Core File Browser (2 hours)
1. Implement dialog-based file browser
2. Add navigation logic and controls
3. Test with various directory structures
4. Add fallback pure bash implementation

### Phase 2: Config Validation (2 hours)
1. Create Python pattern matching module
2. Implement config validation logic
3. Add config suggestion system
4. Test with multiple config scenarios

### Phase 3: Integration (1 hour)
1. Integrate with main menu system
2. Update job creation to use selected files
3. Test end-to-end flow
4. Update documentation

### Phase 4: Polish (1 hour)
1. Add file preview capability
2. Implement recent files tracking
3. Add keyboard shortcuts
4. Performance optimization for large directories

## Benefits
1. **User-Friendly**: Visual navigation instead of typing paths
2. **Error Prevention**: Automatic config validation
3. **Flexibility**: Support multiple workflows
4. **Efficiency**: Remember recent selections
5. **Safety**: Validate before processing

## Risks and Mitigations
1. **Large Directories**: Implement pagination for 1000+ files
2. **Permission Issues**: Graceful error handling with clear messages
3. **Dialog Dependency**: Fallback to pure bash if not available
4. **Config Complexity**: Cache pattern compilations for performance

## Testing Plan
1. Test with various directory structures
2. Test config matching with edge cases
3. Test navigation with symlinks
4. Performance test with 10,000+ files
5. Test fallback mechanisms

## Success Criteria
- Users can navigate and select files intuitively
- Config mismatches detected 100% of the time
- Appropriate configs suggested accurately
- No performance degradation for normal use
- Maintains backward compatibility