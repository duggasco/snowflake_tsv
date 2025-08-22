#!/usr/bin/env python3
"""
TSV Browser Integration Helper
Validates selected files against configs and provides suggestions
"""

import json
import sys
import os
from pathlib import Path
from typing import List, Dict, Optional
import argparse
import re


class ConfigValidator:
    """Validates TSV files against configurations"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.configs = {}
        self.patterns = {}
        self._load_configs()
    
    def _load_configs(self):
        """Load all configuration files"""
        if not self.config_dir.exists():
            return
        
        for config_file in self.config_dir.glob("*.json"):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    self.configs[str(config_file)] = config
                    
                    # Extract patterns
                    patterns = []
                    for file_config in config.get('files', []):
                        pattern = file_config.get('file_pattern', '')
                        if pattern:
                            patterns.append({
                                'pattern': pattern,
                                'regex': self._pattern_to_regex(pattern),
                                'table': file_config.get('table_name', ''),
                                'date_column': file_config.get('date_column', '')
                            })
                    self.patterns[str(config_file)] = patterns
            except Exception as e:
                print(f"Error loading {config_file}: {e}", file=sys.stderr)
    
    def _pattern_to_regex(self, pattern: str) -> re.Pattern:
        """Convert file pattern to compiled regex"""
        # Replace placeholders with regex
        regex_str = re.escape(pattern)
        regex_str = regex_str.replace(r'\{date_range\}', r'(\d{8}-\d{8})')
        regex_str = regex_str.replace(r'\{month\}', r'(\d{4}-\d{2})')
        return re.compile(f"^{regex_str}$")
    
    def validate_file(self, filepath: str, config_path: str) -> Dict:
        """Check if a file matches the specified config"""
        filename = os.path.basename(filepath)
        result = {
            'file': filepath,
            'filename': filename,
            'matches': False,
            'config': config_path,
            'pattern': None,
            'table': None,
            'date_info': None
        }
        
        if config_path not in self.patterns:
            result['error'] = f"Config not found: {config_path}"
            return result
        
        for pattern_info in self.patterns[config_path]:
            match = pattern_info['regex'].match(filename)
            if match:
                result['matches'] = True
                result['pattern'] = pattern_info['pattern']
                result['table'] = pattern_info['table']
                
                # Extract date information from match groups
                if '{date_range}' in pattern_info['pattern'] and match.groups():
                    date_range = match.group(1)
                    dates = date_range.split('-')
                    if len(dates) == 2:
                        result['date_info'] = {
                            'type': 'range',
                            'start': dates[0],
                            'end': dates[1]
                        }
                elif '{month}' in pattern_info['pattern'] and match.groups():
                    result['date_info'] = {
                        'type': 'month',
                        'month': match.group(1)
                    }
                break
        
        return result
    
    def find_matching_configs(self, filepath: str) -> List[Dict]:
        """Find all configs that match the given file"""
        filename = os.path.basename(filepath)
        matches = []
        
        for config_path, patterns in self.patterns.items():
            for pattern_info in patterns:
                if pattern_info['regex'].match(filename):
                    config_name = os.path.basename(config_path)
                    # Get table list from config
                    tables = set()
                    for f in self.configs[config_path].get('files', []):
                        if t := f.get('table_name'):
                            tables.add(t)
                    
                    matches.append({
                        'config_path': config_path,
                        'config_name': config_name,
                        'pattern': pattern_info['pattern'],
                        'table': pattern_info['table'],
                        'tables': list(tables)[:3]  # Show first 3 tables
                    })
                    break
        
        return matches
    
    def generate_config_skeleton(self, filepath: str) -> Dict:
        """Generate a config skeleton for a file that doesn't match any config"""
        filename = os.path.basename(filepath)
        
        # Try to extract pattern from filename
        # Look for date patterns
        date_range_pattern = r'(\d{8}-\d{8})'
        month_pattern = r'(\d{4}-\d{2})'
        
        if re.search(date_range_pattern, filename):
            pattern = re.sub(date_range_pattern, '{date_range}', filename)
            date_type = 'range'
        elif re.search(month_pattern, filename):
            pattern = re.sub(month_pattern, '{month}', filename)
            date_type = 'month'
        else:
            pattern = filename
            date_type = 'unknown'
        
        # Extract potential table name
        table_name = filename.replace('.tsv', '').upper()
        table_name = re.sub(r'[_-]?\d{8}-\d{8}', '', table_name)
        table_name = re.sub(r'[_-]?\d{4}-\d{2}', '', table_name)
        
        return {
            'file_pattern': pattern,
            'table_name': table_name,
            'date_column': 'recordDate',
            'date_type': date_type,
            'needs_columns': True
        }


def validate_files(files: List[str], current_config: str, config_dir: str) -> Dict:
    """Validate multiple files and provide suggestions"""
    validator = ConfigValidator(config_dir)
    
    results = {
        'current_config': current_config,
        'files': [],
        'all_match_current': True,
        'suggestions': [],
        'need_generation': []
    }
    
    for filepath in files:
        # Check against current config
        validation = validator.validate_file(filepath, current_config)
        results['files'].append(validation)
        
        if not validation['matches']:
            results['all_match_current'] = False
            
            # Find alternative configs
            matches = validator.find_matching_configs(filepath)
            if matches:
                for match in matches:
                    if match['config_path'] not in [s['config_path'] for s in results['suggestions']]:
                        results['suggestions'].append(match)
            else:
                # No matches - suggest generation
                skeleton = validator.generate_config_skeleton(filepath)
                skeleton['file'] = filepath
                results['need_generation'].append(skeleton)
    
    return results


def main():
    """Main entry point for validation"""
    parser = argparse.ArgumentParser(description='Validate TSV files against configs')
    parser.add_argument('files', nargs='+', help='TSV files to validate')
    parser.add_argument('--current-config', required=True, 
                       help='Current configuration file')
    parser.add_argument('--config-dir', default='config',
                       help='Configuration directory')
    parser.add_argument('--json', action='store_true',
                       help='Output as JSON')
    
    args = parser.parse_args()
    
    # Validate files
    results = validate_files(
        files=args.files,
        current_config=args.current_config,
        config_dir=args.config_dir
    )
    
    if args.json:
        # Output as JSON for script consumption
        print(json.dumps(results, indent=2))
    else:
        # Human-readable output
        if results['all_match_current']:
            print(f"✓ All {len(results['files'])} file(s) match current config")
        else:
            matching = sum(1 for f in results['files'] if f['matches'])
            print(f"⚠ Only {matching}/{len(results['files'])} files match current config")
            
            if results['suggestions']:
                print("\nSuggested configs that match your files:")
                for i, sugg in enumerate(results['suggestions'], 1):
                    tables = ', '.join(sugg['tables']) if sugg['tables'] else 'No tables'
                    print(f"  {i}. {sugg['config_name']} - Tables: {tables}")
            
            if results['need_generation']:
                print(f"\n{len(results['need_generation'])} file(s) need new config generation")
    
    return 0 if results['all_match_current'] else 1


if __name__ == "__main__":
    sys.exit(main())