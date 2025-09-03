#!/usr/bin/env python3
"""
Standalone TSV compression utility for cross-environment file transfer.
Compresses TSV files using gzip with configurable compression levels.
"""

import argparse
import gzip
import os
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

def format_size(bytes_size: int) -> str:
    """Format bytes into human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def compress_file(
    input_path: str,
    output_path: Optional[str] = None,
    compression_level: int = 6,
    chunk_size: int = 10 * 1024 * 1024,  # 10MB chunks
    show_progress: bool = True
) -> Tuple[bool, str, dict]:
    """
    Compress a TSV file using gzip.
    
    Args:
        input_path: Path to input TSV file
        output_path: Path for output compressed file (default: input_path + .gz)
        compression_level: Gzip compression level (1-9, default 6)
        chunk_size: Size of chunks to read/write (default 10MB)
        show_progress: Whether to show progress bar
        
    Returns:
        Tuple of (success, message, stats_dict)
    """
    try:
        # Validate input file
        if not os.path.exists(input_path):
            return False, f"Input file not found: {input_path}", {}
            
        if not os.path.isfile(input_path):
            return False, f"Input path is not a file: {input_path}", {}
            
        # Set output path if not provided
        if output_path is None:
            output_path = f"{input_path}.gz"
            
        # Check if output already exists
        if os.path.exists(output_path):
            response = input(f"Output file {output_path} already exists. Overwrite? (y/n): ")
            if response.lower() != 'y':
                return False, "Compression cancelled by user", {}
                
        # Get file stats
        file_size = os.path.getsize(input_path)
        start_time = time.time()
        bytes_read = 0
        
        # Perform compression
        with open(input_path, 'rb') as f_in:
            with gzip.open(output_path, 'wb', compresslevel=compression_level) as f_out:
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                        
                    f_out.write(chunk)
                    bytes_read += len(chunk)
                    
                    if show_progress:
                        progress = (bytes_read / file_size) * 100
                        elapsed = time.time() - start_time
                        speed = bytes_read / elapsed if elapsed > 0 else 0
                        eta = (file_size - bytes_read) / speed if speed > 0 else 0
                        
                        print(f'\rProgress: {progress:.1f}% | '
                              f'Speed: {format_size(int(speed))}/s | '
                              f'ETA: {int(eta)}s', end='', file=sys.stderr)
                              
        if show_progress:
            print('\rProgress: 100.0% - Complete!                                    ', file=sys.stderr)
            
        # Calculate compression stats
        end_time = time.time()
        compressed_size = os.path.getsize(output_path)
        compression_ratio = (1 - compressed_size / file_size) * 100
        elapsed_time = end_time - start_time
        compression_speed = file_size / elapsed_time if elapsed_time > 0 else 0
        
        stats = {
            'original_size': file_size,
            'compressed_size': compressed_size,
            'compression_ratio': compression_ratio,
            'compression_time': elapsed_time,
            'compression_speed': compression_speed,
            'input_file': input_path,
            'output_file': output_path,
            'compression_level': compression_level
        }
        
        message = (f"Compression completed successfully!\n"
                  f"  Original: {format_size(file_size)}\n"
                  f"  Compressed: {format_size(compressed_size)}\n"
                  f"  Ratio: {compression_ratio:.1f}% reduction\n"
                  f"  Time: {elapsed_time:.1f}s\n"
                  f"  Speed: {format_size(int(compression_speed))}/s\n"
                  f"  Output: {output_path}")
                  
        return True, message, stats
        
    except PermissionError as e:
        return False, f"Permission denied: {e}", {}
    except IOError as e:
        return False, f"I/O error: {e}", {}
    except Exception as e:
        # Clean up partial file on error
        if output_path and os.path.exists(output_path):
            try:
                os.remove(output_path)
            except:
                pass
        return False, f"Compression failed: {e}", {}

def compress_multiple_files(
    file_patterns: list,
    output_dir: Optional[str] = None,
    compression_level: int = 6,
    show_progress: bool = True
) -> Tuple[int, int, list]:
    """
    Compress multiple TSV files.
    
    Args:
        file_patterns: List of file paths or glob patterns
        output_dir: Output directory for compressed files
        compression_level: Gzip compression level (1-9)
        show_progress: Whether to show progress
        
    Returns:
        Tuple of (success_count, failure_count, results_list)
    """
    from glob import glob
    
    # Expand patterns to actual files
    all_files = []
    for pattern in file_patterns:
        if '*' in pattern or '?' in pattern:
            all_files.extend(glob(pattern))
        else:
            all_files.append(pattern)
            
    # Remove duplicates while preserving order
    all_files = list(dict.fromkeys(all_files))
    
    if not all_files:
        print("No files found matching the specified patterns", file=sys.stderr)
        return 0, 0, []
        
    print(f"Found {len(all_files)} file(s) to compress", file=sys.stderr)
    
    success_count = 0
    failure_count = 0
    results = []
    
    for i, file_path in enumerate(all_files, 1):
        print(f"\n[{i}/{len(all_files)}] Processing: {file_path}", file=sys.stderr)
        
        # Determine output path
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, os.path.basename(file_path) + '.gz')
        else:
            output_path = None
            
        success, message, stats = compress_file(
            file_path, 
            output_path, 
            compression_level, 
            show_progress=show_progress
        )
        
        if success:
            success_count += 1
            print(f"[SUCCESS] {message}")
        else:
            failure_count += 1
            print(f"[FAILED] {message}", file=sys.stderr)
            
        results.append({
            'file': file_path,
            'success': success,
            'message': message,
            'stats': stats
        })
        
    return success_count, failure_count, results

def main():
    """Main entry point for CLI usage."""
    parser = argparse.ArgumentParser(
        description='Compress TSV files for cross-environment transfer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compress single file with default settings
  %(prog)s data/file.tsv
  
  # Compress with specific output path and compression level
  %(prog)s data/file.tsv -o compressed/file.tsv.gz -l 9
  
  # Compress multiple files to specific directory
  %(prog)s data/*.tsv -d compressed/ -l 7
  
  # Compress without progress display (for scripts)
  %(prog)s data/file.tsv --no-progress
        """
    )
    
    parser.add_argument(
        'files',
        nargs='+',
        help='TSV file(s) to compress (supports wildcards)'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Output file path (for single file only)'
    )
    
    parser.add_argument(
        '-d', '--output-dir',
        help='Output directory for compressed files'
    )
    
    parser.add_argument(
        '-l', '--level',
        type=int,
        default=6,
        choices=range(1, 10),
        help='Compression level (1=fastest, 9=best, default=6)'
    )
    
    parser.add_argument(
        '--no-progress',
        action='store_true',
        help='Disable progress display'
    )
    
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help='Overwrite existing files without prompting'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.output and len(args.files) > 1:
        parser.error("--output can only be used with a single input file")
        
    if args.output and args.output_dir:
        parser.error("Cannot use both --output and --output-dir")
        
    # Handle single file with specific output
    if len(args.files) == 1 and args.output:
        success, message, stats = compress_file(
            args.files[0],
            args.output,
            args.level,
            show_progress=not args.no_progress
        )
        
        if success:
            print(message)
            sys.exit(0)
        else:
            print(f"Error: {message}", file=sys.stderr)
            sys.exit(1)
            
    # Handle multiple files or single file with directory output
    success_count, failure_count, results = compress_multiple_files(
        args.files,
        args.output_dir,
        args.level,
        show_progress=not args.no_progress
    )
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Compression Summary:")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {failure_count}")
    print(f"  Total: {success_count + failure_count}")
    
    if success_count > 0:
        total_original = sum(r['stats'].get('original_size', 0) for r in results if r['success'])
        total_compressed = sum(r['stats'].get('compressed_size', 0) for r in results if r['success'])
        total_ratio = (1 - total_compressed / total_original) * 100 if total_original > 0 else 0
        
        print(f"\nOverall Statistics:")
        print(f"  Original Total: {format_size(total_original)}")
        print(f"  Compressed Total: {format_size(total_compressed)}")
        print(f"  Overall Reduction: {total_ratio:.1f}%")
    
    print(f"{'='*60}")
    
    # Exit with appropriate code
    if failure_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == '__main__':
    main()