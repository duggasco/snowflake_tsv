# Fix Implementation for Static Progress Bars Issue

## The Problem (Confirmed)
When processing multiple files in parallel with `--quiet`:
1. Each process creates new progress bars for each file
2. Old bars remain on screen at 100% (static/dead)
3. New bars overwrite at the same position
4. Result: Multiple "Compressing file_X.tsv: 100%" bars accumulate

## The Solution: Reuse Progress Bars

### Key Changes Needed in tsv_loader.py

Instead of creating new bars for each file, we'll:
1. Create bars once during initialization
2. Reset and update them for each new file
3. This prevents dead bars from accumulating

### Implementation

```python
class ProgressTracker:
    def __init__(self, ...):
        # ... existing code ...
        
        # Create persistent bars that will be reused
        if TQDM_AVAILABLE:
            # ... existing file and QC bars ...
            
            # Create reusable bars for operations (not file-specific)
            self.compress_pbar = None
            self.upload_pbar = None
            self.copy_pbar = None
            
            # Track current file being processed
            self.current_compress_file = None
            self.current_upload_file = None
            self.current_copy_table = None
    
    def start_file_compression(self, filename: str, file_size_mb: float):
        """Start or update compression progress for a file"""
        import os
        with self.lock:
            self.current_compress_file = filename
            if TQDM_AVAILABLE:
                if self.compress_pbar is None:
                    # Create bar first time
                    self.compress_pbar = tqdm(
                        total=file_size_mb,
                        desc="{}Compressing {}".format(
                            self.desc_prefix, os.path.basename(filename)
                        ),
                        unit="MB",
                        unit_scale=True,
                        position=self.compress_position,
                        leave=False,
                        file=sys.stderr
                    )
                else:
                    # Reuse existing bar - reset for new file
                    self.compress_pbar.reset(total=file_size_mb)
                    self.compress_pbar.set_description(
                        "{}Compressing {}".format(
                            self.desc_prefix, os.path.basename(filename)
                        )
                    )
                    self.compress_pbar.refresh()
    
    def start_file_upload(self, filename: str, file_size_mb: float):
        """Start or update upload progress for a file"""
        import os
        with self.lock:
            self.current_upload_file = filename
            if TQDM_AVAILABLE:
                if self.upload_pbar is None:
                    # Create bar first time
                    self.upload_pbar = tqdm(
                        total=file_size_mb,
                        desc="{}Uploading {}".format(
                            self.desc_prefix, os.path.basename(filename)
                        ),
                        unit="MB",
                        unit_scale=True,
                        position=self.upload_position,
                        leave=False,
                        file=sys.stderr
                    )
                else:
                    # Reuse existing bar - reset for new file
                    self.upload_pbar.reset(total=file_size_mb)
                    self.upload_pbar.set_description(
                        "{}Uploading {}".format(
                            self.desc_prefix, os.path.basename(filename)
                        )
                    )
                    self.upload_pbar.refresh()
    
    def start_copy_operation(self, table_name: str, row_count: int):
        """Start or update COPY progress for a table"""
        with self.lock:
            self.current_copy_table = table_name
            if TQDM_AVAILABLE:
                if self.copy_pbar is None:
                    # Create bar first time
                    self.copy_pbar = tqdm(
                        total=row_count,
                        desc="{}Loading into {}".format(
                            self.desc_prefix, table_name
                        ),
                        unit="rows",
                        unit_scale=True,
                        position=self.copy_position,
                        leave=False,
                        file=sys.stderr
                    )
                else:
                    # Reuse existing bar - reset for new table
                    self.copy_pbar.reset(total=row_count)
                    self.copy_pbar.set_description(
                        "{}Loading into {}".format(
                            self.desc_prefix, table_name
                        )
                    )
                    self.copy_pbar.refresh()
```

## Alternative: Clear Terminal Lines

If we want to keep creating new bars but clear old ones:

```python
def start_file_compression(self, filename: str, file_size_mb: float):
    """Start compression with line clearing"""
    import os
    with self.lock:
        if TQDM_AVAILABLE:
            if self.compress_pbar:
                # Clear the line at compression position before creating new bar
                sys.stderr.write(f"\033[{self.compress_position + 1};0H\033[K")
                sys.stderr.flush()
                self.compress_pbar.close()
            
            # Now create new bar
            self.compress_pbar = tqdm(...)
```

## Testing the Fix

```bash
# Test with parallel processing
./run_loader.sh --months 012024,022024,032024 --parallel 3 --quiet --skip-qc

# What to look for:
# - No static bars at 100%
# - Each process shows only its current operation
# - Clean display without accumulation
```

## Benefits of This Fix

1. **No dead bars**: Reusing bars prevents accumulation
2. **Cleaner display**: Only active operations shown
3. **Better performance**: Fewer bar objects created/destroyed
4. **Works with parallel**: Each process manages its own persistent bars

## Quick Test Without Full Implementation

To quickly test if this works, we could:
1. Set `leave=True` for all bars (keeps them visible but at least they're complete)
2. Or reduce to just 2 bars total (Files + one operation bar that changes description)