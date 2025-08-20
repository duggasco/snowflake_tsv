# Fix Plan for Bash --parallel --quiet Progress Bar Issues

## Problem Analysis

### Current Behavior (PROBLEMATIC)
When running `./run_loader.sh --parallel 3 --quiet`:

1. **Bash launches 3 separate Python processes** (not threads)
2. Each process creates its own ProgressTracker with 5 progress bars
3. Each process uses TSV_JOB_POSITION to calculate offset (0, 1, 2)
4. **Issue**: The offset calculation assumes 5 lines per job, but:
   - When Process 1 creates a compression bar for file_1.tsv
   - Then creates a new bar for file_2.tsv
   - The old bar for file_1.tsv becomes "static/dead"
   - It's not cleared because `leave=False` only works within same process

### Visual Result
```
[2024-01] Files: 2/3                              <- Process 1
[2024-01] QC Progress: 100%                       <- Process 1
[2024-01] Compressing file_1.tsv: 100%  [DEAD]    <- Old bar from Process 1
[2024-01] Compressing file_2.tsv: 45%             <- Current bar from Process 1
[2024-01] Uploading file_1.tsv.gz: 100% [DEAD]    <- Old bar
[2024-01] Uploading file_2.tsv.gz: 20%            <- Current bar
[2024-02] Files: 1/3                              <- Process 2
[2024-02] Compressing file_3.tsv: 100% [DEAD]     <- Old bar from Process 2
[2024-02] Compressing file_4.tsv: 60%             <- Current bar
... multiple overlapping/dead bars ...
```

## Root Causes

1. **Bar Lifecycle Issue**: `leave=False` only cleans up bars when the Python process exits, not when switching files
2. **Position Reuse**: Same positions are reused for different files, leaving old bars visible
3. **No Inter-Process Communication**: Processes can't coordinate bar cleanup

## Solution Options

### Option 1: Single Progress Bar Per Operation Type (RECOMMENDED)
**Change**: Don't create new bars for each file. Reuse the same bar.

```python
class ProgressTracker:
    def __init__(self):
        # Create bars once, reuse for all files
        self.compress_pbar = tqdm(total=self.total_size_mb, 
                                 desc="{}Compression".format(desc_prefix),
                                 position=self.compress_position)
        self.upload_pbar = tqdm(total=self.total_size_mb,
                               desc="{}Upload".format(desc_prefix),
                               position=self.upload_position)
        
    def start_file_compression(self, filename, size):
        # Just reset and update description, don't create new bar
        if self.compress_pbar:
            self.compress_pbar.reset(total=size)
            self.compress_pbar.set_description("{}Compressing {}".format(
                self.desc_prefix, os.path.basename(filename)))
```

**Pros**: 
- No dead bars
- Clean display
- Simple implementation

**Cons**:
- Less detailed per-file tracking

### Option 2: Clear Line Before Creating New Bar
**Change**: Explicitly clear the terminal line before creating a new bar

```python
def start_file_compression(self, filename, size):
    with self.lock:
        if self.compress_pbar:
            # Clear the line at this position
            sys.stderr.write(f"\033[{self.compress_position};0H\033[K")
            sys.stderr.flush()
            self.compress_pbar.close()
        # Now create new bar
        self.compress_pbar = tqdm(...)
```

**Pros**:
- Removes dead bars
- Maintains per-file display

**Cons**:
- Terminal escape sequences may not work everywhere
- More complex

### Option 3: Use leave=True but Limit Bar Count
**Change**: Keep bars visible but limit how many can exist

```python
def start_file_compression(self, filename, size):
    with self.lock:
        # Keep a history of bars
        if len(self.compress_bar_history) >= 2:
            # Close oldest bar
            oldest = self.compress_bar_history.pop(0)
            oldest.close()
        
        new_bar = tqdm(..., leave=True)
        self.compress_bar_history.append(new_bar)
```

### Option 4: Aggregate Progress for Parallel Mode
**Change**: Detect when running in parallel and use aggregate bars

```python
def __init__(self, ..., parallel_mode=False):
    if parallel_mode or os.environ.get('TSV_JOB_POSITION'):
        # Use aggregate bars for parallel
        self.compress_pbar = tqdm(total=total_size_mb,
                                 desc="{}Compressing files".format(desc_prefix))
    else:
        # Use per-file bars for sequential
        self.compress_pbar = None
```

## Recommended Implementation: Option 1 with Modifications

### Step 1: Modify ProgressTracker to reuse bars

```python
def __init__(self):
    # Create reusable bars for compression, upload, copy
    if TQDM_AVAILABLE:
        # These bars persist for the lifetime of the tracker
        self.compress_pbar = tqdm(total=100, desc="{}Compression".format(desc_prefix),
                                 position=self.compress_position, 
                                 unit="%", leave=False)
        self.upload_pbar = tqdm(total=100, desc="{}Upload".format(desc_prefix),
                               position=self.upload_position,
                               unit="%", leave=False)
        self.copy_pbar = tqdm(total=100, desc="{}Copy".format(desc_prefix),
                             position=self.copy_position,
                             unit="%", leave=False)

def start_file_compression(self, filename, size):
    """Update compression bar for new file"""
    with self.lock:
        if self.compress_pbar:
            # Reset for new file
            self.compress_pbar.n = 0
            self.compress_pbar.total = size
            self.compress_pbar.set_description_str(
                "{}Compressing {}".format(self.desc_prefix, os.path.basename(filename)))
            self.compress_pbar.refresh()
```

### Step 2: Alternative - Add cleanup between files

```python
def finish_file_compression(self):
    """Clean up compression bar after file completes"""
    with self.lock:
        if self.compress_pbar:
            self.compress_pbar.set_description_str(
                "{}Compression complete".format(self.desc_prefix))
            # Don't close, just mark as complete
```

### Step 3: Test thoroughly
- Test with 1, 2, 3, 5 parallel jobs
- Test with --quiet and without
- Test interruption and cleanup
- Verify no dead bars remain

## Quick Fix (Minimal Changes)

If we need a quick fix with minimal code changes:

1. Change `leave=False` to `leave=True` for all bars
2. Add a clear screen at the start of parallel jobs
3. Reduce the number of progress bars shown (maybe just Files + Compression)

## Testing Script

```bash
# Test various parallel configurations
./run_loader.sh --months 01,02,03 --parallel 3 --quiet --dry-run
```

## Recommendation

Implement **Option 1** (Single Progress Bar Per Operation Type) because:
- It's the cleanest solution
- No dead bars
- Works well with parallel processing
- Minimal code changes
- Better user experience

The key insight is that when running in parallel with separate processes, we need to be more conservative about creating/destroying progress bars since each process can't clean up bars from other processes.