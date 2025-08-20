# Plan to Fix Multiple Compression Progress Bar Issue

## Problem Analysis

### Root Cause
When processing multiple files in parallel using ThreadPoolExecutor:
1. All SnowflakeLoader instances share the same ProgressTracker instance
2. Each thread independently calls `start_file_compression()` 
3. The method closes the previous bar and creates a new one, but in parallel execution:
   - Thread A starts creating a bar
   - Thread B starts before A finishes, also tries to close/create
   - Thread C does the same
4. Result: Multiple compression bars appear at the same position, overwriting each other

### Current Implementation Issues
```python
def start_file_compression(self, filename: str, file_size_mb: float):
    with self.lock:
        # This closes the single shared compress_pbar
        if self.compress_pbar:
            self.compress_pbar.close()
        # And creates a new one - but multiple threads do this simultaneously
        self.compress_pbar = tqdm(...)
```

The lock only protects the assignment, not the entire lifecycle of the progress bar.

## Solution Options

### Option 1: Per-File Progress Bar Tracking (Recommended)
**Approach**: Maintain a dictionary of progress bars, one per file being processed

**Pros**:
- Each file gets its own progress bar that persists throughout its processing
- True parallel progress visualization
- No bar conflicts or overwrites

**Cons**:
- More complex position management
- Need to track which positions are in use
- More screen real estate needed

**Implementation**:
```python
class ProgressTracker:
    def __init__(self):
        self.compress_pbars = {}  # filename -> pbar mapping
        self.available_positions = []  # Pool of available positions
        
    def start_file_compression(self, filename, size):
        with self.lock:
            position = self.get_next_position()
            self.compress_pbars[filename] = tqdm(position=position, ...)
            
    def update_compression(self, filename, mb):
        with self.lock:
            if filename in self.compress_pbars:
                self.compress_pbars[filename].update(mb)
```

### Option 2: Single Aggregated Progress Bar
**Approach**: One compression bar showing total progress across all files

**Pros**:
- Simple implementation
- Clean display
- No position conflicts

**Cons**:
- Less granular information
- Can't see individual file progress

**Implementation**:
```python
def __init__(self):
    self.compress_pbar = tqdm(total=self.total_size_mb, desc="Compressing files")
    
def update(self, compressed_mb):
    with self.lock:
        self.compress_pbar.update(compressed_mb)
```

### Option 3: Queue-Based Sequential Progress
**Approach**: Show only the currently active compression, queue others

**Pros**:
- Clean display
- No overlapping bars
- Shows current operation clearly

**Cons**:
- Doesn't show true parallel progress
- May be confusing when files complete out of order

### Option 4: Dynamic Position Assignment
**Approach**: Assign positions dynamically as files start processing

**Pros**:
- Flexible number of simultaneous operations
- Efficient use of screen space

**Cons**:
- Complex position management
- Need to handle position recycling

## Recommended Solution: Hybrid Approach

Combine Option 1 and Option 2:
1. **For sequential processing**: Keep current per-file bar behavior
2. **For parallel processing**: Use aggregated progress bar

### Implementation Plan

1. **Detect Parallel Mode**:
   - Add a `parallel_mode` flag to ProgressTracker
   - Set based on max_workers > 1

2. **Modify ProgressTracker**:
   ```python
   class ProgressTracker:
       def __init__(self, ..., parallel_mode=False):
           self.parallel_mode = parallel_mode
           if parallel_mode:
               # Single aggregated compression bar
               self.compress_pbar = tqdm(total=total_size_mb, 
                                        desc="{}Compressing files".format(desc_prefix))
           else:
               # Current per-file approach
               self.compress_pbar = None
   ```

3. **Update Compression Methods**:
   ```python
   def start_file_compression(self, filename, size):
       with self.lock:
           if self.parallel_mode:
               # Just log, don't create new bar
               self.logger.info(f"Starting compression: {filename}")
           else:
               # Current behavior for sequential
               if self.compress_pbar:
                   self.compress_pbar.close()
               self.compress_pbar = tqdm(...)
   ```

4. **Simplify Upload and COPY Bars**:
   - Apply same pattern to upload and COPY progress bars
   - In parallel mode, show aggregate progress

## Alternative Quick Fix

If we want a minimal change:
- Keep a list of active compression bars
- Limit to max 3 simultaneous bars
- Assign different positions to each
- Clean up completed bars

## Testing Requirements

1. Test with 1, 2, 3, and 10 parallel workers
2. Test with mixed file sizes
3. Test interruption and cleanup
4. Test position recycling
5. Verify no visual artifacts or overlaps

## Recommendation

Implement the **Hybrid Approach** as it:
- Maintains current behavior for sequential processing
- Provides clean, aggregated view for parallel processing
- Requires minimal code changes
- Avoids complex position management
- Provides clear user feedback

The change would involve:
1. Adding parallel_mode detection
2. Creating aggregated progress bars when parallel
3. Updating the update methods to handle both modes
4. Testing with various parallelism levels