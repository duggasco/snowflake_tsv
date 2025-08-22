# Progress Visibility Implementation - v2.3.0

## Problem Solved
The screen was going black during Snowflake operations because all output (including progress bars) was being redirected to log files with `> "$log_file" 2>&1`, making it impossible to see real-time progress.

## Solution Implemented

### 1. Two Execution Modes

#### Foreground Mode (Real-time Progress)
- Shows all output directly in terminal
- Uses `tee` to display AND log simultaneously
- Progress bars, status updates visible in real-time
- User sees immediate feedback

#### Background Mode (Traditional)
- Runs silently in background
- All output captured to log file
- Can monitor via Job Status menu

### 2. Enhanced Job Status Menu

The Job Status menu now provides:
- **View All Jobs Summary** - Overview of all jobs
- **Monitor: [job_name]** - Live monitoring of running jobs
- **Clean Completed Jobs** - Cleanup functionality
- **Refresh** - Update job statuses

When monitoring a running job:
- Uses `tail -f` to show live log updates
- Progress bars and status messages appear in real-time
- Ctrl+C to stop monitoring and return to menu

### 3. User Choice for Each Operation

When starting operations, users are now asked:
```
"Show real-time progress? (Y=foreground, N=background)"
```
- **Yes**: Runs in foreground with visible progress
- **No**: Runs in background, check Job Status menu to monitor

### 4. Implementation Details

#### Code Changes:
```bash
# New function for foreground execution
start_foreground_job() {
    SHOW_REALTIME_OUTPUT=true start_background_job "$@"
}

# Modified start_background_job to check SHOW_REALTIME_OUTPUT
if [[ "$show_output" == "true" ]]; then
    # Use tee for real-time display + logging
    "$@" 2>&1 | tee "$log_file"
else
    # Original background behavior
    "$@" > "$log_file" 2>&1 &
fi
```

#### Job Monitoring:
```bash
monitor_job_progress() {
    # Uses tail -f for live updates
    tail -f "$log_file"
}
```

## Testing Results

### Test 1: Foreground Execution
✅ Progress bars visible in real-time
✅ Output saved to log file
✅ User sees immediate feedback

### Test 2: Background Execution  
✅ Job runs silently
✅ Output captured to log
✅ Can monitor via Job Status menu

### Test 3: Live Monitoring
✅ `tail -f` shows real-time updates
✅ Progress visible as job runs
✅ Can stop monitoring with Ctrl+C

## User Benefits

1. **No More Black Screen** - Choose to see progress in real-time
2. **Flexible Monitoring** - Monitor any running job at any time
3. **Better UX** - Immediate feedback for long operations
4. **Preserved Logging** - All output still captured to logs
5. **Background Option** - Can still run silently if preferred

## Usage Examples

### Example 1: Load with Real-time Progress
```
Quick Load > Load Current Month
> Confirm load? Yes
> Show real-time progress? Yes
[Progress displays in terminal]
```

### Example 2: Background Load with Monitoring
```
Quick Load > Load Current Month  
> Confirm load? Yes
> Show real-time progress? No
[Job starts in background]

Job Status > Monitor: load_2024-01
[Live log updates display]
```

## Technical Notes

- Progress bars write to stderr, which is now properly handled
- `tee` command ensures output goes to both terminal and log
- Job status is updated regardless of execution mode
- Background jobs can still be monitored after starting
- All existing logging functionality preserved

## Version
Implemented in wrapper version 2.3.0