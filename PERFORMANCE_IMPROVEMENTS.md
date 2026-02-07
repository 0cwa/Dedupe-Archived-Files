# ReviewScreen Performance Optimization

## Problem
The selection screen was extremely slow when displaying large numbers of duplicates (e.g., 14,000 duplicates across 400 archives). The UI became unresponsive due to the need to render all items upfront.

## Root Cause
The original `ReviewScreen` implementation used `ListView` with `ListItem` widgets for each duplicate. This approach created all 14,000+ widget instances during `compose()`, causing severe performance issues because:
- All widgets were created in memory upfront
- All widgets needed to be rendered immediately
- Navigation and selection operations had to traverse the entire DOM tree

## Solution
Refactored `ReviewScreen` to use `DataTable` instead of `ListView`:

### Key Changes

1. **Widget Replacement**
   - Replaced `ListView` with `DataTable` widget
   - `DataTable` has built-in row virtualization, only rendering visible rows
   - Significantly reduced memory footprint and rendering time

2. **Asynchronous Data Loading**
   - Moved data loading from `compose()` to `on_mount()` using `_load_data()`
   - Screen renders first, data loads asynchronously
   - Better perceived performance and user experience

3. **Optimized Data Structure**
   - Changed from `item_map` (tracking ListItem objects) to `row_map` (tracking row indices)
   - Each table row has a unique key for efficient lookups
   - Simplified cell updates using row keys

4. **Improved Selection Operations**
   - Streamlined `action_select_all()` and `action_deselect_all()` methods
   - Direct cell updates without unnecessary lookups
   - Reduced complexity from O(n) lookups to direct key-based access

5. **Better Table Layout**
   - Organized data into columns: Select, Source File, Target Path, Size
   - Archive headers displayed as separate rows with visual indicators (📦)
   - Clear checkbox representation using [X] and [ ]

## Benefits

- **Dramatically improved performance**: Screen remains responsive even with tens of thousands of duplicates
- **Better memory usage**: Only visible rows are kept in memory at any time
- **Smoother navigation**: DataTable's built-in navigation handles large datasets efficiently
- **Maintained functionality**: All keyboard shortcuts (Space, A, N, Esc, Q) work as before
- **Better visual organization**: Tabular format makes it easier to scan and compare duplicates

## Files Modified
- `/home/engine/project/tui/app.py`: Refactored `ReviewScreen` class

## Testing
All existing tests pass:
```bash
python3 -m pytest tests/test_tui.py -v
# 29 passed
```

## Technical Details

### Before (ListView approach)
```python
# Created all items upfront in compose()
items = []
for match in matches:
    items.append(ListItem(Label(...)))
yield ListView(*items)
```

### After (DataTable approach)
```python
# Render empty table in compose()
yield DataTable(id="dup-table")

# Load data asynchronously
async def on_mount(self):
    await self._load_data()
```

This change leverages DataTable's virtualization to only render rows that are currently visible, making the UI performant regardless of dataset size.
