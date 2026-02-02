# Quick Start Guide

## Installation

### With uv (Recommended)
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install
uv venv
uv pip install -r requirements.txt

# Run
uv run main.py
```

### With pip
```bash
# Install dependencies
pip install -r requirements.txt
```

## Development & Testing

### Running Tests
```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_models.py -v

# Run with coverage
uv run pytest tests/ --cov=core --cov=tui
```

### Test-Driven Development
This project uses test-driven development (TDD). The test suite includes:
- **test_models.py**: Data model tests (FileEntry, DuplicateMatch, ArchiveInfo, AppConfig)
- **test_hasher.py**: Hash calculation tests with xxHash
- **test_file_ops.py**: File deletion and size formatting tests
- **test_extractor.py**: Archive extraction tests (ZIP, TAR, 7z, RAR)
- **test_database.py**: SQLite database operations tests
- **test_scanner.py**: Source and target scanner tests
- **test_tui.py**: TUI component and navigation tests

## Basic Usage

### 1. Interactive Mode (Easiest)

```bash
python main.py
```

Then:
1. Click "+ Add Source" to add directories containing archives
2. Click "+ Add Target" to add directories to search for duplicates
3. Click "⚙️ Settings" to configure options (optional)
4. Click "▶️ Start Scan" to begin
5. Review duplicates and select which to delete
6. Confirm and delete

### 2. Command-Line Mode

```bash
# Basic scan
python main.py --source /path/to/archives --target /path/to/check

# Dry run (safe - shows what would be deleted)
python main.py -s /archives -t /documents --dry-run

# Multiple sources and targets
python main.py \
  -s /backup1 -s /backup2 \
  -t /home/user -t /mnt/storage
```

## Key Features

### Archive Support
- ZIP, RAR, 7Z, TAR (gz/bz2/xz), ISO
- Nested archives (archives within archives)
- Recursive extraction

### Performance
- xxHash (very fast)
- Partial hashing for large files (>1MB)
- SQLite database for efficient storage
- Archive change detection (skip unchanged archives)

### Safety
- **Trash by default** - files are recoverable
- Dry-run mode available
- Final confirmation before deletion
- Never modifies source archives
- Comprehensive logging

## Common Options

```bash
--dry-run              # Preview without deleting
--delete-method trash  # Move to trash (default)
--delete-method permanent  # Permanently delete
--min-size 1024       # Skip files smaller than 1KB
--db-path /tmp/cache.db  # Custom database location
--no-recheck          # Skip checking if archives changed
--verbose, -v         # Detailed logging
```

## Example Workflows

### Clean old backups
```bash
# Find files in your documents that exist in backup archives
python main.py -s /backups/2024 -t ~/Documents
```

### Remove duplicates across multiple locations
```bash
# Check multiple locations for duplicates
python main.py \
  -s /archive_storage \
  -t /home/user -t /mnt/external -t /media/usb
```

### Safe preview before deletion
```bash
# Always start with dry-run!
python main.py -s /archives -t /data --dry-run

# If satisfied, run without --dry-run
python main.py -s /archives -t /data
```

## Keyboard Shortcuts (Interactive Mode)

### Global
- `q`: Quit application
- `Esc`: Go back to previous screen

### Main Screen
- `s`: Open settings
- `Enter`: Start scan (when directories configured)
- Arrow keys: Navigate directory lists

### Directory Selection
- `Enter` or click "Add": Add directory
- `Esc` or click "Cancel": Go back

### Review Screen
- `Space`: Toggle file selection
- `a`: Select all files
- `n`: Deselect all files
- `Esc`: Go back to scanning

### Settings Screen
- `Enter` or click "Save": Save settings
- `Esc` or click "Cancel": Discard changes

### Message/Dialog Screens
- `Enter`, `Esc`, or click "OK": Dismiss dialog

## Tips

1. **Always backup** important data first
2. **Use dry-run** to preview changes
3. **Keep the database** for faster re-scans
4. **Set min-size** to skip tiny files (faster scanning)
5. **Check the log** file (`dup_cleaner.log`) for details

## Troubleshooting

### Missing library errors
```bash
pip install py7zr rarfile libarchive-c send2trash
```

### RAR support
```bash
# Linux
sudo apt install unrar

# macOS
brew install unrar
```

### Permission errors
Make sure you have read access to source archives and write access to target directories.

## Need Help?

1. Check `README.md` for detailed documentation
2. Review `dup_cleaner.log` for error messages
3. Run with `--verbose` for detailed output

---

**⚠️ Always backup before deleting!**
