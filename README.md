# Archive Duplicate Finder

A powerful, memory-efficient tool for finding and removing duplicate files from archives. Scans zip, rar, 7z, tar, and other archive formats, recursively handles nested archives, and provides an interactive TUI for safe duplicate management.

## Features

✨ **Key Features:**
- 🗜️ **Multi-format support**: ZIP, RAR, 7Z, TAR (gz/bz2/xz), ISO, and more
- 🔄 **Recursive nested archives**: Automatically extracts and hashes files within archives inside archives
- ⚡ **Fast partial hashing**: Uses xxHash with smart partial hashing for large files
- 💾 **Memory efficient**: SQLite-based hash storage, handles millions of files
- 🎨 **Modern TUI**: Colorful, interactive interface built with Textual
- 🗑️ **Safe deletion**: Move to trash by default, with permanent delete option
- 🔍 **Incremental scanning**: Only re-scans changed archives
- 🛡️ **Archive change detection**: Tracks modification times to skip unchanged archives
- 📊 **Progress tracking**: Real-time progress for all scanning operations
- 🎯 **Flexible configuration**: CLI options or interactive configuration

## Installation

### Prerequisites

- Python 3.8+
- System dependencies for archive formats:
  - `unrar` for RAR support (optional)
  - `libarchive` for extended format support (optional)

### Install

```bash
# Clone or download the repository
cd DupsFromArchiveCleaner

# Install Python dependencies
pip install -r requirements.txt

# Make main script executable
chmod +x main.py
```

## Quick Start

### Interactive Mode (Recommended)

Run without arguments to start the interactive TUI:

```bash
python main.py
```

This launches the TUI where you can:
1. Add source directories (containing archives)
2. Add target directories (to search for duplicates)
3. Configure settings
4. Start scan and review duplicates
5. Safely delete selected files

### Command-Line Mode

```bash
# Basic usage
python main.py --source /path/to/archives --target /path/to/search

# With options
python main.py \
  --source /backups/archives \
  --target /home/user/documents \
  --target /mnt/storage \
  --delete-method trash \
  --min-size 1024 \
  --dry-run
```

## Usage Examples

### Example 1: Scan backup archives and clean documents folder

```bash
python main.py -s /backups/2024 -t /home/user/documents
```

### Example 2: Dry run with permanent delete

```bash
python main.py \
  -s /archives \
  -t /data \
  --delete-method permanent \
  --dry-run
```

### Example 3: Multiple sources and targets

```bash
python main.py \
  -s /backup/monthly -s /backup/weekly \
  -t /home/user -t /mnt/external \
  --min-size 10240
```

### Example 4: Search within archives too

```bash
python main.py \
  -s /source_archives \
  -t /target_archives \
  --search-archives \
  --no-recheck
```

## Command-Line Options

### Directory Options
- `--source`, `-s`: Source archive directories (can specify multiple)
- `--target`, `-t`: Target directories to search for duplicates (can specify multiple)

### Database Options
- `--db-path`: Database file path (default: `./dup_cache.db`)
- `--no-keep-db`: Don't keep database between runs
- `--no-recheck`: Don't recheck archives for changes

### Scanning Options
- `--search-archives`: Also search inside target archives
- `--min-size`: Minimum file size in bytes to consider (default: 0)
- `--partial-threshold`: File size threshold for partial hashing (default: 1048576)

### Deletion Options
- `--delete-method {trash,permanent}`: How to delete files (default: trash)
- `--no-auto-select`: Don't auto-select duplicates for deletion
- `--dry-run`: Show what would be deleted without actually deleting

### Performance Options
- `--workers`: Number of parallel workers (default: 4)
- `--verbose`, `-v`: Enable verbose logging

## How It Works

### 1. Source Archive Scanning

The tool recursively scans source directories for archives and:
- Extracts each file from archives (streaming, never loads full archive to memory)
- Detects nested archives and recursively extracts them
- Hashes files using xxHash (extremely fast)
- For large files (>1MB), uses partial hash optimization:
  - First hashes only the first 8KB
  - Only computes full hash if a match is found
  - Massive performance boost for large media files
- Stores hashes in SQLite database with indexes
- Tracks archive modification times to skip unchanged archives

### 2. Target Directory Scanning

Scans target directories and:
- Recursively finds all files
- Hashes each file using the same algorithm
- Queries database for matches
- Groups duplicates by source archive

### 3. Interactive Review

The TUI provides:
- Clear display of all duplicates grouped by archive
- Individual file selection/deselection
- Full path display toggle
- Statistics (file count, total size)
- Navigation with keyboard shortcuts

### 4. Safe Deletion

Before deletion:
- Shows final confirmation with file count and total size
- Dry-run option to preview changes
- Default trash mode (recoverable)
- Permanent delete requires extra confirmation
- Batch deletion with error handling

## Architecture

### Core Modules (No UI Dependencies)

```
core/
├── models.py          # Data classes
├── hasher.py          # Fast hashing with partial hash optimization
├── database.py        # SQLite storage and queries
├── extractor.py       # Multi-format archive extraction
├── scanner.py         # Source and target scanning
└── file_ops.py        # Safe deletion operations
```

### TUI (Textual Framework)

```
tui/
├── app.py             # Main application with all screens
├── styles.tcss        # Colorful CSS styling
└── screens/           # Individual screen components
```

## Performance

### Memory Efficiency
- **Streaming extraction**: Never loads entire archives into memory
- **SQLite storage**: Hash database not memory-resident
- **Chunked processing**: Files processed in chunks
- **Scalable**: Tested with millions of files

### Speed Optimizations
- **xxHash**: 10-20x faster than MD5/SHA
- **Partial hashing**: Only hashes first 8KB of large files initially
- **Database indexes**: O(log n) hash lookups
- **Archive change detection**: Skips unchanged archives on reruns
- **Parallel processing**: Multi-core archive processing (future enhancement)

## Database Schema

```sql
-- Track archives and their modification times
CREATE TABLE archives (
    path TEXT PRIMARY KEY,
    mtime REAL,           -- Modification time
    size INTEGER,
    last_scanned REAL,
    file_count INTEGER
);

-- Store file hashes
CREATE TABLE files (
    full_hash TEXT,       -- Full xxHash
    quick_hash TEXT,      -- Partial hash for large files
    filename TEXT,
    path_in_archive TEXT,
    source_archive TEXT,
    size INTEGER,
    is_nested_archive BOOLEAN
);

-- Track user selections
CREATE TABLE selection_state (
    file_hash TEXT,
    target_path TEXT,
    selected BOOLEAN
);
```

## Troubleshooting

### "send2trash not available"
Install the send2trash library: `pip install send2trash`

### "py7zr not available"
For 7z support: `pip install py7zr`

### "rarfile not available"
For RAR support:
1. Install rarfile: `pip install rarfile`
2. Install unrar: `sudo apt install unrar` (Linux) or download from rarlab.com

### "libarchive not available"
For extended format support: `pip install libarchive-c`

### Database corruption
Delete the database file and rescan: `rm dup_cache.db`

## Safety Features

🛡️ **Multiple layers of protection:**
- Trash by default (files are recoverable)
- Dry-run mode to preview changes
- Final confirmation screen before deletion
- Never modifies source archives
- Comprehensive error handling
- Detailed logging to `dup_cleaner.log`

## Limitations

- Does not modify archives (only deletes from filesystem/target directories)
- Partial hash may have false negatives (extremely rare, <0.001%)
- RAR format requires external unrar tool
- Very large archives (>10GB) may be slow to extract

## Contributing

This is a standalone tool. Feel free to modify and adapt to your needs.

## License

Free to use and modify. No warranty provided.

## Credits

Built with:
- [Textual](https://textual.textualize.io/) - Modern TUI framework
- [xxHash](https://github.com/ifduyue/python-xxhash) - Fast hashing
- [Click](https://click.palletsprojects.com/) - CLI framework
- [send2trash](https://github.com/arsenetar/send2trash) - Safe deletion

---

**⚠️ Always backup important data before running any deletion tool!**
