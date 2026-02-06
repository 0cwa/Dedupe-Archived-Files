"""
SQLite database management for hash storage and duplicate tracking.
"""
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple, Dict
from datetime import datetime
import logging

from .models import FileEntry, ArchiveInfo

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages SQLite database for hash storage and duplicate tracking."""
    
    def __init__(self, db_path: str):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
    
    def connect(self):
        """Connect to database and create tables if needed."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Access columns by name
            self._create_tables()
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def _create_tables(self):
        """Create database tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Archives table - track source archives and their state
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS archives (
                path TEXT PRIMARY KEY,
                mtime REAL NOT NULL,
                size INTEGER NOT NULL,
                last_scanned REAL,
                file_count INTEGER DEFAULT 0
            )
        """)
        
        # Files table - store file hashes from archives
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_hash TEXT,
                quick_hash TEXT,
                filename TEXT NOT NULL,
                path_in_archive TEXT NOT NULL,
                source_archive TEXT NOT NULL,
                size INTEGER NOT NULL,
                is_nested_archive BOOLEAN DEFAULT 0,
                UNIQUE(source_archive, path_in_archive)
            )
        """)
        
        # Create indexes for fast lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_full_hash 
            ON files(full_hash) WHERE full_hash IS NOT NULL
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_quick_hash 
            ON files(quick_hash) WHERE quick_hash IS NOT NULL
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_source_archive 
            ON files(source_archive)
        """)
        
        # Target files table - track target files and their hashes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS target_files (
                path TEXT PRIMARY KEY,
                mtime REAL NOT NULL,
                size INTEGER NOT NULL,
                full_hash TEXT,
                quick_hash TEXT
            )
        """)
        
        # Selection state table - track user's deletion choices
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS selection_state (
                file_hash TEXT NOT NULL,
                target_path TEXT NOT NULL,
                selected BOOLEAN NOT NULL,
                PRIMARY KEY (file_hash, target_path)
            )
        """)
        
        self.conn.commit()
    
    def clear_database(self):
        """Clear all data from database (keep structure)."""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM archives")
        cursor.execute("DELETE FROM files")
        cursor.execute("DELETE FROM target_files")
        cursor.execute("DELETE FROM selection_state")
        self.conn.commit()
        logger.info("Database cleared")
    
    def get_archive_info(self, archive_path: str) -> Optional[ArchiveInfo]:
        """
        Get information about a previously scanned archive.
        
        Args:
            archive_path: Path to archive file
        
        Returns:
            ArchiveInfo if archive exists in DB, None otherwise
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT path, mtime, size, last_scanned, file_count
            FROM archives WHERE path = ?
        """, (archive_path,))
        
        row = cursor.fetchone()
        if row:
            return ArchiveInfo(
                path=row['path'],
                mtime=row['mtime'],
                size=row['size'],
                last_scanned=row['last_scanned'],
                file_count=row['file_count']
            )
        return None
    
    def update_archive(self, archive_path: str, mtime: float, size: int, file_count: int):
        """Update or insert archive information."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO archives (path, mtime, size, last_scanned, file_count)
            VALUES (?, ?, ?, ?, ?)
        """, (archive_path, mtime, size, datetime.now().timestamp(), file_count))
        self.conn.commit()

    def get_target_file_info(self, path: str) -> Optional[Tuple[float, int, Optional[str], Optional[str]]]:
        """
        Get stored hash info for a target file.
        
        Returns:
            Tuple of (mtime, size, full_hash, quick_hash) or None
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT mtime, size, full_hash, quick_hash
            FROM target_files WHERE path = ?
        """, (path,))
        row = cursor.fetchone()
        if row:
            return (row['mtime'], row['size'], row['full_hash'], row['quick_hash'])
        return None

    def update_target_file(self, path: str, mtime: float, size: int, 
                           full_hash: Optional[str], quick_hash: Optional[str]):
        """Update or insert target file hash information."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO target_files (path, mtime, size, full_hash, quick_hash)
            VALUES (?, ?, ?, ?, ?)
        """, (path, mtime, size, full_hash, quick_hash))
        self.conn.commit()
    
    def add_file(self, file_entry: FileEntry):
        """
        Add a file entry to the database.
        
        Args:
            file_entry: FileEntry to store
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO files 
            (full_hash, quick_hash, filename, path_in_archive, source_archive, size, is_nested_archive)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            file_entry.full_hash,
            file_entry.quick_hash,
            file_entry.filename,
            file_entry.path_in_archive,
            file_entry.source_archive,
            file_entry.size,
            file_entry.is_nested_archive
        ))
        self.conn.commit()
    
    def add_files_batch(self, file_entries: List[FileEntry]):
        """Add multiple files in a batch (more efficient)."""
        cursor = self.conn.cursor()
        data = [
            (
                fe.full_hash,
                fe.quick_hash,
                fe.filename,
                fe.path_in_archive,
                fe.source_archive,
                fe.size,
                fe.is_nested_archive
            )
            for fe in file_entries
        ]
        cursor.executemany("""
            INSERT OR REPLACE INTO files 
            (full_hash, quick_hash, filename, path_in_archive, source_archive, size, is_nested_archive)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, data)
        self.conn.commit()
    
    def find_by_full_hash(self, full_hash: str) -> List[FileEntry]:
        """Find all files with matching full hash."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT full_hash, quick_hash, filename, path_in_archive, 
                   source_archive, size, is_nested_archive
            FROM files WHERE full_hash = ?
        """, (full_hash,))
        
        return [self._row_to_file_entry(row) for row in cursor.fetchall()]
    
    def find_by_quick_hash(self, quick_hash: str) -> List[FileEntry]:
        """Find all files with matching quick hash."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT full_hash, quick_hash, filename, path_in_archive, 
                   source_archive, size, is_nested_archive
            FROM files WHERE quick_hash = ?
        """, (quick_hash,))
        
        return [self._row_to_file_entry(row) for row in cursor.fetchall()]
    
    def check_quick_hash_exists(self, quick_hash: str) -> bool:
        """Check if a quick hash exists in database."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM files WHERE quick_hash = ? LIMIT 1", (quick_hash,))
        return cursor.fetchone() is not None
    
    def update_full_hash(self, source_archive: str, path_in_archive: str, full_hash: str):
        """Update full hash for a file that previously only had quick hash."""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE files SET full_hash = ?
            WHERE source_archive = ? AND path_in_archive = ?
        """, (full_hash, source_archive, path_in_archive))
        self.conn.commit()
    
    def get_all_archives(self) -> List[str]:
        """Get list of all source archives in database."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT path FROM archives ORDER BY path")
        return [row[0] for row in cursor.fetchall()]
    
    def get_files_by_archive(self, archive_path: str) -> List[FileEntry]:
        """Get all files from a specific archive."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT full_hash, quick_hash, filename, path_in_archive, 
                   source_archive, size, is_nested_archive
            FROM files WHERE source_archive = ?
            ORDER BY path_in_archive
        """, (archive_path,))
        
        return [self._row_to_file_entry(row) for row in cursor.fetchall()]
    
    def get_selection_state(self, file_hash: str, target_path: str) -> Optional[bool]:
        """Get user's selection state for a duplicate file."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT selected FROM selection_state
            WHERE file_hash = ? AND target_path = ?
        """, (file_hash, target_path))
        
        row = cursor.fetchone()
        return bool(row['selected']) if row else None
    
    def set_selection_state(self, file_hash: str, target_path: str, selected: bool):
        """Set user's selection state for a duplicate file."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO selection_state (file_hash, target_path, selected)
            VALUES (?, ?, ?)
        """, (file_hash, target_path, selected))
        self.conn.commit()
    
    def get_statistics(self) -> Dict[str, int]:
        """Get database statistics."""
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM archives")
        archive_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM files")
        file_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM files WHERE is_nested_archive = 1")
        nested_archive_count = cursor.fetchone()[0]
        
        return {
            'archives': archive_count,
            'files': file_count,
            'nested_archives': nested_archive_count
        }
    
    def _row_to_file_entry(self, row) -> FileEntry:
        """Convert database row to FileEntry object."""
        return FileEntry(
            full_hash=row['full_hash'],
            quick_hash=row['quick_hash'],
            filename=row['filename'],
            path_in_archive=row['path_in_archive'],
            source_archive=row['source_archive'],
            size=row['size'],
            is_nested_archive=bool(row['is_nested_archive'])
        )
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
