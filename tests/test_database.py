"""
Tests for core.database module.
"""
import pytest
import tempfile
import sqlite3
from pathlib import Path
from core.database import DatabaseManager
from core.models import FileEntry, ArchiveInfo


class TestDatabaseManagerInit:
    """Tests for DatabaseManager initialization."""
    
    def test_init(self, tmp_path):
        """Test initialization."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        assert db.db_path == db_path
        assert db.conn is None
    
    def test_connect(self, tmp_path):
        """Test database connection."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        assert db.conn is not None
        db.close()
    
    def test_connect_creates_tables(self, tmp_path):
        """Test that connect creates tables."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        # Check tables exist
        cursor = db.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        assert 'archives' in tables
        assert 'files' in tables
        assert 'selection_state' in tables
        
        db.close()
    
    def test_context_manager(self, tmp_path):
        """Test context manager usage."""
        db_path = str(tmp_path / "test.db")
        with DatabaseManager(db_path) as db:
            assert db.conn is not None
        # After exiting context, connection should be closed
        assert db.conn is None


class TestArchiveOperations:
    """Tests for archive operations."""
    
    def test_update_archive(self, tmp_path):
        """Test updating archive info."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        db.update_archive("/path/archive.zip", 1234567890.0, 10000, 50)
        
        info = db.get_archive_info("/path/archive.zip")
        assert info is not None
        assert info.path == "/path/archive.zip"
        assert info.mtime == 1234567890.0
        assert info.size == 10000
        assert info.file_count == 50
        
        db.close()
    
    def test_get_archive_info_nonexistent(self, tmp_path):
        """Test getting info for non-existent archive."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        info = db.get_archive_info("/path/missing.zip")
        assert info is None
        
        db.close()
    
    def test_update_archive_overwrite(self, tmp_path):
        """Test that update overwrites existing."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        db.update_archive("/path/archive.zip", 1000.0, 5000, 10)
        db.update_archive("/path/archive.zip", 2000.0, 10000, 20)
        
        info = db.get_archive_info("/path/archive.zip")
        assert info.mtime == 2000.0
        assert info.size == 10000
        assert info.file_count == 20
        
        db.close()
    
    def test_get_all_archives(self, tmp_path):
        """Test getting all archives."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        db.update_archive("/path/a.zip", 1.0, 100, 10)
        db.update_archive("/path/b.zip", 2.0, 200, 20)
        db.update_archive("/path/c.zip", 3.0, 300, 30)
        
        archives = db.get_all_archives()
        assert len(archives) == 3
        assert "/path/a.zip" in archives
        assert "/path/b.zip" in archives
        assert "/path/c.zip" in archives
        
        db.close()


class TestFileOperations:
    """Tests for file operations."""
    
    def test_add_file(self, tmp_path):
        """Test adding a single file."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        entry = FileEntry(
            full_hash="abc123",
            quick_hash=None,
            filename="test.txt",
            path_in_archive="docs/test.txt",
            source_archive="/path/archive.zip",
            size=1024,
            is_nested_archive=False
        )
        
        db.add_file(entry)
        
        # Verify by searching
        results = db.find_by_full_hash("abc123")
        assert len(results) == 1
        assert results[0].filename == "test.txt"
        
        db.close()
    
    def test_add_files_batch(self, tmp_path):
        """Test batch file addition."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        entries = [
            FileEntry(
                full_hash=f"hash{i}",
                quick_hash=None,
                filename=f"file{i}.txt",
                path_in_archive=f"file{i}.txt",
                source_archive="/path/archive.zip",
                size=100 * i,
                is_nested_archive=False
            )
            for i in range(100)
        ]
        
        db.add_files_batch(entries)
        
        # Verify all were added
        for i in range(100):
            results = db.find_by_full_hash(f"hash{i}")
            assert len(results) == 1
        
        db.close()
    
    def test_find_by_full_hash(self, tmp_path):
        """Test finding by full hash."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        # Add multiple files with same hash
        for i in range(3):
            entry = FileEntry(
                full_hash="shared_hash",
                quick_hash=None,
                filename=f"file{i}.txt",
                path_in_archive=f"file{i}.txt",
                source_archive=f"/path/archive{i}.zip",
                size=100,
                is_nested_archive=False
            )
            db.add_file(entry)
        
        results = db.find_by_full_hash("shared_hash")
        assert len(results) == 3
        
        db.close()
    
    def test_find_by_quick_hash(self, tmp_path):
        """Test finding by quick hash."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        entry = FileEntry(
            full_hash=None,
            quick_hash="quick123",
            filename="large.bin",
            path_in_archive="large.bin",
            source_archive="/path/archive.zip",
            size=10_000_000,
            is_nested_archive=False
        )
        db.add_file(entry)
        
        results = db.find_by_quick_hash("quick123")
        assert len(results) == 1
        assert results[0].quick_hash == "quick123"
        
        db.close()
    
    def test_check_quick_hash_exists(self, tmp_path):
        """Test checking quick hash existence."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        entry = FileEntry(
            full_hash=None,
            quick_hash="existing_quick",
            filename="file.bin",
            path_in_archive="file.bin",
            source_archive="/path/archive.zip",
            size=1000,
            is_nested_archive=False
        )
        db.add_file(entry)
        
        assert db.check_quick_hash_exists("existing_quick") is True
        assert db.check_quick_hash_exists("nonexistent") is False
        
        db.close()
    
    def test_update_full_hash(self, tmp_path):
        """Test updating full hash."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        entry = FileEntry(
            full_hash=None,
            quick_hash="quick123",
            filename="file.bin",
            path_in_archive="path/in/archive.bin",
            source_archive="/path/archive.zip",
            size=1000,
            is_nested_archive=False
        )
        db.add_file(entry)
        
        # Update with full hash
        db.update_full_hash("/path/archive.zip", "path/in/archive.bin", "full_hash_123")
        
        # Verify update
        results = db.find_by_full_hash("full_hash_123")
        assert len(results) == 1
        
        db.close()
    
    def test_get_files_by_archive(self, tmp_path):
        """Test getting files by archive."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        # Add files to different archives
        for i in range(5):
            entry = FileEntry(
                full_hash=f"hash{i}",
                quick_hash=None,
                filename=f"file{i}.txt",
                path_in_archive=f"file{i}.txt",
                source_archive="/path/target.zip",
                size=100,
                is_nested_archive=False
            )
            db.add_file(entry)
        
        # Add file to different archive
        other_entry = FileEntry(
            full_hash="other",
            quick_hash=None,
            filename="other.txt",
            path_in_archive="other.txt",
            source_archive="/path/other.zip",
            size=100,
            is_nested_archive=False
        )
        db.add_file(other_entry)
        
        files = db.get_files_by_archive("/path/target.zip")
        assert len(files) == 5
        
        db.close()
    
    def test_unique_constraint(self, tmp_path):
        """Test unique constraint on source_archive + path_in_archive."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        entry1 = FileEntry(
            full_hash="hash1",
            quick_hash=None,
            filename="file.txt",
            path_in_archive="same/path.txt",
            source_archive="/path/archive.zip",
            size=100,
            is_nested_archive=False
        )
        
        entry2 = FileEntry(
            full_hash="hash2",  # Different hash
            quick_hash=None,
            filename="file.txt",
            path_in_archive="same/path.txt",  # Same path
            source_archive="/path/archive.zip",  # Same archive
            size=200,  # Different size
            is_nested_archive=False
        )
        
        db.add_file(entry1)
        db.add_file(entry2)  # Should replace entry1
        
        results = db.get_files_by_archive("/path/archive.zip")
        assert len(results) == 1
        assert results[0].full_hash == "hash2"  # Should have new hash
        
        db.close()


class TestSelectionState:
    """Tests for selection state operations."""
    
    def test_set_and_get_selection(self, tmp_path):
        """Test setting and getting selection state."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        db.set_selection_state("file_hash_123", "/path/to/file.txt", True)
        
        selected = db.get_selection_state("file_hash_123", "/path/to/file.txt")
        assert selected is True
        
        db.close()
    
    def test_set_selection_false(self, tmp_path):
        """Test setting selection to False."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        db.set_selection_state("hash", "/path/file.txt", False)
        
        selected = db.get_selection_state("hash", "/path/file.txt")
        assert selected is False
        
        db.close()
    
    def test_get_selection_nonexistent(self, tmp_path):
        """Test getting selection for non-existent entry."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        selected = db.get_selection_state("unknown", "/unknown/file.txt")
        assert selected is None
        
        db.close()
    
    def test_update_selection(self, tmp_path):
        """Test updating selection state."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        db.set_selection_state("hash", "/path/file.txt", True)
        db.set_selection_state("hash", "/path/file.txt", False)
        
        selected = db.get_selection_state("hash", "/path/file.txt")
        assert selected is False
        
        db.close()
    
    def test_multiple_selections(self, tmp_path):
        """Test multiple selection states."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        selections = [
            ("hash1", "/path/a.txt", True),
            ("hash2", "/path/b.txt", False),
            ("hash3", "/path/c.txt", True),
        ]
        
        for h, p, s in selections:
            db.set_selection_state(h, p, s)
        
        assert db.get_selection_state("hash1", "/path/a.txt") is True
        assert db.get_selection_state("hash2", "/path/b.txt") is False
        assert db.get_selection_state("hash3", "/path/c.txt") is True
        
        db.close()


class TestStatistics:
    """Tests for statistics."""
    
    def test_get_statistics_empty(self, tmp_path):
        """Test statistics for empty database."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        stats = db.get_statistics()
        assert stats['archives'] == 0
        assert stats['files'] == 0
        assert stats['nested_archives'] == 0
        
        db.close()
    
    def test_get_statistics(self, tmp_path):
        """Test statistics calculation."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        # Add archives
        db.update_archive("/path/a.zip", 1.0, 100, 5)
        db.update_archive("/path/b.zip", 2.0, 200, 10)
        
        # Add files
        for i in range(10):
            entry = FileEntry(
                full_hash=f"hash{i}",
                quick_hash=None,
                filename=f"file{i}.txt",
                path_in_archive=f"file{i}.txt",
                source_archive="/path/a.zip",
                size=100,
                is_nested_archive=(i < 3)  # First 3 are nested archives
            )
            db.add_file(entry)
        
        stats = db.get_statistics()
        assert stats['archives'] == 2
        assert stats['files'] == 10
        assert stats['nested_archives'] == 3
        
        db.close()


class TestClearDatabase:
    """Tests for clearing database."""
    
    def test_clear_database(self, tmp_path):
        """Test clearing all data."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        # Add data
        db.update_archive("/path/archive.zip", 1.0, 100, 10)
        entry = FileEntry(
            full_hash="hash",
            quick_hash=None,
            filename="file.txt",
            path_in_archive="file.txt",
            source_archive="/path/archive.zip",
            size=100,
            is_nested_archive=False
        )
        db.add_file(entry)
        db.set_selection_state("hash", "/path/file.txt", True)
        
        # Clear
        db.clear_database()
        
        # Verify empty
        stats = db.get_statistics()
        assert stats['archives'] == 0
        assert stats['files'] == 0
        
        db.close()
    
    def test_clear_preserves_schema(self, tmp_path):
        """Test that clearing preserves table structure."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        db.update_archive("/path/archive.zip", 1.0, 100, 10)
        db.clear_database()
        
        # Should be able to add data again
        db.update_archive("/path/new.zip", 2.0, 200, 20)
        stats = db.get_statistics()
        assert stats['archives'] == 1
        
        db.close()


class TestRowToFileEntry:
    """Tests for row conversion."""
    
    def test_row_to_file_entry(self, tmp_path):
        """Test conversion from database row to FileEntry."""
        db_path = str(tmp_path / "test.db")
        db = DatabaseManager(db_path)
        db.connect()
        
        entry = FileEntry(
            full_hash="hash123",
            quick_hash="quick456",
            filename="test.txt",
            path_in_archive="folder/test.txt",
            source_archive="/path/archive.zip",
            size=2048,
            is_nested_archive=True
        )
        db.add_file(entry)
        
        # Retrieve and verify
        results = db.find_by_full_hash("hash123")
        assert len(results) == 1
        
        retrieved = results[0]
        assert retrieved.full_hash == "hash123"
        assert retrieved.quick_hash == "quick456"
        assert retrieved.filename == "test.txt"
        assert retrieved.path_in_archive == "folder/test.txt"
        assert retrieved.source_archive == "/path/archive.zip"
        assert retrieved.size == 2048
        assert retrieved.is_nested_archive is True
        
        db.close()
