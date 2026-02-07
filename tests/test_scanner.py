"""
Tests for core.scanner module.
"""
import pytest
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from core.scanner import SourceScanner, TargetScanner
from core.models import AppConfig, ScanProgress
from core.database import DatabaseManager
from core.extractor import ExtractionError


class TestSourceScanner:
    """Tests for SourceScanner."""
    
    @pytest.fixture
    def config(self, tmp_path):
        """Create a test configuration."""
        return AppConfig(
            source_dirs=[str(tmp_path / "sources")],
            target_dirs=[str(tmp_path / "targets")],
            min_file_size=0,
            partial_hash_threshold=1024 * 1024,
            recheck_archives=True
        )
    
    @pytest.fixture
    def db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        yield db
        db.close()
    
    def test_init(self, config, db):
        """Test SourceScanner initialization."""
        scanner = SourceScanner(config, db)
        assert scanner.config == config
        assert scanner.db == db
        assert scanner.progress_callback is None
    
    def test_init_with_callback(self, config, db):
        """Test initialization with callback."""
        callback = Mock()
        scanner = SourceScanner(config, db, progress_callback=callback)
        assert scanner.progress_callback == callback
    
    def test_scan_empty_directory(self, config, db, tmp_path):
        """Test scanning empty directory."""
        source_dir = tmp_path / "sources"
        source_dir.mkdir()
        
        scanner = SourceScanner(config, db)
        results = scanner.scan_source_directories()
        
        assert results == {}
    
    def test_scan_single_zip(self, config, db, tmp_path):
        """Test scanning directory with single ZIP."""
        source_dir = tmp_path / "sources"
        source_dir.mkdir()
        
        # Create a ZIP with one file
        zip_path = source_dir / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("hello.txt", "Hello World")
        
        scanner = SourceScanner(config, db)
        results = scanner.scan_source_directories()
        
        assert len(results) == 1
        assert str(zip_path) in results
        assert results[str(zip_path)].file_count == 1
    
    def test_scan_multiple_archives(self, config, db, tmp_path):
        """Test scanning multiple archives."""
        source_dir = tmp_path / "sources"
        source_dir.mkdir()
        
        # Create multiple ZIPs
        for i in range(3):
            zip_path = source_dir / f"archive{i}.zip"
            with zipfile.ZipFile(zip_path, 'w') as zf:
                for j in range(5):
                    zf.writestr(f"file{j}.txt", f"Content {j}")
        
        scanner = SourceScanner(config, db)
        results = scanner.scan_source_directories()
        
        assert len(results) == 3
        for info in results.values():
            assert info.file_count == 5
    
    def test_scan_nested_directory(self, config, db, tmp_path):
        """Test scanning nested directories."""
        source_dir = tmp_path / "sources"
        subdir = source_dir / "sub" / "nested"
        subdir.mkdir(parents=True)
        
        # Create ZIPs at different levels
        for folder in [source_dir, subdir]:
            zip_path = folder / "archive.zip"
            with zipfile.ZipFile(zip_path, 'w') as zf:
                zf.writestr("file.txt", "content")
        
        scanner = SourceScanner(config, db)
        results = scanner.scan_source_directories()
        
        assert len(results) == 2
    
    def test_scan_skips_non_archive(self, config, db, tmp_path):
        """Test that non-archive files are skipped."""
        source_dir = tmp_path / "sources"
        source_dir.mkdir()
        
        # Create non-archive files
        (source_dir / "file.txt").write_text("not an archive")
        (source_dir / "image.png").write_bytes(b"fake png")
        
        # Create one valid archive
        zip_path = source_dir / "valid.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("file.txt", "content")
        
        scanner = SourceScanner(config, db)
        results = scanner.scan_source_directories()
        
        assert len(results) == 1
        assert str(zip_path) in results
    
    def test_scan_respects_min_file_size(self, config, db, tmp_path):
        """Test that min_file_size is respected."""
        source_dir = tmp_path / "sources"
        source_dir.mkdir()
        
        zip_path = source_dir / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("small.txt", "x")  # 1 byte
            zf.writestr("large.txt", "x" * 100)  # 100 bytes
        
        config.min_file_size = 50
        scanner = SourceScanner(config, db)
        results = scanner.scan_source_directories()
        
        # Only large.txt should be stored
        files = db.get_files_by_archive(str(zip_path))
        assert len(files) == 1
        assert files[0].filename == "large.txt"
    
    def test_scan_progress_callback(self, config, db, tmp_path):
        """Test progress callback is called."""
        source_dir = tmp_path / "sources"
        source_dir.mkdir()
        
        zip_path = source_dir / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            for i in range(20):
                zf.writestr(f"file{i}.txt", f"Content {i}")
        
        callback = Mock()
        scanner = SourceScanner(config, db, progress_callback=callback)
        scanner.scan_source_directories()
        
        # Callback should have been called multiple times
        assert callback.called
        # Check that progress updates were sent
        calls = callback.call_args_list
        assert len(calls) > 0
    
    def test_scan_respects_recheck_setting(self, config, db, tmp_path):
        """Test that recheck_archives setting is respected."""
        source_dir = tmp_path / "sources"
        source_dir.mkdir()
        
        zip_path = source_dir / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("file.txt", "content")
        
        # First scan
        scanner = SourceScanner(config, db)
        scanner.scan_source_directories()
        
        # Verify file count
        initial_count = len(db.get_files_by_archive(str(zip_path)))
        assert initial_count == 1
        
        # Scan again with recheck disabled
        config.recheck_archives = False
        scanner2 = SourceScanner(config, db)
        results = scanner2.scan_source_directories()
        
        # Should still find the archive but might skip rescanning
        assert str(zip_path) in results
    
    def test_scan_corrupt_zip(self, config, db, tmp_path):
        """Test handling of corrupt ZIP - should not be marked as checked."""
        source_dir = tmp_path / "sources"
        source_dir.mkdir()

        # Create corrupt ZIP
        corrupt_zip = source_dir / "corrupt.zip"
        corrupt_zip.write_bytes(b'PK\x03\x04\x00\x00\x00\x00')

        scanner = SourceScanner(config, db)
        results = scanner.scan_source_directories()

        # Corrupt archive should not be in results and not marked in database
        assert len(results) == 0  # Archive is NOT listed
        assert db.get_archive_info(str(corrupt_zip)) is None  # Not in database


class TestTargetScanner:
    """Tests for TargetScanner."""
    
    @pytest.fixture
    def config(self, tmp_path):
        """Create a test configuration."""
        return AppConfig(
            source_dirs=[str(tmp_path / "sources")],
            target_dirs=[str(tmp_path / "targets")],
            min_file_size=0,
            partial_hash_threshold=1024 * 1024
        )
    
    @pytest.fixture
    def db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        yield db
        db.close()
    
    def test_init(self, config, db):
        """Test TargetScanner initialization."""
        scanner = TargetScanner(config, db)
        assert scanner.config == config
        assert scanner.db == db
    
    def test_scan_empty_target(self, config, db, tmp_path):
        """Test scanning empty target directory."""
        target_dir = tmp_path / "targets"
        target_dir.mkdir()
        
        scanner = TargetScanner(config, db)
        results = scanner.scan_target_directories()
        
        assert results == {}
    
    def test_scan_finds_duplicates(self, config, db, tmp_path):
        """Test finding duplicate files."""
        from core.models import FileEntry
        
        # Add source file to database
        entry = FileEntry(
            full_hash="duplicate_hash_123",
            quick_hash=None,
            filename="original.txt",
            path_in_archive="original.txt",
            source_archive="/path/source.zip",
            size=100,
            is_nested_archive=False
        )
        db.add_file(entry)
        
        # Create target directory with duplicate
        target_dir = tmp_path / "targets"
        target_dir.mkdir()
        target_file = target_dir / "duplicate.txt"
        target_file.write_bytes(b"x" * 100)  # Different content but we're mocking hash
        
        # Patch hasher to return known hash
        with patch('core.scanner.HashCalculator') as mock_hasher_class:
            mock_hasher = Mock()
            mock_hasher.hash_file.return_value = ("duplicate_hash_123", None)
            mock_hasher_class.return_value = mock_hasher
            
            scanner = TargetScanner(config, db)
            results = scanner.scan_target_directories()
        
        assert len(results) == 1
        assert "/path/source.zip" in results
        assert len(results["/path/source.zip"]) == 1
    
    def test_scan_no_duplicates(self, config, db, tmp_path):
        """Test scanning when no duplicates exist."""
        # Create target directory with file
        target_dir = tmp_path / "targets"
        target_dir.mkdir()
        target_file = target_dir / "unique.txt"
        target_file.write_text("unique content")
        
        scanner = TargetScanner(config, db)
        results = scanner.scan_target_directories()
        
        assert results == {}
    
    def test_scan_respects_min_size(self, config, db, tmp_path):
        """Test that min_file_size is respected in target scan."""
        target_dir = tmp_path / "targets"
        target_dir.mkdir()
        
        # Create files of different sizes
        small_file = target_dir / "small.txt"
        small_file.write_text("x")
        
        large_file = target_dir / "large.txt"
        large_file.write_text("x" * 100)
        
        config.min_file_size = 50
        
        scanner = TargetScanner(config, db)
        files = scanner._find_files(str(target_dir))
        
        # Should only find large file
        assert len(files) == 1
        assert str(large_file) in files
    
    def test_scan_multiple_targets(self, config, db, tmp_path):
        """Test scanning multiple target directories."""
        from core.models import FileEntry
        
        # Add source file to database
        entry = FileEntry(
            full_hash="shared_hash",
            quick_hash=None,
            filename="file.txt",
            path_in_archive="file.txt",
            source_archive="/path/source.zip",
            size=50,
            is_nested_archive=False
        )
        db.add_file(entry)
        
        # Create multiple target directories
        targets = []
        for i in range(3):
            target_dir = tmp_path / f"target{i}"
            target_dir.mkdir()
            target_file = target_dir / f"file{i}.txt"
            target_file.write_bytes(b"y" * 50)
            targets.append(str(target_dir))
        
        config.target_dirs = targets
        
        # Patch hasher to return known hash
        with patch('core.scanner.HashCalculator') as mock_hasher_class:
            mock_hasher = Mock()
            mock_hasher.hash_file.return_value = ("shared_hash", None)
            mock_hasher_class.return_value = mock_hasher
            
            scanner = TargetScanner(config, db)
            results = scanner.scan_target_directories()
        
        # Should find duplicates in all targets
        assert len(results) == 1
        assert len(results["/path/source.zip"]) == 3
    
    def test_scan_progress_callback(self, config, db, tmp_path):
        """Test progress callback during target scan."""
        target_dir = tmp_path / "targets"
        target_dir.mkdir()
        
        # Create many files
        for i in range(250):
            (target_dir / f"file{i}.txt").write_text(f"content {i}")
        
        callback = Mock()
        scanner = TargetScanner(config, db, progress_callback=callback)
        scanner.scan_target_directories()
        
        # Callback should have been called (every 100 files)
        assert callback.called
    
    def test_scan_quick_hash_collision(self, config, db, tmp_path):
        """Test handling of quick hash collision."""
        from core.models import FileEntry
        
        # Add source file with only quick hash
        entry = FileEntry(
            full_hash=None,
            quick_hash="quick_hash_123",
            filename="large.bin",
            path_in_archive="large.bin",
            source_archive="/path/source.zip",
            size=1000000,
            is_nested_archive=False
        )
        db.add_file(entry)
        
        # Create target file
        target_dir = tmp_path / "targets"
        target_dir.mkdir()
        target_file = target_dir / "target.bin"
        target_file.write_bytes(b"z" * 1000000)
        
        # Mock hasher for quick hash match
        with patch('core.scanner.HashCalculator') as mock_hasher_class:
            mock_hasher = Mock()
            mock_hasher.hash_file.return_value = (None, "quick_hash_123")
            mock_hasher.compute_full_hash_for_quick.return_value = "full_hash_456"
            mock_hasher_class.return_value = mock_hasher
            
            scanner = TargetScanner(config, db)
            results = scanner.scan_target_directories()
        
        # Since full hash doesn't match, no duplicates should be found
        assert results == {}


class TestScannerIntegration:
    """Integration tests for scanner modules."""
    
    def test_full_scan_workflow(self, tmp_path):
        """Test complete scan workflow."""
        # Setup directories
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create source archive with known content
        zip_path = source_dir / "source.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("test.txt", "duplicate content here")
        
        # Create duplicate in target
        target_file = target_dir / "duplicate.txt"
        target_file.write_text("duplicate content here")
        
        # Setup config and database
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
            partial_hash_threshold=1024 * 1024
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            # Source scan
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            
            assert len(archive_infos) == 1
            assert str(zip_path) in archive_infos
            
            # Target scan
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find the duplicate
            assert len(duplicates) == 1
            assert str(zip_path) in duplicates
            assert len(duplicates[str(zip_path)]) == 1
            assert duplicates[str(zip_path)][0].target_path == str(target_file)
        
        finally:
            db.close()
    
    def test_scan_with_nested_archives(self, tmp_path):
        """Test scanning with nested archives."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create inner archive
        inner_zip = tmp_path / "inner.zip"
        with zipfile.ZipFile(inner_zip, 'w') as zf:
            zf.writestr("nested.txt", "nested content")
        
        # Create outer archive containing inner
        outer_zip = source_dir / "outer.zip"
        with zipfile.ZipFile(outer_zip, 'w') as zf:
            zf.write(inner_zip, "nested/inner.zip")
        
        # Create matching target file
        target_file = target_dir / "nested.txt"
        target_file.write_text("nested content")
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            
            # Should find files from nested archive too
            all_files = []
            for archive_path in db.get_all_archives():
                all_files.extend(db.get_files_by_archive(archive_path))
            
            # Should have the nested file
            nested_files = [f for f in all_files if f.filename == "nested.txt"]
            assert len(nested_files) >= 1
        
        finally:
            db.close()
