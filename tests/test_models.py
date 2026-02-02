"""
Tests for core.models module.
"""
import pytest
from dataclasses import fields
from core.models import FileEntry, DuplicateMatch, ArchiveInfo, ScanProgress, AppConfig


class TestFileEntry:
    """Tests for FileEntry dataclass."""
    
    def test_file_entry_creation(self):
        """Test basic FileEntry creation."""
        entry = FileEntry(
            full_hash="abc123",
            quick_hash=None,
            filename="test.txt",
            path_in_archive="docs/test.txt",
            source_archive="/path/to/archive.zip",
            size=1024,
            is_nested_archive=False
        )
        assert entry.full_hash == "abc123"
        assert entry.quick_hash is None
        assert entry.filename == "test.txt"
        assert entry.size == 1024
    
    def test_file_entry_with_quick_hash(self):
        """Test FileEntry with quick hash for large files."""
        entry = FileEntry(
            full_hash=None,
            quick_hash="quick456",
            filename="large.bin",
            path_in_archive="data/large.bin",
            source_archive="/path/to/archive.zip",
            size=10_000_000,
            is_nested_archive=False
        )
        assert entry.full_hash is None
        assert entry.quick_hash == "quick456"
    
    def test_file_entry_nested_archive(self):
        """Test FileEntry for nested archive."""
        entry = FileEntry(
            full_hash="nested789",
            quick_hash=None,
            filename="inner.zip",
            path_in_archive="archives/inner.zip",
            source_archive="/path/to/outer.zip",
            size=2048,
            is_nested_archive=True
        )
        assert entry.is_nested_archive is True
    
    def test_file_entry_display_name(self):
        """Test display_name property."""
        entry = FileEntry(
            full_hash="hash",
            quick_hash=None,
            filename="document.pdf",
            path_in_archive="folder/document.pdf",
            source_archive="/path/archive.zip",
            size=5000,
            is_nested_archive=False
        )
        assert entry.display_name == "document.pdf"
    
    def test_file_entry_zero_size(self):
        """Test FileEntry with zero size (edge case)."""
        entry = FileEntry(
            full_hash="empty",
            quick_hash=None,
            filename="empty.txt",
            path_in_archive="empty.txt",
            source_archive="/path/archive.zip",
            size=0,
            is_nested_archive=False
        )
        assert entry.size == 0
    
    def test_file_entry_none_source_archive(self):
        """Test FileEntry for filesystem file (no source archive)."""
        entry = FileEntry(
            full_hash="fs_hash",
            quick_hash=None,
            filename="local.txt",
            path_in_archive="/home/user/local.txt",
            source_archive=None,
            size=100,
            is_nested_archive=False
        )
        assert entry.source_archive is None


class TestDuplicateMatch:
    """Tests for DuplicateMatch dataclass."""
    
    def test_duplicate_match_creation(self):
        """Test basic DuplicateMatch creation."""
        source = FileEntry(
            full_hash="hash123",
            quick_hash=None,
            filename="file.txt",
            path_in_archive="file.txt",
            source_archive="/path/archive.zip",
            size=1000,
            is_nested_archive=False
        )
        match = DuplicateMatch(
            source_file=source,
            target_path="/home/user/file.txt",
            target_size=1000,
            selected_for_deletion=True
        )
        assert match.target_path == "/home/user/file.txt"
        assert match.target_size == 1000
        assert match.selected_for_deletion is True
    
    def test_duplicate_match_default_selection(self):
        """Test DuplicateMatch defaults to selected."""
        source = FileEntry(
            full_hash="hash",
            quick_hash=None,
            filename="x.txt",
            path_in_archive="x.txt",
            source_archive="/a.zip",
            size=100,
            is_nested_archive=False
        )
        match = DuplicateMatch(
            source_file=source,
            target_path="/target/x.txt",
            target_size=100
        )
        assert match.selected_for_deletion is True
    
    def test_duplicate_match_size_mb(self):
        """Test size_mb property calculation."""
        source = FileEntry(
            full_hash="hash",
            quick_hash=None,
            filename="big.bin",
            path_in_archive="big.bin",
            source_archive="/a.zip",
            size=5_242_880,  # 5 MB
            is_nested_archive=False
        )
        match = DuplicateMatch(
            source_file=source,
            target_path="/target/big.bin",
            target_size=5_242_880
        )
        assert match.size_mb == 5.0
    
    def test_duplicate_match_zero_size(self):
        """Test size_mb with zero size."""
        source = FileEntry(
            full_hash="hash",
            quick_hash=None,
            filename="empty",
            path_in_archive="empty",
            source_archive="/a.zip",
            size=0,
            is_nested_archive=False
        )
        match = DuplicateMatch(
            source_file=source,
            target_path="/target/empty",
            target_size=0
        )
        assert match.size_mb == 0.0


class TestArchiveInfo:
    """Tests for ArchiveInfo dataclass."""
    
    def test_archive_info_creation(self):
        """Test basic ArchiveInfo creation."""
        info = ArchiveInfo(
            path="/path/to/archive.zip",
            mtime=1234567890.0,
            size=10000,
            last_scanned=1234567900.0,
            file_count=50,
            duplicate_count=5
        )
        assert info.path == "/path/to/archive.zip"
        assert info.file_count == 50
    
    def test_archive_info_name_property(self):
        """Test name property extracts filename."""
        info = ArchiveInfo(
            path="/home/user/backups/archive.zip",
            mtime=1.0,
            size=100,
            last_scanned=None,
            file_count=0
        )
        assert info.name == "archive.zip"
    
    def test_archive_info_needs_rescan_true(self):
        """Test needs_rescan returns True when modified."""
        info = ArchiveInfo(
            path="/path/archive.zip",
            mtime=1000.0,
            size=1000,
            last_scanned=1000.0,
            file_count=10
        )
        # Modified time changed
        assert info.needs_rescan(2000.0, 1000) is True
        # Size changed
        assert info.needs_rescan(1000.0, 2000) is True
    
    def test_archive_info_needs_rescan_false(self):
        """Test needs_rescan returns False when unchanged."""
        info = ArchiveInfo(
            path="/path/archive.zip",
            mtime=1000.0,
            size=1000,
            last_scanned=1000.0,
            file_count=10
        )
        assert info.needs_rescan(1000.0, 1000) is False
    
    def test_archive_info_needs_rescan_never_scanned(self):
        """Test needs_rescan returns True when never scanned."""
        info = ArchiveInfo(
            path="/path/archive.zip",
            mtime=1000.0,
            size=1000,
            last_scanned=None,
            file_count=0
        )
        assert info.needs_rescan(1000.0, 1000) is True
    
    def test_archive_info_default_duplicates(self):
        """Test ArchiveInfo defaults to empty duplicates list."""
        info = ArchiveInfo(
            path="/path/archive.zip",
            mtime=1.0,
            size=100
        )
        assert info.duplicates == []
        assert info.duplicate_count == 0


class TestScanProgress:
    """Tests for ScanProgress dataclass."""
    
    def test_scan_progress_creation(self):
        """Test basic ScanProgress creation."""
        progress = ScanProgress(
            phase="source_scan",
            current_archive="archive.zip",
            current_file="file.txt",
            files_processed=100,
            total_files=1000,
            archives_processed=5,
            total_archives=10
        )
        assert progress.phase == "source_scan"
        assert progress.files_processed == 100
    
    def test_scan_progress_progress_pct(self):
        """Test progress_pct calculation."""
        progress = ScanProgress(
            phase="source_scan",
            current_archive="a.zip",
            current_file=None,
            files_processed=50,
            total_files=100,
            archives_processed=5,
            total_archives=10
        )
        assert progress.progress_pct == 50.0
    
    def test_scan_progress_progress_pct_zero(self):
        """Test progress_pct with zero total."""
        progress = ScanProgress(
            phase="source_scan",
            current_archive=None,
            current_file=None,
            files_processed=0,
            total_files=0,
            archives_processed=0,
            total_archives=0
        )
        assert progress.progress_pct == 0.0
    
    def test_scan_progress_progress_pct_partial(self):
        """Test progress_pct with partial completion."""
        progress = ScanProgress(
            phase="target_scan",
            current_archive=None,
            current_file="file.txt",
            files_processed=333,
            total_files=1000,
            archives_processed=1,
            total_archives=3
        )
        # Based on archives: 1/3 = 33.33%
        assert abs(progress.progress_pct - 33.33) < 0.01


class TestAppConfig:
    """Tests for AppConfig dataclass."""
    
    def test_app_config_defaults(self):
        """Test AppConfig default values."""
        config = AppConfig()
        assert config.source_dirs == []
        assert config.target_dirs == []
        assert config.db_path == "./dup_cache.db"
        assert config.keep_database is True
        assert config.recheck_archives is True
        assert config.search_target_archives is False
        assert config.dry_run is False
        assert config.auto_mode is False
        assert config.delete_method == "trash"
        assert config.auto_select_duplicates is True
        assert config.min_file_size == 0
        assert config.partial_hash_threshold == 1_048_576
        assert config.partial_hash_size == 8192
        assert config.hash_algorithm == "xxhash"
        assert config.parallel_workers == 4
    
    def test_app_config_custom_values(self):
        """Test AppConfig with custom values."""
        config = AppConfig(
            source_dirs=["/src1", "/src2"],
            target_dirs=["/tgt"],
            db_path="/tmp/test.db",
            keep_database=False,
            delete_method="permanent",
            min_file_size=1024,
            parallel_workers=8
        )
        assert config.source_dirs == ["/src1", "/src2"]
        assert config.db_path == "/tmp/test.db"
        assert config.delete_method == "permanent"
        assert config.parallel_workers == 8
    
    def test_app_config_validate_empty(self):
        """Test validation with empty config."""
        config = AppConfig()
        errors = config.validate()
        assert "At least one source directory is required" in errors
        assert "At least one target directory is required" in errors
    
    def test_app_config_validate_valid(self):
        """Test validation with valid config."""
        config = AppConfig(
            source_dirs=["/source"],
            target_dirs=["/target"]
        )
        errors = config.validate()
        assert errors == []
    
    def test_app_config_validate_invalid_delete_method(self):
        """Test validation with invalid delete method."""
        config = AppConfig(
            source_dirs=["/source"],
            target_dirs=["/target"],
            delete_method="invalid"
        )
        errors = config.validate()
        assert "delete_method must be 'trash' or 'permanent'" in errors
    
    def test_app_config_validate_negative_min_size(self):
        """Test validation with negative min file size."""
        config = AppConfig(
            source_dirs=["/source"],
            target_dirs=["/target"],
            min_file_size=-100
        )
        errors = config.validate()
        assert "min_file_size must be >= 0" in errors
    
    def test_app_config_validate_zero_workers(self):
        """Test validation with zero workers."""
        config = AppConfig(
            source_dirs=["/source"],
            target_dirs=["/target"],
            parallel_workers=0
        )
        errors = config.validate()
        assert "parallel_workers must be >= 1" in errors
    
    def test_app_config_validate_auto_mode_no_dirs(self):
        """Test validation passes in auto mode without dirs."""
        config = AppConfig(auto_mode=True)
        errors = config.validate()
        # Auto mode should not require source/target dirs
        assert "At least one source directory is required" not in errors
        assert "At least one target directory is required" not in errors
    
    def test_app_config_validate_multiple_errors(self):
        """Test validation returns multiple errors."""
        config = AppConfig(
            min_file_size=-1,
            parallel_workers=0,
            delete_method="wrong"
        )
        errors = config.validate()
        assert len(errors) >= 3
