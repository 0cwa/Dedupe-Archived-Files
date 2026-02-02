"""
Tests for core.file_ops module.
"""
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from core.file_ops import FileOperations


class TestFileOperationsDelete:
    """Tests for FileOperations.delete_files."""
    
    def test_delete_files_dry_run(self, tmp_path):
        """Test dry run doesn't actually delete."""
        test_file = tmp_path / "to_delete.txt"
        test_file.write_text("content")
        
        successful, failures = FileOperations.delete_files(
            [str(test_file)],
            use_trash=False,
            dry_run=True
        )
        
        assert str(test_file) in successful
        assert failures == []
        assert test_file.exists()  # File should still exist
    
    def test_delete_files_permanent(self, tmp_path):
        """Test permanent deletion."""
        test_file = tmp_path / "permanent.txt"
        test_file.write_text("delete me")
        
        successful, failures = FileOperations.delete_files(
            [str(test_file)],
            use_trash=False,
            dry_run=False
        )
        
        assert str(test_file) in successful
        assert failures == []
        assert not test_file.exists()
    
    def test_delete_files_nonexistent(self, tmp_path):
        """Test deleting non-existent file."""
        nonexistent = tmp_path / "does_not_exist.txt"
        
        successful, failures = FileOperations.delete_files(
            [str(nonexistent)],
            use_trash=False,
            dry_run=False
        )
        
        assert successful == []
        assert len(failures) == 1
        assert str(nonexistent) in [f[0] for f in failures]
    
    def test_delete_files_dry_run_nonexistent(self, tmp_path):
        """Test dry run with non-existent file."""
        nonexistent = tmp_path / "missing.txt"
        
        successful, failures = FileOperations.delete_files(
            [str(nonexistent)],
            use_trash=False,
            dry_run=True
        )
        
        assert successful == []
        assert len(failures) == 1
    
    def test_delete_files_multiple(self, tmp_path):
        """Test deleting multiple files."""
        files = []
        for i in range(5):
            f = tmp_path / f"file{i}.txt"
            f.write_text(f"content {i}")
            files.append(str(f))
        
        successful, failures = FileOperations.delete_files(
            files,
            use_trash=False,
            dry_run=False
        )
        
        assert len(successful) == 5
        assert failures == []
        for f in files:
            assert not Path(f).exists()
    
    @patch('core.file_ops.HAS_SEND2TRASH', True)
    @patch('core.file_ops.send2trash')
    def test_delete_files_trash(self, mock_send2trash, tmp_path):
        """Test trash deletion."""
        test_file = tmp_path / "trash.txt"
        test_file.write_text("trash me")
        
        successful, failures = FileOperations.delete_files(
            [str(test_file)],
            use_trash=True,
            dry_run=False
        )
        
        assert str(test_file) in successful
        mock_send2trash.assert_called_once_with(str(test_file))
    
    @patch('core.file_ops.HAS_SEND2TRASH', False)
    def test_delete_files_trash_no_library(self, tmp_path):
        """Test trash deletion without library."""
        test_file = tmp_path / "trash.txt"
        test_file.write_text("trash me")
        
        successful, failures = FileOperations.delete_files(
            [str(test_file)],
            use_trash=True,
            dry_run=False
        )
        
        assert successful == []
        assert len(failures) == 1
        assert "send2trash library not available" in failures[0][1]
    
    def test_delete_files_partial_failure(self, tmp_path):
        """Test deletion with some failures."""
        existing = tmp_path / "exists.txt"
        existing.write_text("content")
        nonexistent = tmp_path / "does_not_exist.txt"
        
        successful, failures = FileOperations.delete_files(
            [str(existing), str(nonexistent)],
            use_trash=False,
            dry_run=False
        )
        
        assert str(existing) in successful
        assert len(failures) == 1
        assert not existing.exists()


class TestFileOperationsSize:
    """Tests for FileOperations size methods."""
    
    def test_format_size_bytes(self):
        """Test formatting bytes."""
        assert FileOperations.format_size(0) == "0.0 B"
        assert FileOperations.format_size(500) == "500.0 B"
        assert FileOperations.format_size(1023) == "1023.0 B"
    
    def test_format_size_kilobytes(self):
        """Test formatting KB."""
        assert FileOperations.format_size(1024) == "1.0 KB"
        assert FileOperations.format_size(1536) == "1.5 KB"
        assert FileOperations.format_size(10240) == "10.0 KB"
    
    def test_format_size_megabytes(self):
        """Test formatting MB."""
        assert FileOperations.format_size(1024 * 1024) == "1.0 MB"
        assert FileOperations.format_size(5 * 1024 * 1024) == "5.0 MB"
        assert FileOperations.format_size(1.5 * 1024 * 1024) == "1.5 MB"
    
    def test_format_size_gigabytes(self):
        """Test formatting GB."""
        assert FileOperations.format_size(1024 ** 3) == "1.0 GB"
        assert FileOperations.format_size(2.5 * 1024 ** 3) == "2.5 GB"
    
    def test_format_size_terabytes(self):
        """Test formatting TB."""
        assert FileOperations.format_size(1024 ** 4) == "1.0 TB"
    
    def test_format_size_petabytes(self):
        """Test formatting PB."""
        assert FileOperations.format_size(1024 ** 5) == "1.0 PB"
    
    def test_get_total_size(self, tmp_path):
        """Test calculating total size."""
        file1 = tmp_path / "1.txt"
        file1.write_bytes(b"x" * 100)
        
        file2 = tmp_path / "2.txt"
        file2.write_bytes(b"y" * 200)
        
        total = FileOperations.get_total_size([str(file1), str(file2)])
        assert total == 300
    
    def test_get_total_size_empty(self):
        """Test total size of empty list."""
        total = FileOperations.get_total_size([])
        assert total == 0
    
    def test_get_total_size_nonexistent(self, tmp_path):
        """Test total size with non-existent file."""
        existing = tmp_path / "exists.txt"
        existing.write_bytes(b"xxx")
        nonexistent = tmp_path / "missing.txt"
        
        total = FileOperations.get_total_size([str(existing), str(nonexistent)])
        assert total == 3  # Only counts existing
    
    def test_get_total_size_directory(self, tmp_path):
        """Test total size with directories - may vary by filesystem."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        
        total = FileOperations.get_total_size([str(subdir)])
        # Directory size varies by filesystem, just ensure it doesn't crash
        assert isinstance(total, int)
        assert total >= 0


class TestFileOperationsVerify:
    """Tests for FileOperations.verify_files_exist."""
    
    def test_verify_all_exist(self, tmp_path):
        """Test when all files exist."""
        file1 = tmp_path / "1.txt"
        file1.write_text("a")
        file2 = tmp_path / "2.txt"
        file2.write_text("b")
        
        existing, missing = FileOperations.verify_files_exist([str(file1), str(file2)])
        
        assert len(existing) == 2
        assert missing == []
    
    def test_verify_some_missing(self, tmp_path):
        """Test when some files are missing."""
        existing = tmp_path / "exists.txt"
        existing.write_text("x")
        missing = tmp_path / "missing.txt"
        
        existing_list, missing_list = FileOperations.verify_files_exist(
            [str(existing), str(missing)]
        )
        
        assert len(existing_list) == 1
        assert len(missing_list) == 1
        assert str(existing) in existing_list
        assert str(missing) in missing_list
    
    def test_verify_all_missing(self, tmp_path):
        """Test when all files are missing."""
        missing1 = tmp_path / "gone1.txt"
        missing2 = tmp_path / "gone2.txt"
        
        existing, missing = FileOperations.verify_files_exist([str(missing1), str(missing2)])
        
        assert existing == []
        assert len(missing) == 2
    
    def test_verify_empty_list(self):
        """Test with empty list."""
        existing, missing = FileOperations.verify_files_exist([])
        
        assert existing == []
        assert missing == []
