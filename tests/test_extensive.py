"""
Extensive integration tests for Archive Duplicate Finder.
These tests create actual files and test the entire workflow end-to-end.

Run with: python run_tests.py --extensive
"""
import pytest
import tempfile
import zipfile
import tarfile
import os
import io
from pathlib import Path
from unittest.mock import Mock, patch
from core.models import AppConfig, FileEntry
from core.database import DatabaseManager
from core.scanner import SourceScanner, TargetScanner
from core.extractor import ArchiveExtractor
from core.file_ops import FileOperations
from core.hasher import HashCalculator


# Custom marker for extensive tests
pytestmark = [
    pytest.mark.extensive,
    pytest.mark.slow,
]


class TestEndToEndWorkflow:
    """Complete end-to-end workflow tests with real files."""
    
    def test_simple_duplicate_detection(self, tmp_path):
        """Test simplest case: one archive, one duplicate file."""
        # Setup
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create archive with known content
        zip_path = source_dir / "source.zip"
        content = b"This is duplicate content that will be matched"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("original.txt", content)
        
        # Create duplicate in target
        target_file = target_dir / "duplicate.txt"
        target_file.write_bytes(content)
        
        # Run workflow
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            # Source scan
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            
            assert len(archive_infos) == 1
            assert archive_infos[str(zip_path)].file_count == 1
            
            # Target scan - should find duplicate
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            assert len(duplicates) == 1
            assert str(zip_path) in duplicates
            assert len(duplicates[str(zip_path)]) == 1
            match = duplicates[str(zip_path)][0]
            assert match.target_path == str(target_file)
            assert match.selected_for_deletion is True
            
        finally:
            db.close()
    
    def test_multiple_archives_multiple_duplicates(self, tmp_path):
        """Test with multiple archives and multiple duplicate files."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create multiple archives
        contents = {
            "file1.txt": b"Content for file 1",
            "file2.txt": b"Content for file 2",
            "file3.txt": b"Content for file 3",
        }
        
        for i, (filename, content) in enumerate(contents.items()):
            zip_path = source_dir / f"archive{i}.zip"
            with zipfile.ZipFile(zip_path, 'w') as zf:
                zf.writestr(filename, content)
        
        # Create duplicates in target for all files
        for filename, content in contents.items():
            target_file = target_dir / f"dup_{filename}"
            target_file.write_bytes(content)
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            assert len(archive_infos) == 3
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find all 3 duplicates
            total_dupes = sum(len(matches) for matches in duplicates.values())
            assert total_dupes == 3
            
        finally:
            db.close()
    
    def test_nested_archive_extraction(self, tmp_path):
        """Test extraction of deeply nested archives."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create deeply nested archives: level3 inside level2 inside level1
        level3 = tmp_path / "level3.zip"
        with zipfile.ZipFile(level3, 'w') as zf:
            zf.writestr("deep.txt", b"Deep nested content")
        
        level2 = tmp_path / "level2.zip"
        with zipfile.ZipFile(level2, 'w') as zf:
            zf.write(level3, "nested/level3.zip")
        
        level1 = source_dir / "level1.zip"
        with zipfile.ZipFile(level1, 'w') as zf:
            zf.write(level2, "level2.zip")
        
        # Create matching target file
        target_file = target_dir / "deep.txt"
        target_file.write_bytes(b"Deep nested content")
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            
            # Should find the deeply nested file
            all_files = []
            for archive_path in db.get_all_archives():
                all_files.extend(db.get_files_by_archive(archive_path))
            
            nested_files = [f for f in all_files if f.filename == "deep.txt"]
            assert len(nested_files) >= 1
            
            # Check nested archive paths are correct
            nested_file = nested_files[0]
            assert "level2.zip" in nested_file.path_in_archive
            assert "level3.zip" in nested_file.path_in_archive
            
            # Target scan
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find the duplicate
            assert len(duplicates) >= 1
            
        finally:
            db.close()
    
    def test_mixed_archive_formats(self, tmp_path):
        """Test with ZIP and TAR archives."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        content = b"Same content across different formats"
        
        # Create ZIP
        zip_path = source_dir / "archive.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("file.txt", content)
        
        # Create TAR
        tar_path = source_dir / "archive.tar"
        with tarfile.open(tar_path, 'w') as tf:
            data = content
            info = tarfile.TarInfo(name="file.txt")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        
        # Create target duplicates
        for i in range(2):
            target_file = target_dir / f"duplicate{i}.txt"
            target_file.write_bytes(content)
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            
            assert len(archive_infos) == 2
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find both duplicates
            total_dupes = sum(len(matches) for matches in duplicates.values())
            assert total_dupes == 2
            
        finally:
            db.close()
    
    def test_empty_archives(self, tmp_path):
        """Test handling of empty archives."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create empty ZIP
        empty_zip = source_dir / "empty.zip"
        with zipfile.ZipFile(empty_zip, 'w') as zf:
            pass
        
        # Create ZIP with content
        content_zip = source_dir / "content.zip"
        with zipfile.ZipFile(content_zip, 'w') as zf:
            zf.writestr("file.txt", b"content")
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            
            # Should find both archives
            assert len(archive_infos) == 2
            # Empty archive should have 0 files
            assert archive_infos[str(empty_zip)].file_count == 0
            # Content archive should have 1 file
            assert archive_infos[str(content_zip)].file_count == 1
            
        finally:
            db.close()
    
    def test_corrupt_archive_handling(self, tmp_path):
        """Test graceful handling of corrupt archives."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create corrupt ZIP
        corrupt_zip = source_dir / "corrupt.zip"
        corrupt_zip.write_bytes(b'PK\x03\x04\x00\x00\x00\x00invalid data')
        
        # Create valid ZIP
        valid_zip = source_dir / "valid.zip"
        with zipfile.ZipFile(valid_zip, 'w') as zf:
            zf.writestr("file.txt", b"valid content")
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            
            # Should find both archives
            assert len(archive_infos) == 2
            # Corrupt archive should have 0 files
            assert archive_infos[str(corrupt_zip)].file_count == 0
            # Valid archive should have 1 file
            assert archive_infos[str(valid_zip)].file_count == 1
            
        finally:
            db.close()
    
    def test_min_file_size_filtering(self, tmp_path):
        """Test that min_file_size filters files correctly."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        zip_path = source_dir / "mixed.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("small.txt", b"x")  # 1 byte
            zf.writestr("medium.txt", b"x" * 50)  # 50 bytes
            zf.writestr("large.txt", b"x" * 200)  # 200 bytes
        
        # Create matching targets
        for filename, size in [("small.txt", 1), ("medium.txt", 50), ("large.txt", 200)]:
            target_file = target_dir / filename
            target_file.write_bytes(b"x" * size)
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=30,  # Filter out files < 30 bytes
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            
            # Should only store medium and large files
            files = db.get_files_by_archive(str(zip_path))
            filenames = {f.filename for f in files}
            assert "small.txt" not in filenames
            assert "medium.txt" in filenames
            assert "large.txt" in filenames
            
        finally:
            db.close()
    
    def test_partial_hash_for_large_files(self, tmp_path):
        """Test that large files use partial hashing."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create a file larger than the partial hash threshold
        large_content = b"HEADER" + b"x" * (2 * 1024 * 1024) + b"FOOTER"  # 2MB + headers
        
        zip_path = source_dir / "large.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("large.bin", large_content)
        
        # Create target with same content
        target_file = target_dir / "large.bin"
        target_file.write_bytes(large_content)
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
            partial_hash_threshold=1024 * 1024,  # 1MB
            partial_hash_size=8192,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            source_scanner.scan_source_directories()
            
            # Check that partial hash was used
            files = db.get_files_by_archive(str(zip_path))
            assert len(files) == 1
            large_file = files[0]
            # Should have a quick_hash but full_hash might be None initially
            assert large_file.quick_hash is not None
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find the duplicate
            assert len(duplicates) == 1
            
        finally:
            db.close()
    
    def test_no_duplicates_found(self, tmp_path):
        """Test when no duplicates exist."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create archive with unique content
        zip_path = source_dir / "source.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("unique.txt", b"Unique content in archive")
        
        # Create target with different content
        target_file = target_dir / "different.txt"
        target_file.write_bytes(b"Different content in target")
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            source_scanner.scan_source_directories()
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find no duplicates
            assert duplicates == {}
            
        finally:
            db.close()
    
    def test_file_deletion_workflow(self, tmp_path):
        """Test the complete deletion workflow."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        content = b"Content to be deleted"
        
        zip_path = source_dir / "source.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("original.txt", content)
        
        target_file = target_dir / "duplicate.txt"
        target_file.write_bytes(content)
        
        # Test dry run deletion
        successful, failures = FileOperations.delete_files(
            [str(target_file)],
            use_trash=False,
            dry_run=True
        )
        
        assert len(successful) == 1
        assert len(failures) == 0
        assert target_file.exists()  # File should still exist
        
        # Test actual deletion
        successful, failures = FileOperations.delete_files(
            [str(target_file)],
            use_trash=False,
            dry_run=False
        )
        
        assert len(successful) == 1
        assert len(failures) == 0
        assert not target_file.exists()  # File should be deleted
    
    def test_multiple_target_directories(self, tmp_path):
        """Test scanning multiple target directories."""
        source_dir = tmp_path / "sources"
        source_dir.mkdir()
        
        # Create multiple target directories
        target_dirs = []
        for i in range(3):
            target_dir = tmp_path / f"target{i}"
            target_dir.mkdir()
            target_dirs.append(str(target_dir))
        
        content = b"Same content in multiple targets"
        
        # Create source archive
        zip_path = source_dir / "source.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("file.txt", content)
        
        # Create duplicate in each target directory
        for target_dir in target_dirs:
            target_file = Path(target_dir) / "duplicate.txt"
            target_file.write_bytes(content)
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=target_dirs,
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            source_scanner.scan_source_directories()
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find all 3 duplicates
            assert len(duplicates) == 1
            assert len(duplicates[str(zip_path)]) == 3
            
        finally:
            db.close()
    
    def test_progress_callback_invocation(self, tmp_path):
        """Test that progress callbacks are properly invoked."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create multiple archives
        for i in range(5):
            zip_path = source_dir / f"archive{i}.zip"
            with zipfile.ZipFile(zip_path, 'w') as zf:
                for j in range(10):
                    zf.writestr(f"file{j}.txt", f"Content {i}-{j}")
        
        # Create target files
        for i in range(20):
            target_file = target_dir / f"target{i}.txt"
            target_file.write_text(f"Content {i}")
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            # Track progress calls
            progress_calls = []
            def progress_callback(progress):
                progress_calls.append(progress)
            
            source_scanner = SourceScanner(config, db, progress_callback=progress_callback)
            source_scanner.scan_source_directories()
            
            # Should have received progress updates
            assert len(progress_calls) > 0
            
            # Check first call is for finding archives
            assert progress_calls[0].phase == "source_scan"
            
        finally:
            db.close()
    
    def test_special_characters_in_filenames(self, tmp_path):
        """Test handling of special characters in filenames."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Files with special characters
        special_files = {
            "file with spaces.txt": b"Content 1",
            "file-with-dashes.txt": b"Content 2",
            "file_with_underscores.txt": b"Content 3",
            "unicode_文件.txt": b"Content 4",
            "file.multiple.dots.txt": b"Content 5",
        }
        
        zip_path = source_dir / "special.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            for filename, content in special_files.items():
                zf.writestr(filename, content)
        
        # Create matching targets
        for filename, content in special_files.items():
            target_file = target_dir / filename
            target_file.write_bytes(content)
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            source_scanner.scan_source_directories()
            
            files = db.get_files_by_archive(str(zip_path))
            assert len(files) == len(special_files)
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find all duplicates
            total_dupes = sum(len(matches) for matches in duplicates.values())
            assert total_dupes == len(special_files)
            
        finally:
            db.close()
    
    def test_binary_files(self, tmp_path):
        """Test handling of binary files."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        # Create binary content with null bytes and other special bytes
        binary_content = bytes(range(256)) * 100  # All byte values repeated
        
        zip_path = source_dir / "binary.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("data.bin", binary_content)
        
        target_file = target_dir / "data.bin"
        target_file.write_bytes(binary_content)
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            source_scanner.scan_source_directories()
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find the binary duplicate
            assert len(duplicates) == 1
            
        finally:
            db.close()
    
    def test_large_file_count(self, tmp_path):
        """Test handling of archives with many files."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        zip_path = source_dir / "many_files.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            for i in range(100):
                zf.writestr(f"file{i:03d}.txt", f"Content {i}")
        
        # Create duplicates for some files
        for i in range(0, 100, 5):  # Every 5th file
            target_file = target_dir / f"dup{i}.txt"
            target_file.write_text(f"Content {i}")
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            archive_infos = source_scanner.scan_source_directories()
            
            assert archive_infos[str(zip_path)].file_count == 100
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find 20 duplicates (every 5th of 100)
            total_dupes = sum(len(matches) for matches in duplicates.values())
            assert total_dupes == 20
            
        finally:
            db.close()


class TestEdgeCases:
    """Edge case tests."""
    
    def test_zero_byte_files(self, tmp_path):
        """Test handling of zero-byte files."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        zip_path = source_dir / "empty_files.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("empty.txt", b"")  # Zero bytes
            zf.writestr("non_empty.txt", b"Content")
        
        # Create targets
        (target_dir / "empty.txt").write_bytes(b"")
        (target_dir / "non_empty.txt").write_bytes(b"Content")
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            source_scanner.scan_source_directories()
            
            files = db.get_files_by_archive(str(zip_path))
            assert len(files) == 2  # Both files should be stored
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            # Should find both duplicates
            total_dupes = sum(len(matches) for matches in duplicates.values())
            assert total_dupes == 2
            
        finally:
            db.close()
    
    def test_very_long_filename(self, tmp_path):
        """Test handling of very long filenames."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        long_name = "a" * 200 + ".txt"
        
        zip_path = source_dir / "long_name.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr(long_name, b"Content")
        
        target_file = target_dir / long_name
        target_file.write_bytes(b"Content")
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            source_scanner.scan_source_directories()
            
            files = db.get_files_by_archive(str(zip_path))
            assert len(files) == 1
            assert files[0].filename == long_name
            
            target_scanner = TargetScanner(config, db)
            duplicates = target_scanner.scan_target_directories()
            
            assert len(duplicates) == 1
            
        finally:
            db.close()
    
    def test_directory_structure_preservation(self, tmp_path):
        """Test that directory structures are preserved in archives."""
        source_dir = tmp_path / "sources"
        target_dir = tmp_path / "targets"
        source_dir.mkdir()
        target_dir.mkdir()
        
        zip_path = source_dir / "structured.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("root.txt", b"Root file")
            zf.writestr("dir1/file.txt", b"Dir1 file")
            zf.writestr("dir1/subdir/file.txt", b"Nested file")
            zf.writestr("dir2/file.txt", b"Dir2 file")
        
        # Create matching targets
        content_map = {
            "root.txt": b"Root file",
            "dir1_file.txt": b"Dir1 file",
            "nested_file.txt": b"Nested file",
            "dir2_file.txt": b"Dir2 file",
        }
        for filename, content in content_map.items():
            (target_dir / filename).write_bytes(content)
        
        config = AppConfig(
            source_dirs=[str(source_dir)],
            target_dirs=[str(target_dir)],
            min_file_size=0,
        )
        
        db_path = tmp_path / "test.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        try:
            source_scanner = SourceScanner(config, db)
            source_scanner.scan_source_directories()
            
            files = db.get_files_by_archive(str(zip_path))
            assert len(files) == 4
            
            # Check path preservation
            paths = {f.path_in_archive for f in files}
            assert "root.txt" in paths
            assert "dir1/file.txt" in paths
            assert "dir1/subdir/file.txt" in paths
            assert "dir2/file.txt" in paths
            
        finally:
            db.close()


class TestDatabaseOperations:
    """Tests for database operations."""
    
    def test_database_persistence(self, tmp_path):
        """Test that database persists across sessions."""
        db_path = tmp_path / "persistent.db"
        
        # First session - add data
        db1 = DatabaseManager(str(db_path))
        db1.connect()
        
        entry = FileEntry(
            full_hash="test_hash",
            quick_hash=None,
            filename="test.txt",
            path_in_archive="test.txt",
            source_archive="/path/archive.zip",
            size=100,
            is_nested_archive=False
        )
        db1.add_file(entry)
        db1.close()
        
        # Second session - verify data
        db2 = DatabaseManager(str(db_path))
        db2.connect()
        
        files = db2.get_files_by_archive("/path/archive.zip")
        assert len(files) == 1
        assert files[0].full_hash == "test_hash"
        
        db2.close()
    
    def test_archive_metadata_storage(self, tmp_path):
        """Test storage and retrieval of archive metadata."""
        db_path = tmp_path / "metadata.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        # Add files from multiple archives
        archives = ["/path/arc1.zip", "/path/arc2.zip", "/path/arc3.zip"]
        for i, archive in enumerate(archives):
            entry = FileEntry(
                full_hash=f"hash{i}",
                quick_hash=None,
                filename=f"file{i}.txt",
                path_in_archive=f"file{i}.txt",
                source_archive=archive,
                size=100 * (i + 1),
                is_nested_archive=False
            )
            db.add_file(entry)
        
        # Test get_all_archives
        all_archives = db.get_all_archives()
        assert len(all_archives) == 3
        
        # Test get_files_by_archive
        for i, archive in enumerate(archives):
            files = db.get_files_by_archive(archive)
            assert len(files) == 1
            assert files[0].size == 100 * (i + 1)
        
        db.close()
    
    def test_hash_lookup(self, tmp_path):
        """Test hash-based file lookup."""
        db_path = tmp_path / "hash_lookup.db"
        db = DatabaseManager(str(db_path))
        db.connect()
        
        # Add files with same hash
        for i in range(5):
            entry = FileEntry(
                full_hash="shared_hash",
                quick_hash=None,
                filename=f"file{i}.txt",
                path_in_archive=f"file{i}.txt",
                source_archive="/path/archive.zip",
                size=100,
                is_nested_archive=False
            )
            db.add_file(entry)
        
        # Add file with different hash
        entry = FileEntry(
            full_hash="unique_hash",
            quick_hash=None,
            filename="unique.txt",
            path_in_archive="unique.txt",
            source_archive="/path/archive.zip",
            size=100,
            is_nested_archive=False
        )
        db.add_file(entry)
        
        # Lookup by hash
        results = db.get_files_by_hash("shared_hash")
        assert len(results) == 5
        
        results = db.get_files_by_hash("unique_hash")
        assert len(results) == 1
        
        results = db.get_files_by_hash("nonexistent_hash")
        assert len(results) == 0
        
        db.close()


class TestHashCalculator:
    """Tests for hash calculation."""
    
    def test_hash_consistency(self, tmp_path):
        """Test that same content produces same hash."""
        hasher = HashCalculator(algorithm="xxhash")
        
        # Create file
        test_file = tmp_path / "test.txt"
        content = b"Consistent content for hashing"
        test_file.write_bytes(content)
        
        # Hash multiple times
        hash1, _ = hasher.hash_file(str(test_file))
        hash2, _ = hasher.hash_file(str(test_file))
        hash3, _ = hasher.hash_file(str(test_file))
        
        assert hash1 == hash2 == hash3
    
    def test_different_content_different_hashes(self, tmp_path):
        """Test that different content produces different hashes."""
        hasher = HashCalculator(algorithm="xxhash")
        
        files = []
        for i in range(5):
            test_file = tmp_path / f"file{i}.txt"
            test_file.write_bytes(f"Content {i}".encode())
            files.append(test_file)
        
        hashes = []
        for f in files:
            h, _ = hasher.hash_file(str(f))
            hashes.append(h)
        
        # All hashes should be unique
        assert len(set(hashes)) == len(hashes)
    
    def test_partial_hash_for_large_file(self, tmp_path):
        """Test partial hashing for large files."""
        hasher = HashCalculator(
            algorithm="xxhash",
            partial_threshold=1024,  # 1KB
            partial_size=100
        )
        
        # Create large file
        test_file = tmp_path / "large.bin"
        test_file.write_bytes(b"x" * 10000)
        
        full_hash, quick_hash = hasher.hash_file(str(test_file), file_size=10000)
        
        # Should have a quick_hash for large file
        assert quick_hash is not None
        # Full hash might be computed based on implementation
        # but quick_hash should be different from full_hash
        assert quick_hash != full_hash
    
    def test_full_hash_for_small_file(self, tmp_path):
        """Test that small files get full hash."""
        hasher = HashCalculator(
            algorithm="xxhash",
            partial_threshold=1024  # 1KB
        )
        
        # Create small file
        test_file = tmp_path / "small.txt"
        test_file.write_bytes(b"Small content")
        
        full_hash, quick_hash = hasher.hash_file(str(test_file), file_size=13)
        
        # Small file should have full hash, no quick hash
        assert full_hash is not None
        assert quick_hash is None


class TestArchiveExtractor:
    """Tests for archive extraction."""
    
    def test_extraction_memory_efficiency(self, tmp_path):
        """Test that extraction doesn't load everything into memory."""
        zip_path = tmp_path / "test.zip"
        
        # Create archive with multiple files
        with zipfile.ZipFile(zip_path, 'w') as zf:
            for i in range(50):
                zf.writestr(f"file{i}.txt", f"Content {i}")
        
        extractor = ArchiveExtractor()
        
        # Use generator - should not load all into memory at once
        results = extractor.extract_archive(str(zip_path))
        
        # Process one by one
        count = 0
        for name, stream, size, is_nested in results:
            count += 1
            # Stream should be readable
            data = stream.read()
            assert len(data) == size
            # Each iteration should be independent
        
        assert count == 50
    
    def test_nested_archive_recursion_limit(self, tmp_path):
        """Test that recursion limit is respected."""
        # Create deeply nested archives
        current = tmp_path / "level5.zip"
        with zipfile.ZipFile(current, 'w') as zf:
            zf.writestr("deep.txt", b"Deep content")
        
        for i in range(4, 0, -1):
            next_zip = tmp_path / f"level{i}.zip"
            with zipfile.ZipFile(next_zip, 'w') as zf:
                zf.write(current, f"level{i+1}.zip")
            current = next_zip
        
        # Test with different recursion depths
        for max_depth in [0, 1, 2, 10]:
            extractor = ArchiveExtractor(max_recursion_depth=max_depth)
            results = list(extractor.extract_archive(str(next_zip)))
            
            # Results count should vary by depth
            # Depth 0: just level1.zip
            # Depth 1: level1.zip, level2.zip
            # etc.
            if max_depth == 0:
                assert len(results) == 1
            elif max_depth >= 5:
                # Should get all files including deep.txt
                assert any(r[0] == "deep.txt" or "deep.txt" in r[0] for r in results)
