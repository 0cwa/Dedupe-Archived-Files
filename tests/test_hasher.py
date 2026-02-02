"""
Tests for core.hasher module.
"""
import pytest
import tempfile
import os
from pathlib import Path
from core.hasher import HashCalculator


class TestHashCalculator:
    """Tests for HashCalculator class."""
    
    def test_init_defaults(self):
        """Test HashCalculator initialization with defaults."""
        hasher = HashCalculator()
        assert hasher.partial_hash_threshold == 1_048_576  # 1MB
        assert hasher.partial_hash_size == 8192  # 8KB
        assert hasher.chunk_size == 65536  # 64KB
    
    def test_init_custom(self):
        """Test HashCalculator with custom values."""
        hasher = HashCalculator(
            partial_hash_threshold=500_000,
            partial_hash_size=4096,
            chunk_size=32768
        )
        assert hasher.partial_hash_threshold == 500_000
        assert hasher.partial_hash_size == 4096
        assert hasher.chunk_size == 32768
    
    def test_hash_file_small(self, tmp_path):
        """Test hashing a small file (below threshold)."""
        hasher = HashCalculator(partial_hash_threshold=1024)
        
        # Create small file (100 bytes)
        test_file = tmp_path / "small.txt"
        test_file.write_bytes(b"x" * 100)
        
        full_hash, quick_hash = hasher.hash_file(str(test_file))
        
        assert full_hash is not None
        assert isinstance(full_hash, str)
        assert quick_hash is None  # No quick hash for small files
    
    def test_hash_file_large(self, tmp_path):
        """Test hashing a large file (above threshold)."""
        hasher = HashCalculator(partial_hash_threshold=100)  # Low threshold for testing
        
        # Create large file (1KB)
        test_file = tmp_path / "large.bin"
        test_file.write_bytes(b"y" * 1024)
        
        full_hash, quick_hash = hasher.hash_file(str(test_file))
        
        assert full_hash is None  # Only quick hash computed initially
        assert quick_hash is not None
        assert isinstance(quick_hash, str)
    
    def test_hash_file_with_size(self, tmp_path):
        """Test hashing with provided file size."""
        hasher = HashCalculator(partial_hash_threshold=1024)
        
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"test content")
        
        full_hash, quick_hash = hasher.hash_file(str(test_file), file_size=100)
        
        assert full_hash is not None
        assert quick_hash is None
    
    def test_hash_file_nonexistent(self, tmp_path):
        """Test hashing a non-existent file."""
        hasher = HashCalculator()
        
        nonexistent = tmp_path / "does_not_exist.txt"
        full_hash, quick_hash = hasher.hash_file(str(nonexistent))
        
        assert full_hash is None
        assert quick_hash is None
    
    def test_hash_file_empty(self, tmp_path):
        """Test hashing an empty file."""
        hasher = HashCalculator()
        
        empty_file = tmp_path / "empty.txt"
        empty_file.write_bytes(b"")
        
        full_hash, quick_hash = hasher.hash_file(str(empty_file))
        
        assert full_hash is not None  # Empty file has a hash
        assert quick_hash is None
    
    def test_hash_file_consistency(self, tmp_path):
        """Test that same file produces same hash."""
        hasher = HashCalculator()
        
        test_file = tmp_path / "consistent.txt"
        test_file.write_bytes(b"same content here")
        
        hash1, _ = hasher.hash_file(str(test_file))
        hash2, _ = hasher.hash_file(str(test_file))
        
        assert hash1 == hash2
    
    def test_hash_file_different_content(self, tmp_path):
        """Test different content produces different hashes."""
        hasher = HashCalculator()
        
        file1 = tmp_path / "file1.txt"
        file1.write_bytes(b"content A")
        
        file2 = tmp_path / "file2.txt"
        file2.write_bytes(b"content B")
        
        hash1, _ = hasher.hash_file(str(file1))
        hash2, _ = hasher.hash_file(str(file2))
        
        assert hash1 != hash2
    
    def test_hash_stream_small(self):
        """Test hashing a small stream."""
        hasher = HashCalculator(partial_hash_threshold=1024)
        
        import io
        stream = io.BytesIO(b"stream content")
        
        full_hash, quick_hash = hasher.hash_stream(stream, size=100)
        
        assert full_hash is not None
        assert quick_hash is None
    
    def test_hash_stream_large(self):
        """Test hashing a large stream."""
        hasher = HashCalculator(partial_hash_threshold=100)
        
        import io
        stream = io.BytesIO(b"x" * 1000)
        
        full_hash, quick_hash = hasher.hash_stream(stream, size=1000)
        
        assert full_hash is not None
        assert quick_hash is not None
    
    def test_hash_stream_no_size(self):
        """Test hashing stream without size."""
        hasher = HashCalculator()
        
        import io
        stream = io.BytesIO(b"small")
        
        full_hash, quick_hash = hasher.hash_stream(stream)
        
        assert full_hash is not None
        assert quick_hash is None
    
    def test_compute_partial_hash(self, tmp_path):
        """Test partial hash computation."""
        hasher = HashCalculator(partial_hash_size=10)
        
        test_file = tmp_path / "partial.txt"
        test_file.write_bytes(b"12345678901234567890")  # 20 bytes
        
        partial = hasher._compute_partial_hash(str(test_file))
        
        assert partial is not None
        assert isinstance(partial, str)
    
    def test_compute_full_hash(self, tmp_path):
        """Test full hash computation."""
        hasher = HashCalculator()
        
        test_file = tmp_path / "full.txt"
        test_file.write_bytes(b"complete content for hashing")
        
        full = hasher._compute_full_hash(str(test_file))
        
        assert full is not None
        assert isinstance(full, str)
    
    def test_compute_full_hash_for_quick(self, tmp_path):
        """Test computing full hash from quick hash file."""
        hasher = HashCalculator()
        
        test_file = tmp_path / "tocompute.txt"
        test_file.write_bytes(b"compute full hash from this")
        
        full = hasher.compute_full_hash_for_quick(str(test_file))
        
        assert full is not None
        assert isinstance(full, str)
    
    def test_compute_full_hash_for_quick_nonexistent(self, tmp_path):
        """Test computing full hash for non-existent file."""
        hasher = HashCalculator()
        
        nonexistent = tmp_path / "missing.txt"
        full = hasher.compute_full_hash_for_quick(str(nonexistent))
        
        assert full is None
    
    def test_large_file_chunked(self, tmp_path):
        """Test hashing large file with chunking."""
        hasher = HashCalculator(chunk_size=1024)
        
        # Create 10KB file
        test_file = tmp_path / "large.bin"
        test_file.write_bytes(b"x" * 10240)
        
        full_hash, _ = hasher.hash_file(str(test_file))
        
        assert full_hash is not None
    
    def test_partial_vs_full_consistency(self, tmp_path):
        """Test that partial hash is consistent with start of file."""
        hasher = HashCalculator(
            partial_hash_threshold=1,  # Always use partial
            partial_hash_size=10
        )
        
        test_file = tmp_path / "test.bin"
        content = b"0123456789abcdefghij"
        test_file.write_bytes(content)
        
        _, quick_hash = hasher.hash_file(str(test_file))
        
        # Compute expected partial hash manually
        import xxhash
        expected = xxhash.xxh3_64(content[:10]).hexdigest()
        
        assert quick_hash == expected
