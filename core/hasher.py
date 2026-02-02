"""
Fast file hashing with partial hash optimization.
"""
import xxhash
from pathlib import Path
from typing import Tuple, Optional, BinaryIO
import logging

logger = logging.getLogger(__name__)


class HashCalculator:
    """Handles file hashing with performance optimizations."""
    
    def __init__(self, partial_hash_threshold: int = 1_048_576, 
                 partial_hash_size: int = 8192,
                 chunk_size: int = 65536):
        """
        Initialize hash calculator.
        
        Args:
            partial_hash_threshold: File size threshold for using partial hash (bytes)
            partial_hash_size: Number of bytes to read for partial hash
            chunk_size: Chunk size for streaming hash computation
        """
        self.partial_hash_threshold = partial_hash_threshold
        self.partial_hash_size = partial_hash_size
        self.chunk_size = chunk_size
    
    def hash_file(self, filepath: str, file_size: Optional[int] = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Hash a file with partial hash optimization.
        
        Args:
            filepath: Path to file to hash
            file_size: Known file size (optional, will stat if not provided)
        
        Returns:
            Tuple of (full_hash, quick_hash)
            - For small files: (full_hash, None)
            - For large files: (full_hash, quick_hash) or (None, quick_hash) if no collision
        """
        try:
            path = Path(filepath)
            
            if file_size is None:
                file_size = path.stat().st_size
            
            # Small files: only full hash
            if file_size < self.partial_hash_threshold:
                full_hash = self._compute_full_hash(filepath)
                return full_hash, None
            
            # Large files: partial hash first
            quick_hash = self._compute_partial_hash(filepath)
            
            # Return quick hash for now; full hash computed only if needed during duplicate detection
            return None, quick_hash
            
        except Exception as e:
            logger.error(f"Failed to hash file {filepath}: {e}")
            return None, None
    
    def hash_stream(self, stream: BinaryIO, size: Optional[int] = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Hash a file-like stream (for archive contents).
        
        Args:
            stream: File-like object to hash
            size: Known size (optional)
        
        Returns:
            Tuple of (full_hash, quick_hash)
        """
        try:
            # For streams, we need to read once, so we compute both hashes if needed
            if size is not None and size >= self.partial_hash_threshold:
                return self._compute_dual_hash_stream(stream, size)
            else:
                full_hash = self._compute_full_hash_stream(stream)
                return full_hash, None
        except Exception as e:
            logger.error(f"Failed to hash stream: {e}")
            return None, None
    
    def _compute_partial_hash(self, filepath: str) -> str:
        """Compute quick hash from first bytes of file."""
        hasher = xxhash.xxh3_64()
        
        with open(filepath, 'rb') as f:
            data = f.read(self.partial_hash_size)
            hasher.update(data)
        
        return hasher.hexdigest()
    
    def _compute_full_hash(self, filepath: str) -> str:
        """Compute full hash of entire file using streaming."""
        hasher = xxhash.xxh3_64()
        
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.chunk_size):
                hasher.update(chunk)
        
        return hasher.hexdigest()
    
    def _compute_full_hash_stream(self, stream: BinaryIO) -> str:
        """Compute full hash of a stream."""
        hasher = xxhash.xxh3_64()
        
        while chunk := stream.read(self.chunk_size):
            hasher.update(chunk)
        
        return hasher.hexdigest()
    
    def _compute_dual_hash_stream(self, stream: BinaryIO, size: int) -> Tuple[str, str]:
        """Compute both partial and full hash from stream."""
        quick_hasher = xxhash.xxh3_64()
        full_hasher = xxhash.xxh3_64()
        
        bytes_read = 0
        while chunk := stream.read(self.chunk_size):
            full_hasher.update(chunk)
            
            # Add to quick hash only for first bytes
            if bytes_read < self.partial_hash_size:
                remaining = self.partial_hash_size - bytes_read
                quick_hasher.update(chunk[:remaining])
            
            bytes_read += len(chunk)
        
        return full_hasher.hexdigest(), quick_hasher.hexdigest()
    
    def compute_full_hash_for_quick(self, filepath: str) -> Optional[str]:
        """
        Compute full hash for a file that previously only had a quick hash.
        Used when a quick hash collision is detected.
        
        Args:
            filepath: Path to file
        
        Returns:
            Full hash string or None on error
        """
        try:
            return self._compute_full_hash(filepath)
        except Exception as e:
            logger.error(f"Failed to compute full hash for {filepath}: {e}")
            return None
