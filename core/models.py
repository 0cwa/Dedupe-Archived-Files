"""
Data models for the Archive Duplicate Finder.
"""
from dataclasses import dataclass, field
from typing import Optional, List
from pathlib import Path


@dataclass
class FileEntry:
    """Represents a file found in an archive or directory."""
    full_hash: Optional[str]  # None if quick hash check failed
    quick_hash: Optional[str]  # For large files
    filename: str
    path_in_archive: str  # Full path within archive, or relative path for filesystem
    source_archive: Optional[str]  # None for filesystem files
    size: int
    is_nested_archive: bool = False
    
    @property
    def display_name(self) -> str:
        """Get display name for UI."""
        return self.filename


@dataclass
class DuplicateMatch:
    """Represents a duplicate file match."""
    source_file: FileEntry  # File from source archive
    target_path: str  # Full filesystem path where duplicate was found
    target_size: int
    selected_for_deletion: bool = True  # Default: selected
    
    @property
    def size_mb(self) -> float:
        """Get size in MB."""
        return self.target_size / (1024 * 1024)


@dataclass
class ArchiveInfo:
    """Information about a source archive."""
    path: str
    mtime: float  # Modification time
    size: int
    last_scanned: Optional[float] = None
    file_count: int = 0
    duplicate_count: int = 0
    duplicates: List[DuplicateMatch] = field(default_factory=list)
    
    @property
    def name(self) -> str:
        """Get archive filename."""
        return Path(self.path).name
    
    def needs_rescan(self, current_mtime: float, current_size: int) -> bool:
        """Check if archive has changed since last scan."""
        if self.last_scanned is None:
            return True
        return self.mtime != current_mtime or self.size != current_size


@dataclass
class ScanProgress:
    """Progress information for scanning operations."""
    phase: str  # "source_scan", "target_scan", "archive_scan"
    current_archive: Optional[str] = None
    current_file: Optional[str] = None
    files_processed: int = 0
    total_files: int = 0
    archives_processed: int = 0
    total_archives: int = 0
    
    @property
    def progress_pct(self) -> float:
        """Get overall progress percentage."""
        if self.total_archives == 0:
            return 0.0
        return (self.archives_processed / self.total_archives) * 100


@dataclass
class AppConfig:
    """Application configuration."""
    source_dirs: List[str] = field(default_factory=list)
    target_dirs: List[str] = field(default_factory=list)
    db_path: str = "./dup_cache.db"
    keep_database: bool = True
    recheck_archives: bool = False
    search_target_archives: bool = False
    dry_run: bool = False
    auto_mode: bool = False
    delete_method: str = "trash"  # "trash" or "permanent"
    auto_select_duplicates: bool = True
    min_file_size: int = 0  # bytes
    partial_hash_threshold: int = 1_048_576  # 1MB
    partial_hash_size: int = 8192  # 8KB
    hash_algorithm: str = "xxhash"
    parallel_workers: int = 4
    
    def validate(self) -> List[str]:
        """Validate configuration, return list of errors."""
        errors = []
        
        if not self.source_dirs and not self.auto_mode:
            errors.append("At least one source directory is required")
        
        if not self.target_dirs and not self.auto_mode:
            errors.append("At least one target directory is required")
        
        if self.delete_method not in ["trash", "permanent"]:
            errors.append("delete_method must be 'trash' or 'permanent'")
        
        if self.min_file_size < 0:
            errors.append("min_file_size must be >= 0")
        
        if self.parallel_workers < 1:
            errors.append("parallel_workers must be >= 1")
        
        return errors
