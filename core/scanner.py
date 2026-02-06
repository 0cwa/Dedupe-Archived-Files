"""
Scanner for source archives and target directories.
"""
from pathlib import Path
from typing import List, Dict, Callable, Optional
import os
import logging
from multiprocessing import Pool, Manager
from queue import Empty

from .models import FileEntry, DuplicateMatch, ArchiveInfo, ScanProgress, AppConfig
from .database import DatabaseManager
from .hasher import HashCalculator
from .extractor import ArchiveExtractor

logger = logging.getLogger(__name__)


class SourceScanner:
    """Scans source archives and extracts file hashes."""
    
    def __init__(self, config: AppConfig, db: DatabaseManager, 
                 progress_callback: Optional[Callable[[ScanProgress], None]] = None):
        """
        Initialize source scanner.
        
        Args:
            config: Application configuration
            db: Database manager
            progress_callback: Optional callback for progress updates
        """
        self.config = config
        self.db = db
        self.hasher = HashCalculator(
            partial_hash_threshold=config.partial_hash_threshold,
            partial_hash_size=config.partial_hash_size
        )
        self.extractor = ArchiveExtractor()
        self.progress_callback = progress_callback
    
    def scan_source_directories(self) -> Dict[str, ArchiveInfo]:
        """
        Scan all source directories for archives and extract file hashes.
        
        Returns:
            Dictionary mapping archive paths to ArchiveInfo objects
        """
        # Report that we're finding archives
        if self.progress_callback:
            self.progress_callback(ScanProgress(
                phase="source_scan",
                current_archive="Finding archives...",
                archives_processed=0,
                total_archives=0
            ))
        
        # Find all archives
        archives = []
        for source_dir in self.config.source_dirs:
            archives.extend(self._find_archives(source_dir))
        
        logger.info(f"Found {len(archives)} archives to scan")
        
        # Scan each archive
        archive_infos = {}
        for idx, archive_path in enumerate(archives):
            try:
                info = self._scan_archive(archive_path, idx, len(archives))
                archive_infos[archive_path] = info
            except Exception as e:
                logger.error(f"Failed to scan archive {archive_path}: {e}")
        
        return archive_infos
    
    def _find_archives(self, directory: str) -> List[str]:
        """Recursively find all archive files in a directory."""
        archives = []
        dir_path = Path(directory)
        
        if not dir_path.exists():
            logger.warning(f"Directory does not exist: {directory}")
            return archives
        
        for root, dirs, files in os.walk(directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                if self.extractor.is_archive(filename):
                    archives.append(filepath)
        
        return archives
    
    def _scan_archive(self, archive_path: str, archive_idx: int, total_archives: int) -> ArchiveInfo:
        """
        Scan a single archive and store file hashes.
        
        Args:
            archive_path: Path to archive file
            archive_idx: Current archive index (for progress)
            total_archives: Total number of archives
        
        Returns:
            ArchiveInfo object
        """
        path = Path(archive_path)
        stat = path.stat()
        mtime = stat.st_mtime
        size = stat.st_size
        
        # Check if we need to rescan
        if self.config.recheck_archives:
            existing_info = self.db.get_archive_info(archive_path)
            if existing_info and not existing_info.needs_rescan(mtime, size):
                logger.info(f"Skipping unchanged archive: {path.name}")
                if self.progress_callback:
                    progress = ScanProgress(
                        phase="source_scan",
                        current_archive=path.name,
                        archives_processed=archive_idx + 1,
                        total_archives=total_archives
                    )
                    self.progress_callback(progress)
                return existing_info
        
        logger.info(f"Scanning archive [{archive_idx + 1}/{total_archives}]: {path.name}")
        
        # Extract and hash files
        file_entries = []
        files_processed = 0
        
        for path_in_archive, file_stream, file_size, is_nested_archive in self.extractor.extract_archive(archive_path):
            # Skip files below minimum size
            if file_size < self.config.min_file_size:
                continue
            
            # Hash the file
            full_hash, quick_hash = self.hasher.hash_stream(file_stream, file_size)
            
            if full_hash or quick_hash:
                file_entry = FileEntry(
                    full_hash=full_hash,
                    quick_hash=quick_hash,
                    filename=Path(path_in_archive).name,
                    path_in_archive=path_in_archive,
                    source_archive=archive_path,
                    size=file_size,
                    is_nested_archive=is_nested_archive
                )
                file_entries.append(file_entry)
                files_processed += 1
                
                # Update progress
                if self.progress_callback and files_processed % 10 == 0:
                    progress = ScanProgress(
                        phase="source_scan",
                        current_archive=path.name,
                        current_file=path_in_archive,
                        files_processed=files_processed,
                        archives_processed=archive_idx,
                        total_archives=total_archives
                    )
                    self.progress_callback(progress)
        
        # Store in database
        self.db.add_files_batch(file_entries)
        self.db.update_archive(archive_path, mtime, size, len(file_entries))
        
        logger.info(f"Extracted {len(file_entries)} files from {path.name}")
        
        # Final progress update
        if self.progress_callback:
            progress = ScanProgress(
                phase="source_scan",
                current_archive=path.name,
                files_processed=files_processed,
                archives_processed=archive_idx + 1,
                total_archives=total_archives
            )
            self.progress_callback(progress)
        
        archive_info = ArchiveInfo(
            path=archive_path,
            mtime=mtime,
            size=size,
            file_count=len(file_entries)
        )
        
        return archive_info


class TargetScanner:
    """Scans target directories for duplicate files."""
    
    def __init__(self, config: AppConfig, db: DatabaseManager,
                 progress_callback: Optional[Callable[[ScanProgress], None]] = None):
        """
        Initialize target scanner.
        
        Args:
            config: Application configuration
            db: Database manager
            progress_callback: Optional callback for progress updates
        """
        self.config = config
        self.db = db
        self.hasher = HashCalculator(
            partial_hash_threshold=config.partial_hash_threshold,
            partial_hash_size=config.partial_hash_size
        )
        self.progress_callback = progress_callback
    
    def scan_target_directories(self) -> Dict[str, List[DuplicateMatch]]:
        """
        Scan target directories for files matching source archive hashes.
        
        Returns:
            Dictionary mapping source archive paths to lists of DuplicateMatch objects
        """
        duplicates_by_archive: Dict[str, List[DuplicateMatch]] = {}
        
        # Get all files from target directories
        target_files = []
        for target_dir in self.config.target_dirs:
            target_files.extend(self._find_files(target_dir))
        
        logger.info(f"Found {len(target_files)} files in target directories")
        
        # Report total files found before starting scan
        if self.progress_callback:
            progress = ScanProgress(
                phase="target_scan",
                total_files=len(target_files),
                files_processed=0
            )
            self.progress_callback(progress)
        
        # Check each file for duplicates
        match_count = 0
        for idx, filepath in enumerate(target_files):
            try:
                matches = self._check_file(filepath, idx, len(target_files))
                
                # Group matches by source archive
                for match in matches:
                    archive_path = match.source_file.source_archive
                    if archive_path not in duplicates_by_archive:
                        duplicates_by_archive[archive_path] = []
                    duplicates_by_archive[archive_path].append(match)
                    match_count += 1
                
                # Report progress every 10 files for smoother updates
                if self.progress_callback and idx % 10 == 0:
                    progress = ScanProgress(
                        phase="target_scan",
                        current_file=filepath,
                        files_processed=idx,
                        total_files=len(target_files),
                        archives_processed=match_count
                    )
                    self.progress_callback(progress)
            
            except Exception as e:
                logger.error(f"Failed to check file {filepath}: {e}")
        
        logger.info(f"Found {sum(len(v) for v in duplicates_by_archive.values())} duplicate files")
        
        return duplicates_by_archive
    
    def _find_files(self, directory: str) -> List[str]:
        """Recursively find all files in a directory."""
        files = []
        dir_path = Path(directory)
        
        if not dir_path.exists():
            logger.warning(f"Directory does not exist: {directory}")
            return files
        
        for root, dirs, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(root, filename)
                try:
                    # Skip if below minimum size
                    if os.path.getsize(filepath) >= self.config.min_file_size:
                        files.append(filepath)
                except OSError:
                    logger.warning(f"Could not access file: {filepath}")
        
        return files
    
    def _check_file(self, filepath: str, file_idx: int, total_files: int) -> List[DuplicateMatch]:
        """
        Check if a file matches any hashes in the database.
        
        Args:
            filepath: Path to file to check
            file_idx: Current file index (for progress)
            total_files: Total number of files
        
        Returns:
            List of DuplicateMatch objects
        """
        try:
            stat = Path(filepath).stat()
            file_size = stat.st_size
            
            # Hash the file
            full_hash, quick_hash = self.hasher.hash_file(filepath, file_size)
            
            # Look for matches
            matches = []
            
            # Check full hash if available
            if full_hash:
                source_files = self.db.find_by_full_hash(full_hash)
                for source_file in source_files:
                    # Get selection state from database or use config default
                    stored_state = self.db.get_selection_state(full_hash, filepath)
                    selected = stored_state if stored_state is not None else self.config.auto_select_duplicates
                    
                    match = DuplicateMatch(
                        source_file=source_file,
                        target_path=filepath,
                        target_size=file_size,
                        selected_for_deletion=selected
                    )
                    matches.append(match)
            
            # Check quick hash if no full hash
            elif quick_hash:
                # Check if quick hash exists in database
                if self.db.check_quick_hash_exists(quick_hash):
                    # Compute full hash to verify
                    full_hash = self.hasher.compute_full_hash_for_quick(filepath)
                    if full_hash:
                        source_files = self.db.find_by_full_hash(full_hash)
                        for source_file in source_files:
                            stored_state = self.db.get_selection_state(full_hash, filepath)
                            selected = stored_state if stored_state is not None else self.config.auto_select_duplicates
                            
                            match = DuplicateMatch(
                                source_file=source_file,
                                target_path=filepath,
                                target_size=file_size,
                                selected_for_deletion=selected
                            )
                            matches.append(match)
            
            return matches
        
        except Exception as e:
            logger.error(f"Error checking file {filepath}: {e}")
            return []
