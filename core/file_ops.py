"""
File operations for deletion and trash management.
"""
from pathlib import Path
from typing import List, Tuple
import logging
import os

try:
    from send2trash import send2trash
    HAS_SEND2TRASH = True
except ImportError:
    HAS_SEND2TRASH = False
    logging.warning("send2trash not available - only permanent deletion available")

logger = logging.getLogger(__name__)


class FileOperations:
    """Handles file deletion with safety features."""
    
    @staticmethod
    def delete_files(filepaths: List[str], use_trash: bool = True, 
                     dry_run: bool = False) -> Tuple[List[str], List[Tuple[str, str]]]:
        """
        Delete or move files to trash.
        
        Args:
            filepaths: List of file paths to delete
            use_trash: If True, move to trash; if False, permanently delete
            dry_run: If True, don't actually delete anything
        
        Returns:
            Tuple of (successful_deletions, failures_with_reasons)
        """
        successful = []
        failures = []
        
        for filepath in filepaths:
            try:
                if dry_run:
                    # In dry run, just check if file exists
                    if Path(filepath).exists():
                        successful.append(filepath)
                        logger.info(f"[DRY RUN] Would delete: {filepath}")
                    else:
                        failures.append((filepath, "File not found"))
                else:
                    # Actually delete
                    if use_trash:
                        if not HAS_SEND2TRASH:
                            failures.append((filepath, "send2trash library not available"))
                            logger.error(f"Cannot move to trash (library missing): {filepath}")
                            continue
                        
                        send2trash(filepath)
                        successful.append(filepath)
                        logger.info(f"Moved to trash: {filepath}")
                    else:
                        # Permanent deletion
                        path = Path(filepath)
                        if path.exists():
                            path.unlink()
                            successful.append(filepath)
                            logger.info(f"Permanently deleted: {filepath}")
                        else:
                            failures.append((filepath, "File not found"))
            
            except PermissionError as e:
                failures.append((filepath, f"Permission denied: {e}"))
                logger.error(f"Permission denied: {filepath}")
            except Exception as e:
                failures.append((filepath, str(e)))
                logger.error(f"Failed to delete {filepath}: {e}")
        
        return successful, failures
    
    @staticmethod
    def get_total_size(filepaths: List[str]) -> int:
        """
        Calculate total size of files.
        
        Args:
            filepaths: List of file paths
        
        Returns:
            Total size in bytes
        """
        total = 0
        for filepath in filepaths:
            try:
                total += Path(filepath).stat().st_size
            except (OSError, FileNotFoundError):
                logger.warning(f"Could not get size of {filepath}")
        return total
    
    @staticmethod
    def format_size(size_bytes: int) -> str:
        """
        Format size in bytes to human-readable string.
        
        Args:
            size_bytes: Size in bytes
        
        Returns:
            Formatted string (e.g., "1.5 GB")
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"
    
    @staticmethod
    def verify_files_exist(filepaths: List[str]) -> Tuple[List[str], List[str]]:
        """
        Verify which files exist and which don't.
        
        Args:
            filepaths: List of file paths to check
        
        Returns:
            Tuple of (existing_files, missing_files)
        """
        existing = []
        missing = []
        
        for filepath in filepaths:
            if Path(filepath).exists():
                existing.append(filepath)
            else:
                missing.append(filepath)
        
        return existing, missing
