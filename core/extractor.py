"""
Archive extraction with support for multiple formats and recursive extraction.
"""
import zipfile
import tarfile
import tempfile
import shutil
import os
from pathlib import Path
from typing import Generator, Tuple, Optional, BinaryIO, List, Any
import logging
import io

logger = logging.getLogger(__name__)

# Try importing optional archive libraries
try:
    import py7zr
    HAS_7Z = True
except ImportError:
    HAS_7Z = False
    logger.warning("py7zr not available - 7z support disabled")

try:
    import rarfile
    HAS_RAR = True
except ImportError:
    HAS_RAR = False
    logger.warning("rarfile not available - RAR support disabled")

try:
    import libarchive
    HAS_LIBARCHIVE = True
except ImportError:
    HAS_LIBARCHIVE = False
    logger.warning("libarchive not available - extended format support disabled")


class ArchiveExtractor:
    """Handles extraction of various archive formats with recursive support."""
    
    # Archive extensions we can handle
    ARCHIVE_EXTENSIONS = {
        # Common archives
        '.zip', '.jar', '.war', '.ear', '.zipx',
        '.7z',
        '.rar',
        '.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz', '.tar.zst', '.tzst',
        
        # Package formats
        '.rpm', '.deb', '.msi', '.cab', '.ar', '.xar', '.pkg',
        
        # Executables and App Images
        '.exe', '.appimage', '.run',
        
        # Disk and filesystem images
        '.iso', '.img', '.dmg', '.vmdk', '.vdi', '.vhd', '.squashfs',
        
        # Other formats
        '.cpio', '.wim', '.lzh', '.lha', '.lz',
    }
    
    def __init__(self, max_recursion_depth: int = 10):
        """
        Initialize archive extractor.
        
        Args:
            max_recursion_depth: Maximum depth for nested archive extraction
        """
        self.max_recursion_depth = max_recursion_depth
    
    @classmethod
    def is_archive(cls, filename: str) -> bool:
        """Check if a file is a recognized archive format."""
        path = Path(filename).name.lower()
        
        # Check for compound extensions like .tar.gz
        for ext in cls.ARCHIVE_EXTENSIONS:
            if path.endswith(ext):
                return True
        
        return False
    
    def extract_archive(self, archive_path: str, recursion_depth: int = 0) -> Generator[Tuple[str, BinaryIO, int, bool], None, None]:
        """
        Extract files from an archive recursively.
        
        Yields tuples of: (relative_path, file_stream, size, is_nested_archive)
        
        Args:
            archive_path: Path to archive file
            recursion_depth: Current recursion depth (for nested archives)
        
        Yields:
            (path_in_archive, file_stream, size, is_nested_archive)
        """
        if recursion_depth > self.max_recursion_depth:
            logger.warning(f"Max recursion depth reached for {archive_path}")
            return
        
        path = Path(archive_path)
        suffix = path.suffix.lower()
        name_lower = path.name.lower()
        
        # Build list of potential handlers to try
        handlers = []
        
        # Extension-based primary handlers
        if suffix in ('.zip', '.zipx') or any(name_lower.endswith(e) for e in ('.jar', '.war', '.ear')):
            handlers.append(self._extract_zip)
            if HAS_LIBARCHIVE:
                handlers.append(self._extract_libarchive)
        elif suffix == '.7z' and HAS_7Z:
            handlers.append(self._extract_7z)
            if HAS_LIBARCHIVE:
                handlers.append(self._extract_libarchive)
        elif suffix == '.rar' and HAS_RAR:
            handlers.append(self._extract_rar)
            if HAS_LIBARCHIVE:
                handlers.append(self._extract_libarchive)
        elif any(name_lower.endswith(e) for e in ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz', '.tar.zst', '.tzst')):
            handlers.append(self._extract_tar)
            if HAS_LIBARCHIVE:
                handlers.append(self._extract_libarchive)
        elif suffix in ('.deb', '.rpm', '.iso', '.img', '.msi', '.cab', '.cpio', '.wim', '.squashfs'):
            if HAS_LIBARCHIVE:
                handlers.append(self._extract_libarchive)
        
        # Format-specific fallback handlers (SFX, AppImage, etc.)
        if suffix == '.exe':
            if HAS_7Z:
                handlers.append(self._extract_7z)
            handlers.append(self._extract_zip)
            if HAS_LIBARCHIVE:
                handlers.append(self._extract_libarchive)
        elif suffix in ('.appimage', '.run'):
            handlers.append(self._extract_appimage)
            if HAS_7Z:
                handlers.append(self._extract_7z)
            handlers.append(self._extract_zip)
            if HAS_LIBARCHIVE:
                handlers.append(self._extract_libarchive)
        
        # Always add libarchive as a fallback if not already added
        if HAS_LIBARCHIVE and self._extract_libarchive not in handlers:
            handlers.append(self._extract_libarchive)
            
        success = False
        last_exception = None
        
        for handler in handlers:
            try:
                # Try to get at least one item from the generator
                it = handler(archive_path, recursion_depth)
                try:
                    first_item = next(it)
                    yield first_item
                    yield from it
                    success = True
                    break # Success with this handler
                except StopIteration:
                    # No items yielded by this handler
                    continue
            except Exception as e:
                logger.debug(f"Handler {handler.__name__} failed for {archive_path}: {e}")
                last_exception = e
                continue
        
        if not success:
            if last_exception:
                logger.error(f"Failed to extract {archive_path}: {last_exception}")
            elif not handlers:
                logger.error(f"No handler available for {archive_path}")
            else:
                logger.warning(f"Recognized archive but could not extract any files from {archive_path}")
    
    def _extract_zip(self, archive_path: str, recursion_depth: int) -> Generator:
        """Extract ZIP archive."""
        try:
            with zipfile.ZipFile(archive_path, 'r') as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    
                    try:
                        # Read file data
                        data = zf.read(info.filename)
                        size = len(data)
                        
                        # Check if this is a nested archive
                        is_nested = self.is_archive(info.filename)
                        
                        # Yield the file itself first
                        yield (info.filename, io.BytesIO(data), size, is_nested)
                        
                        # If it's a nested archive, recursively extract it
                        if is_nested and recursion_depth < self.max_recursion_depth:
                            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(info.filename).suffix) as tmp:
                                tmp.write(data)
                                tmp_path = tmp.name
                            
                            try:
                                # Recursively extract nested archive
                                for nested_path, nested_stream, nested_size, nested_is_archive in self._extract_nested(tmp_path, info.filename, recursion_depth):
                                    yield (nested_path, nested_stream, nested_size, nested_is_archive)
                            finally:
                                Path(tmp_path).unlink(missing_ok=True)
                    
                    except Exception as e:
                        logger.debug(f"Failed to extract {info.filename} from ZIP {archive_path}: {e}")
        
        except Exception as e:
            logger.debug(f"Failed to open ZIP {archive_path}: {e}")
            raise
    
    def _extract_7z(self, archive_path: str, recursion_depth: int) -> Generator:
        """Extract 7z archive."""
        if not HAS_7Z:
            return
        
        try:
            with py7zr.SevenZipFile(archive_path, 'r') as szf:
                # Get list of files
                file_list = szf.list()
                
                for file_info in file_list:
                    name = file_info.filename
                    
                    # Skip directories
                    if file_info.is_directory:
                        continue
                    
                    try:
                        # Read single file using the newer API
                        szf.reset()
                        file_dict = szf.read([name])
                        
                        if name not in file_dict:
                            continue
                            
                        bio = file_dict[name]
                        data = bio.read() if hasattr(bio, 'read') else bio
                        
                        if isinstance(data, bytes):
                            size = len(data)
                        else:
                            data_bytes = data.getvalue() if hasattr(data, 'getvalue') else str(data).encode()
                            data = data_bytes
                            size = len(data_bytes)
                        
                        is_nested = self.is_archive(name)
                        yield (name, io.BytesIO(data), size, is_nested)
                        
                        if is_nested and recursion_depth < self.max_recursion_depth:
                            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(name).suffix) as tmp:
                                tmp.write(data)
                                tmp_path = tmp.name
                            
                            try:
                                for nested_path, nested_stream, nested_size, nested_is_archive in self._extract_nested(tmp_path, name, recursion_depth):
                                    yield (nested_path, nested_stream, nested_size, nested_is_archive)
                            finally:
                                Path(tmp_path).unlink(missing_ok=True)
                    
                    except Exception as e:
                        logger.debug(f"Failed to extract {name} from 7z {archive_path}: {e}")
        
        except Exception as e:
            logger.debug(f"Failed to open 7z {archive_path}: {e}")
            raise
    
    def _extract_rar(self, archive_path: str, recursion_depth: int) -> Generator:
        """Extract RAR archive."""
        if not HAS_RAR:
            return
        
        try:
            with rarfile.RarFile(archive_path, 'r') as rf:
                for info in rf.infolist():
                    if info.isdir():
                        continue
                    
                    try:
                        data = rf.read(info.filename)
                        size = len(data)
                        
                        is_nested = self.is_archive(info.filename)
                        yield (info.filename, io.BytesIO(data), size, is_nested)
                        
                        if is_nested and recursion_depth < self.max_recursion_depth:
                            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(info.filename).suffix) as tmp:
                                tmp.write(data)
                                tmp_path = tmp.name
                            
                            try:
                                for nested_path, nested_stream, nested_size, nested_is_archive in self._extract_nested(tmp_path, info.filename, recursion_depth):
                                    yield (nested_path, nested_stream, nested_size, nested_is_archive)
                            finally:
                                Path(tmp_path).unlink(missing_ok=True)
                    
                    except Exception as e:
                        logger.debug(f"Failed to extract {info.filename} from RAR {archive_path}: {e}")
        
        except Exception as e:
            logger.debug(f"Failed to open RAR {archive_path}: {e}")
            raise
    
    def _extract_tar(self, archive_path: str, recursion_depth: int) -> Generator:
        """Extract TAR archive (including compressed variants)."""
        try:
            with tarfile.open(archive_path, 'r:*') as tf:
                for member in tf.getmembers():
                    if not member.isfile():
                        continue
                    
                    try:
                        f = tf.extractfile(member)
                        if f is None:
                            continue
                        
                        data = f.read()
                        size = len(data)
                        
                        is_nested = self.is_archive(member.name)
                        yield (member.name, io.BytesIO(data), size, is_nested)
                        
                        if is_nested and recursion_depth < self.max_recursion_depth:
                            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(member.name).suffix) as tmp:
                                tmp.write(data)
                                tmp_path = tmp.name
                            
                            try:
                                for nested_path, nested_stream, nested_size, nested_is_archive in self._extract_nested(tmp_path, member.name, recursion_depth):
                                    yield (nested_path, nested_stream, nested_size, nested_is_archive)
                            finally:
                                Path(tmp_path).unlink(missing_ok=True)
                    
                    except Exception as e:
                        logger.debug(f"Failed to extract {member.name} from TAR {archive_path}: {e}")
        
        except Exception as e:
            logger.debug(f"Failed to open TAR {archive_path}: {e}")
            raise
    
    def _extract_libarchive(self, archive_path: str, recursion_depth: int) -> Generator:
        """Extract archive using libarchive (fallback for various formats)."""
        if not HAS_LIBARCHIVE:
            return
        
        try:
            with libarchive.file_reader(archive_path) as archive:
                for entry in archive:
                    # Some entries might not be regular files (symlinks, etc.)
                    # We only care about files for duplicate detection
                    if not entry.isfile:
                        continue
                    
                    try:
                        # Read entry data
                        data = b''.join(entry.get_blocks())
                        size = len(data)
                        
                        is_nested = self.is_archive(entry.name)
                        yield (entry.name, io.BytesIO(data), size, is_nested)
                        
                        if is_nested and recursion_depth < self.max_recursion_depth:
                            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(entry.name).suffix) as tmp:
                                tmp.write(data)
                                tmp_path = tmp.name
                            
                            try:
                                for nested_path, nested_stream, nested_size, nested_is_archive in self._extract_nested(tmp_path, entry.name, recursion_depth):
                                    yield (nested_path, nested_stream, nested_size, nested_is_archive)
                            finally:
                                Path(tmp_path).unlink(missing_ok=True)
                    
                    except Exception as e:
                        logger.debug(f"Failed to extract {entry.name} from libarchive {archive_path}: {e}")
        
        except Exception as e:
            logger.debug(f"Failed to open archive with libarchive {archive_path}: {e}")
            # Don't raise here, let other handlers try
    
    def _extract_appimage(self, archive_path: str, recursion_depth: int) -> Generator:
        """Specialized extractor for AppImage (using --appimage-extract or carving)."""
        archive_name = Path(archive_path).name
        
        # 1. Try extraction via execution (most reliable on Linux for SquashFS AppImages)
        if os.name == 'posix':
            try:
                # AppImages need to be executable
                original_mode = os.stat(archive_path).st_mode
                if not (original_mode & 0o111):
                    try:
                        os.chmod(archive_path, original_mode | 0o111)
                    except Exception:
                        pass # Might not have permission, try anyway
                
                with tempfile.TemporaryDirectory() as tmp_dir:
                    # --appimage-extract extracts to 'squashfs-root' in the CWD
                    try:
                        import subprocess
                        subprocess.run(
                            [os.path.abspath(archive_path), "--appimage-extract"],
                            cwd=tmp_dir,
                            capture_output=True,
                            timeout=30,
                            check=False
                        )
                        
                        extract_path = Path(tmp_dir) / "squashfs-root"
                        if extract_path.exists():
                            found_any = False
                            for root, dirs, files in os.walk(extract_path):
                                for file in files:
                                    full_path = Path(root) / file
                                    if not full_path.is_file():
                                        continue
                                        
                                    rel_path = full_path.relative_to(extract_path)
                                    try:
                                        with open(full_path, 'rb') as f:
                                            data = f.read()
                                            size = len(data)
                                            is_nested = self.is_archive(str(rel_path))
                                            
                                            # Yield the file
                                            yield (f"{archive_name}/{rel_path}", io.BytesIO(data), size, is_nested)
                                            found_any = True
                                            
                                            # If it's a nested archive, recursively extract it
                                            if is_nested and recursion_depth < self.max_recursion_depth:
                                                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file).suffix) as tmp_file:
                                                    tmp_file.write(data)
                                                    tmp_file_path = tmp_file.name
                                                
                                                try:
                                                    for n_path, n_stream, n_size, n_is_archive in self._extract_nested(tmp_file_path, f"{archive_name}/{rel_path}", recursion_depth):
                                                        yield (n_path, n_stream, n_size, n_is_archive)
                                                finally:
                                                    Path(tmp_file_path).unlink(missing_ok=True)
                                    except Exception as e:
                                        logger.debug(f"Failed to read extracted AppImage file {rel_path}: {e}")
                            
                            if found_any:
                                return # Success with execution approach
                    except (subprocess.SubprocessError, OSError) as e:
                        logger.debug(f"AppImage execution failed: {e}")
            except Exception as e:
                logger.debug(f"AppImage execution approach failed: {e}")

        # 2. Fall back to carving (Type 2 SquashFS/ISO/ZIP)
        try:
            with open(archive_path, 'rb') as f:
                # AppImage Type 2 has magic 'AI\x02' at offset 8
                f.seek(8)
                magic = f.read(3)
                is_type2 = (magic == b'AI\x02')
                
                f.seek(0)
                # Read head to find squashfs/iso/zip. 10MB should cover most runtimes.
                head = f.read(10 * 1024 * 1024)
                
                # Collect potential offsets to try
                potential_offsets = []
                
                # Try SquashFS (Type 2)
                sqfs_found = False
                for m in [b'hsqs', b'sqsh']:
                    # AppImage SquashFS is usually at the end of the runtime (>= 32KB).
                    # We prioritize offsets after 32KB to avoid false positives in ELF headers,
                    # but fall back to 0 for non-standard or small AppImages (and tests).
                    for start_pos in [32 * 1024, 0]:
                        idx = head.find(m, start_pos)
                        if idx != -1:
                            potential_offsets.append((idx, '.squashfs'))
                            sqfs_found = True
                            break
                    if sqfs_found: break
                
                # Check for ISO (Type 1)
                for iso_offset in [0x8001, 0x8801, 0x9001]:
                    if len(head) > iso_offset + 5 and head[iso_offset:iso_offset+5] == b'CD001':
                        potential_offsets.append((iso_offset - 0x8000, '.iso'))
                        break

                # Check for ZIP (some SFX/AppImages)
                zip_idx = head.find(b'PK\x03\x04')
                if zip_idx != -1:
                    potential_offsets.append((zip_idx, '.zip'))

                # Sort and try offsets to find the one that works
                for offset, suffix in sorted(potential_offsets):
                    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                        f.seek(offset)
                        shutil.copyfileobj(f, tmp)
                        tmp_path = tmp.name
                    
                    try:
                        # Use _extract_nested to properly handle path prefixing and recursion.
                        found_any = False
                        for nested_path, nested_stream, nested_size, nested_is_archive in self._extract_nested(tmp_path, archive_name, recursion_depth):
                            yield (nested_path, nested_stream, nested_size, nested_is_archive)
                            found_any = True
                        
                        if found_any:
                            return # Success!
                    finally:
                        Path(tmp_path).unlink(missing_ok=True)

        except Exception as e:
            logger.debug(f"AppImage carving extraction failed: {e}")

    def _extract_nested(self, temp_path: str, original_name: str, parent_recursion_depth: int) -> Generator:
        """Helper to extract nested archives with proper path tracking."""
        for nested_rel_path, nested_stream, nested_size, nested_is_archive in self.extract_archive(temp_path, parent_recursion_depth + 1):
            # Combine paths: parent_archive/nested_file
            combined_path = f"{original_name}/{nested_rel_path}"
            yield (combined_path, nested_stream, nested_size, nested_is_archive)
