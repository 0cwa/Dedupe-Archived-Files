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
    from backports import zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False

try:
    import libarchive
    HAS_LIBARCHIVE = True
except ImportError:
    HAS_LIBARCHIVE = False
    logger.warning("libarchive not available - extended format support disabled")

try:
    import pymsi
    HAS_MSI = True
except ImportError:
    HAS_MSI = False
    logger.warning("python-msi not available - MSI support limited")

try:
    import pefile
    HAS_PEFILE = True
except ImportError:
    HAS_PEFILE = False
    logger.warning("pefile not available - EXE resource extraction disabled")


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
    
    def _detect_magic_handlers(self, archive_path: str) -> List[Any]:
        """Detect potential handlers based on magic bytes."""
        try:
            with open(archive_path, 'rb') as f:
                magic = f.read(262)
            
            handlers = []
            # ZIP
            if magic.startswith(b'PK\x03\x04'):
                handlers.append(self._extract_zip)
            # 7z
            if magic.startswith(b'7z\xbc\xaf\x27\x1c'):
                handlers.append(self._extract_7z)
            # RAR
            if magic.startswith(b'Rar!\x1a\x07'):
                handlers.append(self._extract_rar)
            # TAR and its compressed variants
            if any(magic.startswith(m) for m in [b'\x1f\x8b', b'BZh', b'\xfd7zXZ\x00', b'(\xb5/\xfd']): # GZip, BZip2, XZ, Zstd
                handlers.append(self._extract_tar)
                if HAS_LIBARCHIVE:
                    handlers.append(self._extract_libarchive)
            # TAR (ustar)
            if b'ustar' in magic[257:262]:
                handlers.append(self._extract_tar)
            # MSI/OLE
            if magic.startswith(b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'):
                if HAS_MSI:
                    handlers.append(self._extract_msi)
                if HAS_LIBARCHIVE:
                    handlers.append(self._extract_libarchive)
            # EXE
            if magic.startswith(b'MZ'):
                if HAS_PEFILE:
                    handlers.append(self._extract_exe)
                if HAS_7Z:
                    handlers.append(self._extract_7z)
                handlers.append(self._extract_zip)
                if HAS_LIBARCHIVE:
                    handlers.append(self._extract_libarchive)
            return handlers
        except Exception:
            return []

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
        if recursion_depth == 0:
            logger.info(f"Analyzing archive: {archive_path}")
            
        if recursion_depth > self.max_recursion_depth:
            logger.warning(f"Max recursion depth reached for {archive_path}")
            return
        
        path = Path(archive_path)
        suffix = path.suffix.lower()
        name_lower = path.name.lower()
        
        # 1. Detect handlers based on magic bytes (highest priority)
        magic_handlers = self._detect_magic_handlers(archive_path)
        
        # 2. Build list of potential handlers based on extension
        ext_handlers = []
        if suffix in ('.zip', '.zipx') or any(name_lower.endswith(e) for e in ('.jar', '.war', '.ear')):
            ext_handlers.append(self._extract_zip)
        elif suffix == '.7z' and HAS_7Z:
            ext_handlers.append(self._extract_7z)
        elif suffix == '.rar' and HAS_RAR:
            ext_handlers.append(self._extract_rar)
        elif any(name_lower.endswith(e) for e in ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz', '.tar.zst', '.tzst')):
            ext_handlers.append(self._extract_tar)
        elif suffix == '.msi':
            if HAS_MSI:
                ext_handlers.append(self._extract_msi)
            if HAS_LIBARCHIVE:
                ext_handlers.append(self._extract_libarchive)
        elif suffix in ('.appimage', '.run'):
            ext_handlers.append(self._extract_appimage)
        elif suffix == '.exe':
            if HAS_PEFILE:
                ext_handlers.append(self._extract_exe)
            if HAS_7Z:
                ext_handlers.append(self._extract_7z)
            ext_handlers.append(self._extract_zip)
            if HAS_LIBARCHIVE:
                ext_handlers.append(self._extract_libarchive)
        elif suffix in ('.deb', '.rpm', '.iso', '.img', '.msi', '.cab', '.cpio', '.wim', '.squashfs'):
            if HAS_LIBARCHIVE:
                ext_handlers.append(self._extract_libarchive)
        
        # 3. Combine handlers in order of preference
        # Magic-detected handlers go first, then extension-specific ones, then generic fallbacks
        handlers = []
        for h in magic_handlers + ext_handlers:
            if h not in handlers:
                handlers.append(h)
        
        # Generic fallbacks if not already present
        if HAS_LIBARCHIVE and self._extract_libarchive not in handlers:
            handlers.append(self._extract_libarchive)
        if self._extract_carved not in handlers:
            handlers.append(self._extract_carved)
            
        success = False
        all_errors = []
        
        for handler in handlers:
            try:
                # Try to get at least one item from the generator
                it = handler(archive_path, recursion_depth)
                try:
                    first_item = next(it)
                    yield first_item
                    success = True
                except StopIteration:
                    # No items yielded by this handler
                    continue
                
                # If we got here, we successfully started extraction
                yield from it
                break # Success with this handler
            except Exception as e:
                handler_name = getattr(handler, '__name__', str(handler))
                if success:
                    # We already yielded some items, so we don't want to try other handlers
                    # as that would likely produce duplicate entries.
                    logger.error(f"Extraction interrupted for {archive_path} using {handler_name}: {e}")
                    break
                
                logger.debug(f"Handler {handler_name} failed to start for {archive_path}: {e}")
                all_errors.append(f"{handler_name}: {e}")
                continue
        
        if not success:
            if all_errors:
                error_msg = "; ".join(all_errors)
                logger.error(f"Failed to extract {archive_path}: {error_msg}")
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
        f_to_close = None
        tf = None
        try:
            try:
                tf = tarfile.open(archive_path, 'r:*')
            except Exception as e:
                # Fallback for Zstandard or other formats if tarfile doesn't support them directly
                if HAS_ZSTD:
                    with open(archive_path, 'rb') as f:
                        magic = f.read(4)
                    if magic == b'(\xb5/\xfd':
                        f_to_close = open(archive_path, 'rb')
                        dctx = zstd.ZstdDecompressor()
                        reader = dctx.stream_reader(f_to_close)
                        # tarfile.open can take a fileobj and mode 'r|' for streaming
                        tf = tarfile.open(fileobj=reader, mode='r|')
                    else:
                        raise e
                else:
                    raise e
            
            with tf:
                # Use iteration which works for both normal and streaming mode
                for member in tf:
                    if not member.isfile():
                        continue
                    
                    try:
                        extracted_f = tf.extractfile(member)
                        if extracted_f is None:
                            continue
                        
                        data = extracted_f.read()
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
                    
                    except Exception as member_e:
                        logger.debug(f"Failed to extract {member.name} from TAR {archive_path}: {member_e}")
        
        except Exception as e:
            logger.debug(f"Failed to open TAR {archive_path}: {e}")
            raise
        finally:
            if f_to_close:
                f_to_close.close()
    
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
            raise
    
    def _find_magic_offset(self, file_obj: BinaryIO, magic: bytes, start_offset: int = 0,
                           chunk_size: int = 1024 * 1024) -> Optional[int]:
        """Find the first occurrence of magic bytes in a file-like object."""
        file_obj.seek(start_offset)
        overlap = len(magic) - 1
        buffer = b""
        offset = start_offset

        while True:
            chunk = file_obj.read(chunk_size)
            if not chunk:
                return None

            data = buffer + chunk
            idx = data.find(magic)
            if idx != -1:
                return offset - len(buffer) + idx

            if len(chunk) < chunk_size:
                return None

            buffer = data[-overlap:]
            offset += len(chunk)

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
                                            
                                            # Yield the file (without archive name prefix as it's added by _extract_nested if needed)
                                            yield (str(rel_path), io.BytesIO(data), size, is_nested)
                                            found_any = True
                                            
                                            # If it's a nested archive, recursively extract it
                                            if is_nested and recursion_depth < self.max_recursion_depth:
                                                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file).suffix) as tmp_file:
                                                    tmp_file.write(data)
                                                    tmp_file_path = tmp_file.name
                                                
                                                try:
                                                    for n_path, n_stream, n_size, n_is_archive in self._extract_nested(tmp_file_path, str(rel_path), recursion_depth):
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

        # 2. Fall back to generic carving
        yield from self._extract_carved(archive_path, recursion_depth)

    def _extract_carved(self, archive_path: str, recursion_depth: int) -> Generator:
        """Generic carving extractor for files with embedded archives."""
        try:
            with open(archive_path, 'rb') as f:
                f.seek(0, os.SEEK_END)
                file_size = f.tell()
                f.seek(0)
                
                # Collect potential offsets to try
                potential_offsets = []
                
                # Check for various archive magics
                magics = [
                    (b'hsqs', '.squashfs'),
                    (b'sqsh', '.squashfs'),
                    (b'PK\x03\x04', '.zip'),
                    (b'7z\xbc\xaf\x27\x1c', '.7z'),
                    (b'MSCF', '.cab'),
                    (b'Rar!\x1a\x07\x01\x00', '.rar'),
                ]
                
                for magic, suffix in magics:
                    offset = 0
                    while True:
                        offset = self._find_magic_offset(f, magic, start_offset=offset)
                        if offset is None:
                            break
                        potential_offsets.append((offset, suffix))
                        offset += len(magic)
                        
                # Check for ISO (Type 1)
                for iso_offset in [0x8001, 0x8801, 0x9001]:
                    if file_size > iso_offset + 5:
                        f.seek(iso_offset)
                        if f.read(5) == b'CD001':
                            potential_offsets.append((iso_offset - 0x8000, '.iso'))
                            break

                # Sort and try offsets
                found_any_at_all = False
                archive_name = Path(archive_path).name
                for offset, suffix in sorted(potential_offsets):
                    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                        f.seek(offset)
                        shutil.copyfileobj(f, tmp)
                        tmp_path = tmp.name
                    
                    try:
                        found_at_offset = False
                        # Try primary extraction method via recursion
                        for rel_path, stream, size, is_arch in self._extract_nested(tmp_path, archive_name, recursion_depth):
                            yield (rel_path, stream, size, is_arch)
                            found_at_offset = True
                            found_any_at_all = True
                        
                        if not found_at_offset:
                            # If primary extraction failed, try to find embedded formats in carved content
                            # We don't prefix here because _try_extract_embedded_formats already calls extract_archive recursively
                            for item in self._try_extract_embedded_formats(tmp_path, recursion_depth):
                                yield item
                                found_any_at_all = True
                                
                    except Exception as e:
                        logger.debug(f"Carving extraction failed for offset {offset}: {e}")
                    finally:
                        Path(tmp_path).unlink(missing_ok=True)
                
        except Exception as e:
            logger.debug(f"Carving extraction failed for {archive_path}: {e}")
            raise

    def _try_extract_embedded_formats(self, carved_file_path: str, recursion_depth: int) -> Generator:
        """
        Try to extract embedded archive formats from carved content.
        
        This handles cases where carving finds a magic signature (like SquashFS)
        but the actual content is a different format (like ZIP) embedded after the magic.
        
        Args:
            carved_file_path: Path to the carved temporary file
            recursion_depth: Current recursion depth
            
        Yields:
            Extracted files
        """
        try:
            with open(carved_file_path, 'rb') as f:
                embedded_formats = [
                    (b'PK\x03\x04', '.zip'),  # ZIP
                    (b'7z\xbc\xaf\x27\x1c', '.7z'),  # 7z
                    (b'Rar!\x1a\x07\x01\x00', '.rar'),  # RAR
                    (b'ustar', '.tar'),  # TAR
                    (b'CD001', '.iso'),  # ISO
                    (b'MSCF', '.cab'), # CAB
                ]
                
                for magic, extension in embedded_formats:
                    f.seek(0)
                    offset = self._find_magic_offset(f, magic)
                    if offset is not None and offset > 0: # If offset is 0, it's already what we tried
                        logger.info(f"Found embedded {extension[1:]} format at offset {offset}")
                        
                        with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as embedded_tmp:
                            f.seek(offset)
                            shutil.copyfileobj(f, embedded_tmp)
                            embedded_tmp_path = embedded_tmp.name
                        
                        try:
                            found_any = False
                            for rel_path, stream, size, is_arch in self.extract_archive(embedded_tmp_path, recursion_depth + 1):
                                yield (rel_path, stream, size, is_arch)
                                found_any = True
                            if found_any:
                                return # Only need one successful embedded format
                        except Exception as e:
                            logger.debug(f"Failed to extract embedded {extension[1:]} format: {e}")
                        finally:
                            Path(embedded_tmp_path).unlink(missing_ok=True)
            
        except Exception as e:
            logger.debug(f"Error trying to extract embedded formats: {e}")
    
    def _extract_msi(self, archive_path: str, recursion_depth: int) -> Generator:
        """Extract MSI archive using pymsi and olefile."""
        if not HAS_MSI:
            return

        try:
            import olefile
            if olefile.isOleFile(archive_path):
                with olefile.OleFileIO(archive_path) as ole:
                    for stream_path in ole.listdir():
                        stream_name = "/".join(stream_path)
                        try:
                            with ole.openstream(stream_path) as s:
                                data = s.read()
                                size = len(data)
                                is_nested = self.is_archive(stream_name)
                                yield (stream_name, io.BytesIO(data), size, is_nested)
                                
                                if is_nested and recursion_depth < self.max_recursion_depth:
                                    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(stream_name).suffix) as tmp:
                                        tmp.write(data)
                                        tmp_path = tmp.name
                                    
                                    try:
                                        for n_path, n_stream, n_size, n_is_archive in self._extract_nested(tmp_path, stream_name, recursion_depth):
                                            yield (n_path, n_stream, n_size, n_is_archive)
                                    finally:
                                        Path(tmp_path).unlink(missing_ok=True)
                        except Exception as e:
                            logger.debug(f"Failed to extract stream {stream_name} from MSI: {e}")
            
            # pymsi can also be used to explore tables, but streams are usually where the data is
        except Exception as e:
            logger.debug(f"Failed to open MSI {archive_path}: {e}")
            raise

    def _extract_exe(self, archive_path: str, recursion_depth: int) -> Generator:
        """Extract resources from EXE using pefile and try PyInstaller extraction."""
        found_any = False

        # 1. Try pefile for resources
        if HAS_PEFILE:
            try:
                pe = pefile.PE(archive_path, fast_load=True)
                if hasattr(pe, 'DIRECTORY_ENTRY_RESOURCE'):
                    for type_entry in pe.DIRECTORY_ENTRY_RESOURCE.entries:
                        if hasattr(type_entry, 'directory'):
                            for name_entry in type_entry.directory.entries:
                                if hasattr(name_entry, 'directory'):
                                    for language_entry in name_entry.directory.entries:
                                        try:
                                            data_rva = language_entry.data.struct.OffsetToData
                                            size = language_entry.data.struct.Size
                                            data = pe.get_data(data_rva, size)
                                            
                                            # Construct a name for the resource
                                            res_type = pefile.RESOURCE_TYPE.get(type_entry.id, type_entry.name or f"type_{type_entry.id}")
                                            res_name = name_entry.name or name_entry.id
                                            filename = f"resources/{res_type}/{res_name}"
                                            
                                            is_nested = self.is_archive(filename)
                                            yield (filename, io.BytesIO(data), size, is_nested)
                                            found_any = True
                                            
                                            if is_nested and recursion_depth < self.max_recursion_depth:
                                                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(filename).suffix) as tmp:
                                                    tmp.write(data)
                                                    tmp_path = tmp.name
                                                try:
                                                    for n_path, n_stream, n_size, n_is_archive in self._extract_nested(tmp_path, filename, recursion_depth):
                                                        yield (n_path, n_stream, n_size, n_is_archive)
                                                finally:
                                                    Path(tmp_path).unlink(missing_ok=True)
                                        except Exception as res_e:
                                            logger.debug(f"Failed to extract resource from {archive_path}: {res_e}")
                pe.close()
            except Exception as e:
                logger.debug(f"pefile extraction failed for {archive_path}: {e}")

        # 2. Try PyInstaller extraction if it looks like one
        try:
            with open(archive_path, 'rb') as f:
                content = f.read()
                if b'PyInstaller' in content or b'python' in content.lower():
                    # Use pyinstaller_extractor via subprocess for safety
                    import subprocess
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        try:
                            # Use the CLI tool we found in venv/bin
                            subprocess.run(
                                ["pyinstaller_extractor", os.path.abspath(archive_path)],
                                cwd=tmp_dir,
                                capture_output=True,
                                timeout=60,
                                check=False
                            )
                            
                            # The extractor creates a directory named [filename]_extracted
                            extracted_dir = Path(tmp_dir) / f"{Path(archive_path).name}_extracted"
                            if not extracted_dir.exists():
                                # Try to find any directory created
                                dirs = [d for d in Path(tmp_dir).iterdir() if d.is_dir()]
                                if dirs:
                                    extracted_dir = dirs[0]

                            if extracted_dir.exists():
                                for root, _, files in os.walk(extracted_dir):
                                    for file in files:
                                        full_path = Path(root) / file
                                        rel_path = full_path.relative_to(extracted_dir)
                                        try:
                                            size = full_path.stat().st_size
                                            is_nested = self.is_archive(file)
                                            with open(full_path, 'rb') as ef:
                                                data = ef.read()
                                                yield (str(rel_path), io.BytesIO(data), size, is_nested)
                                                found_any = True
                                                
                                                if is_nested and recursion_depth < self.max_recursion_depth:
                                                    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file).suffix) as tmp:
                                                        tmp.write(data)
                                                        tmp_path = tmp.name
                                                    try:
                                                        for n_path, n_stream, n_size, n_is_archive in self._extract_nested(tmp_path, str(rel_path), recursion_depth):
                                                            yield (n_path, n_stream, n_size, n_is_archive)
                                                    finally:
                                                        Path(tmp_path).unlink(missing_ok=True)
                                        except Exception as ef_e:
                                            logger.debug(f"Failed to read extracted PyInstaller file {rel_path}: {ef_e}")
                        except Exception as pyi_e:
                            logger.debug(f"PyInstaller extraction failed: {pyi_e}")
        except Exception as e:
            logger.debug(f"EXE content check failed: {e}")

        if not found_any:
            # Fallback to carving or other methods if no resources found
            pass

    def _extract_nested(self, temp_path: str, original_name: str, parent_recursion_depth: int) -> Generator:
        """Helper to extract nested archives with proper path tracking."""
        logger.info(f"Extracting subarchive: {original_name}")
        for nested_rel_path, nested_stream, nested_size, nested_is_archive in self.extract_archive(temp_path, parent_recursion_depth + 1):
            # Combine paths: parent_archive/nested_file
            combined_path = f"{original_name}/{nested_rel_path}"
            yield (combined_path, nested_stream, nested_size, nested_is_archive)
