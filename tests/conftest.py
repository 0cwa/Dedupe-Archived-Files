"""
Pytest configuration and fixtures for Archive Duplicate Finder tests.
"""
import pytest
import sys
from pathlib import Path

# Ensure project root is in path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_config():
    """Create a sample AppConfig for testing."""
    from core.models import AppConfig
    return AppConfig(
        source_dirs=["/test/source"],
        target_dirs=["/test/target"],
        db_path=":memory:"
    )


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    from core.database import DatabaseManager
    db_path = tmp_path / "test.db"
    db = DatabaseManager(str(db_path))
    db.connect()
    yield db
    db.close()


@pytest.fixture
def sample_file_entry():
    """Create a sample FileEntry for testing."""
    from core.models import FileEntry
    return FileEntry(
        full_hash="test_hash_123",
        quick_hash=None,
        filename="test.txt",
        path_in_archive="docs/test.txt",
        source_archive="/path/archive.zip",
        size=1024,
        is_nested_archive=False
    )
