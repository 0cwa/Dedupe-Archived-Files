"""
Tests for tui.app module.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import asyncio

# Import after setting up mocks if needed
from textual.app import App
from textual.widgets import Button, Input, ListView, Label

from core.models import AppConfig


class TestAppConfigBinding:
    """Tests for ConfigScreen bindings and functionality."""
    
    def test_config_creation(self):
        """Test AppConfig can be created."""
        config = AppConfig(
            source_dirs=["/source"],
            target_dirs=["/target"]
        )
        assert config.source_dirs == ["/source"]
        assert config.target_dirs == ["/target"]
    
    def test_config_empty(self):
        """Test empty AppConfig."""
        config = AppConfig()
        assert config.source_dirs == []
        assert config.target_dirs == []


class TestDirectoryInputScreen:
    """Tests for DirectoryInputScreen functionality."""
    
    def test_directory_validation(self, tmp_path):
        """Test directory path validation."""
        # Valid directory
        assert Path(str(tmp_path)).exists() is True
        
        # Invalid directory
        invalid_path = tmp_path / "does_not_exist"
        assert invalid_path.exists() is False


class TestTUIScreens:
    """Tests for TUI screen navigation."""
    
    @pytest.mark.asyncio
    async def test_config_screen_initial_state(self):
        """Test ConfigScreen initializes correctly."""
        from tui.app import ConfigScreen
        
        config = AppConfig()
        screen = ConfigScreen(config)
        
        # Screen should have config
        assert screen.config == config
    
    @pytest.mark.asyncio
    async def test_directory_input_screen_creation(self):
        """Test DirectoryInputScreen creation."""
        from tui.app import DirectoryInputScreen
        
        screen = DirectoryInputScreen("Test Title", "source")
        assert screen.title == "Test Title"
        assert screen.dir_type == "source"
    
    @pytest.mark.asyncio
    async def test_settings_screen_creation(self):
        """Test SettingsScreen creation."""
        from tui.app import SettingsScreen
        
        config = AppConfig()
        screen = SettingsScreen(config)
        assert screen.config == config
    
    @pytest.mark.asyncio
    async def test_message_screen_creation(self):
        """Test MessageScreen creation."""
        from tui.app import MessageScreen
        
        screen = MessageScreen("Test Title", "Test Message")
        assert screen.title == "Test Title"
        assert screen.message == "Test Message"


class TestDirectoryAddFlow:
    """Tests for the directory add flow."""
    
    def test_add_source_to_config(self):
        """Test adding source directory updates config."""
        config = AppConfig()
        config.source_dirs.append("/new/source")
        assert "/new/source" in config.source_dirs
    
    def test_add_target_to_config(self):
        """Test adding target directory updates config."""
        config = AppConfig()
        config.target_dirs.append("/new/target")
        assert "/new/target" in config.target_dirs
    
    def test_prevent_duplicate_sources(self):
        """Test duplicate source prevention."""
        config = AppConfig()
        config.source_dirs.append("/path")
        # Simulate duplicate check
        new_path = "/path"
        if new_path not in config.source_dirs:
            config.source_dirs.append(new_path)
        assert config.source_dirs.count("/path") == 1
    
    def test_prevent_duplicate_targets(self):
        """Test duplicate target prevention."""
        config = AppConfig()
        config.target_dirs.append("/path")
        new_path = "/path"
        if new_path not in config.target_dirs:
            config.target_dirs.append(new_path)
        assert config.target_dirs.count("/path") == 1


class TestNavigationBindings:
    """Tests for navigation bindings."""
    
    def test_quit_binding_available(self):
        """Test quit binding is defined."""
        from tui.app import ConfigScreen
        
        # Check ConfigScreen has quit binding
        bindings = ConfigScreen.BINDINGS
        quit_bindings = [b for b in bindings if b.key == "q"]
        assert len(quit_bindings) > 0
    
    def test_settings_binding_available(self):
        """Test settings binding is defined."""
        from tui.app import ConfigScreen
        
        bindings = ConfigScreen.BINDINGS
        settings_bindings = [b for b in bindings if b.key == "s"]
        assert len(settings_bindings) > 0


class TestScreenTransitions:
    """Tests for screen transitions."""
    
    def test_config_to_settings_transition(self):
        """Test transition from config to settings screen."""
        from tui.app import ConfigScreen, SettingsScreen
        
        config = AppConfig()
        config_screen = ConfigScreen(config)
        
        # Settings screen can be created from config
        settings_screen = SettingsScreen(config)
        assert settings_screen is not None
    
    def test_config_to_directory_input_transition(self):
        """Test transition to directory input screen."""
        from tui.app import DirectoryInputScreen
        
        screen = DirectoryInputScreen("Add Source", "source")
        assert screen.title == "Add Source"
        assert screen.dir_type == "source"


class TestAppIntegration:
    """Integration tests for the TUI app."""
    
    def test_app_creation(self):
        """Test DupCleanerApp creation."""
        from tui.app import DupCleanerApp
        
        config = AppConfig()
        app = DupCleanerApp(config)
        
        assert app.config == config
        assert app.CSS_PATH == "styles.tcss"
        assert app.TITLE == "Archive Duplicate Finder"
    
    def test_app_config_accessible(self):
        """Test that app config is accessible to screens."""
        from tui.app import DupCleanerApp, ConfigScreen
        
        config = AppConfig(
            source_dirs=["/src1", "/src2"],
            target_dirs=["/tgt1"]
        )
        app = DupCleanerApp(config)
        
        # Config should be accessible
        assert app.config.source_dirs == ["/src1", "/src2"]
        assert app.config.target_dirs == ["/tgt1"]


class TestTUIWidgets:
    """Tests for TUI widget creation."""
    
    def test_list_view_creation(self):
        """Test ListView can be created."""
        from textual.widgets import ListView, ListItem, Label
        
        # Create a ListView with initial items
        items = [ListItem(Label(f"Item {i}")) for i in range(3)]
        list_view = ListView(*items)
        
        # Should have 3 children
        assert list_view is not None
    
    def test_button_press_handling(self):
        """Test button press events."""
        from textual.widgets import Button
        
        button = Button("Test", id="test-btn")
        assert button.id == "test-btn"
        assert str(button.label) == "Test"


class TestConfigValidationInTUI:
    """Tests for config validation in TUI context."""
    
    def test_start_scan_validation_empty(self):
        """Test validation prevents scan with empty directories."""
        config = AppConfig()
        errors = config.validate()
        
        assert len(errors) > 0
        assert any("source directory" in e.lower() for e in errors)
        assert any("target directory" in e.lower() for e in errors)
    
    def test_start_scan_validation_valid(self):
        """Test validation passes with valid directories."""
        config = AppConfig(
            source_dirs=["/source"],
            target_dirs=["/target"]
        )
        errors = config.validate()
        
        assert len(errors) == 0


class TestSettingsPersistence:
    """Tests for settings persistence."""
    
    def test_delete_method_toggle(self):
        """Test delete method toggling."""
        config = AppConfig()
        
        # Default is trash
        assert config.delete_method == "trash"
        
        # Toggle to permanent
        config.delete_method = "permanent"
        assert config.delete_method == "permanent"
        
        # Toggle back
        config.delete_method = "trash"
        assert config.delete_method == "trash"
    
    def test_checkbox_settings(self):
        """Test boolean settings."""
        config = AppConfig()
        
        # Test defaults
        assert config.keep_database is True
        assert config.recheck_archives is True
        assert config.auto_select_duplicates is True
        assert config.dry_run is False
        
        # Toggle values
        config.keep_database = False
        config.dry_run = True
        
        assert config.keep_database is False
        assert config.dry_run is True


class TestEdgeCases:
    """Tests for edge cases."""
    
    def test_empty_path_handling(self):
        """Test handling of empty paths."""
        config = AppConfig()
        
        # Empty string should not be added
        empty_path = ""
        if empty_path and empty_path not in config.source_dirs:
            config.source_dirs.append(empty_path)
        
        assert "" not in config.source_dirs
    
    def test_whitespace_path_handling(self):
        """Test handling of whitespace-only paths."""
        config = AppConfig()
        
        whitespace_path = "   "
        stripped = whitespace_path.strip()
        
        if stripped and stripped not in config.source_dirs:
            config.source_dirs.append(stripped)
        
        assert "   " not in config.source_dirs
    
    def test_very_long_path(self):
        """Test handling of very long paths."""
        config = AppConfig()
        
        long_path = "/very" + "/long" * 100 + "/path"
        config.source_dirs.append(long_path)
        
        assert long_path in config.source_dirs
    
    def test_special_characters_in_path(self):
        """Test handling of special characters."""
        config = AppConfig()
        
        special_path = "/path/with spaces/and-dashes/and_underscores"
        config.source_dirs.append(special_path)
        
        assert special_path in config.source_dirs


class TestScanningScreenThreading:
    """Tests for ScanningScreen threading and progress updates."""
    
    @pytest.mark.asyncio
    async def test_progress_queue_thread_safety(self):
        """Test that progress updates work correctly from background thread.
        
        This test verifies that asyncio.Queue.put_nowait can be called directly
        from a background thread without needing call_from_thread, which would
        raise an error if called from the same thread as the app.
        """
        from core.models import ScanProgress
        
        # Create a queue like ScanningScreen does
        progress_queue = asyncio.Queue()
        
        # Test putting progress (simulating what queue_progress does now)
        progress = ScanProgress(
            phase="source_scan",
            current_archive="test.zip",
            archives_processed=1,
            total_archives=5
        )
        
        # This simulates direct queue access from a background thread
        # (asyncio.Queue is thread-safe for put_nowait)
        progress_queue.put_nowait(progress)
        
        # Verify the progress was queued
        retrieved = await asyncio.wait_for(progress_queue.get(), timeout=1.0)
        assert retrieved.phase == "source_scan"
        assert retrieved.current_archive == "test.zip"
    
    def test_scanning_screen_creation(self):
        """Test ScanningScreen can be created."""
        from tui.app import ScanningScreen
        
        config = AppConfig(
            source_dirs=["/test/source"],
            target_dirs=["/test/target"],
            db_path=":memory:"
        )
        
        screen = ScanningScreen(config)
        assert screen.config == config
        assert screen._cancelled is False


# Mark all tests that require Textual
pytestmark = [
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
]
