"""
Main Textual application for Archive Duplicate Finder.
"""
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical, ScrollableContainer
from textual.widgets import Header, Footer, Button, Static, Input, Label, ListView, ListItem, ProgressBar, Checkbox
from textual.binding import Binding
from textual.screen import Screen
from pathlib import Path
import logging
from typing import List, Dict, Optional

from core.models import AppConfig, DuplicateMatch, ArchiveInfo, ScanProgress
from core.database import DatabaseManager
from core.scanner import SourceScanner, TargetScanner
from core.file_ops import FileOperations

logger = logging.getLogger(__name__)


class ConfigScreen(Screen):
    """Initial configuration screen."""
    
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("s", "settings", "Settings"),
        Binding("enter", "start_scan", "Start Scan"),
    ]
    
    def __init__(self, config: AppConfig):
        super().__init__()
        self.config = config
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("📁 Archive Duplicate Finder", classes="header"),
            Label(""),
            Label("Source directories (archives to scan):"),
            ListView(
                *[ListItem(Label(f"  • {src}", classes="path-source")) for src in self.config.source_dirs],
                id="source-list"
            ),
            Button("+ Add Source", id="add-source"),
            Label(""),
            Label("Target directories (where to find duplicates):"),
            ListView(
                *[ListItem(Label(f"  • {tgt}", classes="path-target")) for tgt in self.config.target_dirs],
                id="target-list"
            ),
            Button("+ Add Target", id="add-target"),
            Label(""),
            Horizontal(
                Button("⚙️  Settings", id="settings-btn", variant="default"),
                Button("▶️  Start Scan", id="start-btn", variant="primary"),
                Button("❌ Quit", id="quit-btn", variant="error"),
            ),
            id="config-container"
        )
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add-source":
            self.app.push_screen(DirectoryInputScreen("Add Source Directory", "source"))
        elif event.button.id == "add-target":
            self.app.push_screen(DirectoryInputScreen("Add Target Directory", "target"))
        elif event.button.id == "settings-btn":
            self.action_settings()
        elif event.button.id == "start-btn":
            self.action_start_scan()
        elif event.button.id == "quit-btn":
            self.app.exit()
    
    def action_settings(self) -> None:
        """Open settings screen."""
        self.app.push_screen(SettingsScreen(self.config))
    
    def action_start_scan(self) -> None:
        """Start scanning."""
        if not self.config.source_dirs or not self.config.target_dirs:
            self.app.push_screen(MessageScreen("Error", "Please add at least one source and one target directory."))
            return
        self.app.push_screen(ScanningScreen(self.config))


class DirectoryInputScreen(Screen):
    """Screen for inputting a directory path."""
    
    def __init__(self, title: str, dir_type: str):
        super().__init__()
        self.title = title
        self.dir_type = dir_type
    
    def compose(self) -> ComposeResult:
        yield Container(
            Label(self.title),
            Input(placeholder="Enter directory path...", id="dir-input"),
            Horizontal(
                Button("Add", id="add-btn", variant="primary"),
                Button("Cancel", id="cancel-btn"),
            ),
        )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add-btn":
            input_widget = self.query_one("#dir-input", Input)
            path = input_widget.value.strip()
            if path and Path(path).exists():
                if self.dir_type == "source":
                    if path not in self.app.config.source_dirs:
                        self.app.config.source_dirs.append(path)
                else:
                    if path not in self.app.config.target_dirs:
                        self.app.config.target_dirs.append(path)
                self.app.pop_screen()
            else:
                self.app.push_screen(MessageScreen("Error", "Invalid or non-existent directory path."))
        elif event.button.id == "cancel-btn":
            self.app.pop_screen()


class SettingsScreen(Screen):
    """Settings configuration screen."""
    
    def __init__(self, config: AppConfig):
        super().__init__()
        self.config = config
    
    def compose(self) -> ComposeResult:
        yield Container(
            Static("⚙️  Settings", classes="header"),
            Label(""),
            Horizontal(
                Label("Delete method: "),
                Button(f"[{self.config.delete_method.upper()}]", id="toggle-delete-method"),
            ),
            Checkbox("Keep database", value=self.config.keep_database, id="keep-db"),
            Checkbox("Recheck archives", value=self.config.recheck_archives, id="recheck"),
            Checkbox("Auto-select duplicates", value=self.config.auto_select_duplicates, id="auto-select"),
            Checkbox("Dry run mode", value=self.config.dry_run, id="dry-run"),
            Label(""),
            Horizontal(
                Label("Min file size (bytes): "),
                Input(str(self.config.min_file_size), id="min-size"),
            ),
            Label(""),
            Horizontal(
                Button("💾 Save", id="save-btn", variant="primary"),
                Button("❌ Cancel", id="cancel-btn"),
            ),
        )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save-btn":
            # Save settings
            self.config.keep_database = self.query_one("#keep-db", Checkbox).value
            self.config.recheck_archives = self.query_one("#recheck", Checkbox).value
            self.config.auto_select_duplicates = self.query_one("#auto-select", Checkbox).value
            self.config.dry_run = self.query_one("#dry-run", Checkbox).value
            try:
                self.config.min_file_size = int(self.query_one("#min-size", Input).value)
            except ValueError:
                pass
            self.app.pop_screen()
        elif event.button.id == "cancel-btn":
            self.app.pop_screen()
        elif event.button.id == "toggle-delete-method":
            self.config.delete_method = "permanent" if self.config.delete_method == "trash" else "trash"
            event.button.label = f"[{self.config.delete_method.upper()}]"


class ScanningScreen(Screen):
    """Screen showing scan progress."""
    
    def __init__(self, config: AppConfig):
        super().__init__()
        self.config = config
        self.db = None
        self.duplicates_by_archive = {}
    
    def compose(self) -> ComposeResult:
        yield Container(
            Static("🔍 Scanning...", classes="header"),
            Label(""),
            Label("Phase 1: Scanning source archives...", id="phase-label", classes="status-scanning"),
            ProgressBar(total=100, show_eta=True, id="progress"),
            Label("", id="status-label"),
            Label("", id="current-file-label"),
            Label(""),
            Button("❌ Cancel", id="cancel-btn"),
        )
    
    async def on_mount(self) -> None:
        """Start scanning when screen is mounted."""
        await self.run_scan()
    
    async def run_scan(self) -> None:
        """Run the scanning process."""
        try:
            # Connect to database
            self.db = DatabaseManager(self.config.db_path)
            self.db.connect()
            
            def update_progress(progress: ScanProgress):
                self.query_one("#status-label", Label).update(
                    f"Archives: {progress.archives_processed}/{progress.total_archives} | "
                    f"Files: {progress.files_processed}"
                )
                if progress.current_file:
                    self.query_one("#current-file-label", Label).update(f"Current: {progress.current_file}")
                
                # Update progress bar
                if progress.total_archives > 0:
                    pct = (progress.archives_processed / progress.total_archives) * 100
                    self.query_one("#progress", ProgressBar).update(progress=pct)
            
            # Phase 1: Scan source archives
            self.query_one("#phase-label", Label).update("Phase 1: Scanning source archives...")
            source_scanner = SourceScanner(self.config, self.db, progress_callback=update_progress)
            archive_infos = source_scanner.scan_source_directories()
            
            # Phase 2: Scan target directories
            self.query_one("#phase-label", Label).update("Phase 2: Scanning target directories...")
            self.query_one("#progress", ProgressBar).update(progress=0)
            target_scanner = TargetScanner(self.config, self.db, progress_callback=update_progress)
            self.duplicates_by_archive = target_scanner.scan_target_directories()
            
            # Done
            self.query_one("#phase-label", Label).update("✅ Scan complete!", classes="status-complete")
            self.query_one("#progress", ProgressBar).update(progress=100)
            
            total_dupes = sum(len(v) for v in self.duplicates_by_archive.values())
            self.query_one("#status-label", Label).update(
                f"Found {total_dupes} duplicate files across {len(self.duplicates_by_archive)} archives"
            )
            
            # Update button
            cancel_btn = self.query_one("#cancel-btn", Button)
            cancel_btn.label = "Continue →"
            cancel_btn.id = "continue-btn"
        
        except Exception as e:
            logger.error(f"Scan failed: {e}")
            self.query_one("#phase-label", Label).update(f"❌ Error: {str(e)}", classes="status-error")
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel-btn":
            if self.db:
                self.db.close()
            self.app.pop_screen()
        elif event.button.id == "continue-btn":
            # Go to review screen
            if self.duplicates_by_archive:
                self.app.push_screen(ReviewScreen(self.config, self.db, self.duplicates_by_archive))
            else:
                self.app.push_screen(MessageScreen("No Duplicates", "No duplicate files were found."))
                if self.db:
                    self.db.close()
                self.app.pop_screen()


class ReviewScreen(Screen):
    """Screen for reviewing duplicates."""
    
    BINDINGS = [
        Binding("space", "toggle_selection", "Toggle Selection"),
        Binding("a", "select_all", "Select All"),
        Binding("n", "deselect_all", "Deselect All"),
    ]
    
    def __init__(self, config: AppConfig, db: DatabaseManager, duplicates_by_archive: Dict):
        super().__init__()
        self.config = config
        self.db = db
        self.duplicates_by_archive = duplicates_by_archive
        self.current_selections = {}
        
        # Initialize selections
        for archive, matches in duplicates_by_archive.items():
            for match in matches:
                key = (match.source_file.full_hash or match.source_file.quick_hash, match.target_path)
                self.current_selections[key] = match.selected_for_deletion
    
    def compose(self) -> ComposeResult:
        # Build duplicate list
        items = []
        for archive_path, matches in self.duplicates_by_archive.items():
            archive_name = Path(archive_path).name
            items.append(ListItem(Label(f"📦 {archive_name} ({len(matches)} duplicates)", classes="archive-name")))
            
            for match in matches[:10]:  # Show first 10
                checkbox_mark = "[X]" if match.selected_for_deletion else "[ ]"
                size_str = FileOperations.format_size(match.target_size)
                items.append(ListItem(
                    Label(f"  {checkbox_mark} {match.source_file.filename} → {match.target_path} ({size_str})")
                ))
        
        yield Container(
            Static("📋 Review Duplicates", classes="header"),
            Label("Use Space to toggle selection, Arrow keys to navigate"),
            ScrollableContainer(
                ListView(*items, id="dup-list"),
                id="review-scroll"
            ),
            Horizontal(
                Button("← Back", id="back-btn"),
                Button("Select All", id="select-all-btn"),
                Button("Deselect All", id="deselect-all-btn"),
                Button("Continue →", id="continue-btn", variant="primary"),
            ),
        )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back-btn":
            self.db.close()
            self.app.pop_screen()
        elif event.button.id == "select-all-btn":
            self.action_select_all()
        elif event.button.id == "deselect-all-btn":
            self.action_deselect_all()
        elif event.button.id == "continue-btn":
            # Move to confirmation screen
            selected_files = []
            for archive, matches in self.duplicates_by_archive.items():
                for match in matches:
                    key = (match.source_file.full_hash or match.source_file.quick_hash, match.target_path)
                    if self.current_selections.get(key, True):
                        selected_files.append(match.target_path)
            
            if selected_files:
                self.app.push_screen(ConfirmationScreen(self.config, self.db, selected_files, self.duplicates_by_archive))
            else:
                self.app.push_screen(MessageScreen("No Selection", "No files selected for deletion."))
    
    def action_select_all(self) -> None:
        for key in self.current_selections:
            self.current_selections[key] = True
        self.refresh()
    
    def action_deselect_all(self) -> None:
        for key in self.current_selections:
            self.current_selections[key] = False
        self.refresh()


class ConfirmationScreen(Screen):
    """Final confirmation before deletion."""
    
    def __init__(self, config: AppConfig, db: DatabaseManager, selected_files: List[str], duplicates_by_archive: Dict):
        super().__init__()
        self.config = config
        self.db = db
        self.selected_files = selected_files
        self.duplicates_by_archive = duplicates_by_archive
    
    def compose(self) -> ComposeResult:
        total_size = FileOperations.get_total_size(self.selected_files)
        size_str = FileOperations.format_size(total_size)
        
        method = "move to trash" if self.config.delete_method == "trash" else "PERMANENTLY DELETE"
        dry_run_str = " [DRY RUN - Nothing will be deleted]" if self.config.dry_run else ""
        
        yield Container(
            Static(f"⚠️  Final Confirmation{dry_run_str}", classes="header"),
            Label(""),
            Label(f"Ready to {method}:", classes="highlight-count"),
            Label(f"  • {len(self.selected_files)} files", classes="stat-value"),
            Label(f"  • Total size: {size_str}", classes="highlight-size"),
            Label(""),
            Label("This action cannot be undone!" if self.config.delete_method == "permanent" else "Files will be moved to trash."),
            Label(""),
            Horizontal(
                Button("← Back", id="back-btn"),
                Button(f"✓ {method.title()}", id="proceed-btn", variant="error" if self.config.delete_method == "permanent" else "primary"),
            ),
        )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back-btn":
            self.app.pop_screen()
        elif event.button.id == "proceed-btn":
            # Execute deletion
            use_trash = self.config.delete_method == "trash"
            successful, failures = FileOperations.delete_files(
                self.selected_files,
                use_trash=use_trash,
                dry_run=self.config.dry_run
            )
            
            # Show results
            result_msg = f"{'[DRY RUN] Would delete' if self.config.dry_run else 'Deleted'} {len(successful)} files"
            if failures:
                result_msg += f"\n{len(failures)} files failed"
            
            self.app.push_screen(MessageScreen("Complete", result_msg))
            self.db.close()


class MessageScreen(Screen):
    """Simple message display screen."""
    
    def __init__(self, title: str, message: str):
        super().__init__()
        self.title = title
        self.message = message
    
    def compose(self) -> ComposeResult:
        yield Container(
            Static(self.title, classes="header"),
            Label(""),
            Label(self.message),
            Label(""),
            Button("OK", id="ok-btn", variant="primary"),
        )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.app.pop_screen()


class DupCleanerApp(App):
    """Main application."""
    
    CSS_PATH = "styles.tcss"
    TITLE = "Archive Duplicate Finder"
    
    def __init__(self, config: AppConfig):
        super().__init__()
        self.config = config
    
    def on_mount(self) -> None:
        self.push_screen(ConfigScreen(self.config))
