"""
Main Textual application for Archive Duplicate Finder.
"""
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical, ScrollableContainer
from textual.widgets import Header, Footer, Button, Static, Input, Label, ListView, ListItem, ProgressBar, Checkbox, Select, RadioSet, RadioButton, DataTable
from textual.binding import Binding
from textual.screen import Screen
from textual.worker import Worker
from pathlib import Path
import logging
import asyncio
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
            ListView(id="source-list"),
            Horizontal(
                Button("+ Add Source", id="add-source"),
                Button("- Remove Selected", id="remove-source", variant="error"),
            ),
            Label(""),
            Label("Target directories (where to find duplicates):"),
            ListView(id="target-list"),
            Horizontal(
                Button("+ Add Target", id="add-target"),
                Button("- Remove Selected", id="remove-target", variant="error"),
            ),
            Label(""),
            Label("Current Settings:", classes="setting-label"),
            Static(id="settings-summary", classes="settings-summary-box"),
            Label(""),
            Horizontal(
                Button("⚙️  Settings", id="settings-btn", variant="default"),
                Button("▶️  Start Scan", id="start-btn", variant="primary"),
                Button("❌ Quit", id="quit-btn", variant="error"),
            ),
            id="config-container"
        )
        yield Footer()

    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()
    
    async def on_mount(self) -> None:
        """Update the lists and settings summary when screen is mounted."""
        await self._update_lists()
        self._update_settings_summary()
    
    def _update_settings_summary(self) -> None:
        """Update the settings summary display."""
        summary = self.query_one("#settings-summary", Static)
        
        method = "🗑️ Trash" if self.config.delete_method == "trash" else "⚠️ Permanent"
        keep_db = "Yes" if self.config.keep_database else "No"
        recheck = "Yes" if self.config.recheck_archives else "No"
        recheck_tgt = "Yes" if self.config.recheck_targets else "No"
        dry_run = " [DRY RUN]" if self.config.dry_run else ""
        
        summary.update(
            f"Delete: [b]{method}[/b]{dry_run} | "
            f"Keep DB: {keep_db} | "
            f"Recheck Arc: {recheck} | "
            f"Recheck Tgt: {recheck_tgt} | "
            f"Min Size: {FileOperations.format_size(self.config.min_file_size)}"
        )
    
    async def _update_lists(self) -> None:
        """Update the source and target lists from config."""
        # Update source list
        source_list = self.query_one("#source-list", ListView)
        await source_list.clear()
        for idx, src in enumerate(self.config.source_dirs):
            source_list.append(ListItem(
                Label(f"  • {src}", classes="path-source"),
                id=f"source-{idx}"
            ))
        if not self.config.source_dirs:
            source_list.append(ListItem(Label("  (No source directories added)", classes="text-muted")))
        
        # Update target list
        target_list = self.query_one("#target-list", ListView)
        await target_list.clear()
        for idx, tgt in enumerate(self.config.target_dirs):
            target_list.append(ListItem(
                Label(f"  • {tgt}", classes="path-target"),
                id=f"target-{idx}"
            ))
        if not self.config.target_dirs:
            target_list.append(ListItem(Label("  (No target directories added)", classes="text-muted")))
    
    async def action_remove_source(self) -> None:
        """Remove selected source directory."""
        source_list = self.query_one("#source-list", ListView)
        if source_list.index is not None:
            idx = source_list.index
            if 0 <= idx < len(self.config.source_dirs):
                removed = self.config.source_dirs.pop(idx)
                await self._update_lists()
                self.app.push_screen(MessageScreen("Removed", f"Removed source: {removed}"))
    
    async def action_remove_target(self) -> None:
        """Remove selected target directory."""
        target_list = self.query_one("#target-list", ListView)
        if target_list.index is not None:
            idx = target_list.index
            if 0 <= idx < len(self.config.target_dirs):
                removed = self.config.target_dirs.pop(idx)
                await self._update_lists()
                self.app.push_screen(MessageScreen("Removed", f"Removed target: {removed}"))
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add-source":
            self.app.push_screen(
                DirectoryInputScreen("Add Source Directory", "source"),
                callback=self._on_directory_added
            )
        elif event.button.id == "add-target":
            self.app.push_screen(
                DirectoryInputScreen("Add Target Directory", "target"),
                callback=self._on_directory_added
            )
        elif event.button.id == "remove-source":
            self.action_remove_source()
        elif event.button.id == "remove-target":
            self.action_remove_target()
        elif event.button.id == "settings-btn":
            self.action_settings()
        elif event.button.id == "start-btn":
            self.action_start_scan()
        elif event.button.id == "quit-btn":
            self.app.exit()
    
    async def _on_directory_added(self, result: Optional[str]) -> None:
        """Callback when directory input screen closes."""
        if result:
            await self._update_lists()
    
    def action_settings(self) -> None:
        """Open settings screen."""
        self.app.push_screen(SettingsScreen(self.config), callback=self._on_settings_closed)
    
    def _on_settings_closed(self, result: Optional[bool] = None) -> None:
        """Callback when settings screen closes."""
        self._update_settings_summary()
    
    def action_start_scan(self) -> None:
        """Start scanning."""
        if not self.config.source_dirs or not self.config.target_dirs:
            self.app.push_screen(MessageScreen("Error", "Please add at least one source and one target directory."))
            return
        self.app.push_screen(ScanningScreen(self.config))


class DirectoryInputScreen(Screen):
    """Screen for inputting a directory path."""
    
    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
    ]
    
    def __init__(self, title: str, dir_type: str):
        super().__init__()
        self.title = title
        self.dir_type = dir_type
    
    def compose(self) -> ComposeResult:
        yield Container(
            Static(self.title, classes="header"),
            Label(""),
            Input(placeholder="Enter directory path...", id="dir-input"),
            Label(""),
            Horizontal(
                Button("Add", id="add-btn", variant="primary"),
                Button("Cancel", id="cancel-btn"),
            ),
            id="directory-input-container"
        )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add-btn":
            self._try_add_directory()
        elif event.button.id == "cancel-btn":
            self.action_go_back()
    
    def _try_add_directory(self) -> None:
        """Try to add the directory."""
        input_widget = self.query_one("#dir-input", Input)
        path = input_widget.value.strip()
        
        if not path:
            self.app.push_screen(MessageScreen("Error", "Please enter a directory path."))
            return
        
        path_obj = Path(path)
        if not path_obj.exists():
            self.app.push_screen(MessageScreen("Error", f"Path does not exist: {path}"))
            return
        
        if not path_obj.is_dir():
            self.app.push_screen(MessageScreen("Error", f"Path is not a directory: {path}"))
            return
        
        # Resolve to absolute path
        abs_path = str(path_obj.resolve())
        
        if self.dir_type == "source":
            if abs_path in self.app.config.source_dirs:
                self.app.push_screen(MessageScreen("Info", "Directory already in source list."))
                return
            self.app.config.source_dirs.append(abs_path)
            self.dismiss(abs_path)
        else:
            if abs_path in self.app.config.target_dirs:
                self.app.push_screen(MessageScreen("Info", "Directory already in target list."))
                return
            self.app.config.target_dirs.append(abs_path)
            self.dismiss(abs_path)
    
    def action_go_back(self) -> None:
        """Go back without adding."""
        self.dismiss(None)
    
    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()


class SettingsScreen(Screen):
    """Settings configuration screen."""
    
    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
    ]
    
    def __init__(self, config: AppConfig):
        super().__init__()
        self.config = config
    
    def compose(self) -> ComposeResult:
        yield Container(
            Static("⚙️  Settings", classes="header"),
            Label(""),
            Label("Delete method:", classes="setting-label"),
            RadioSet(
                RadioButton("🗑️  Move to Trash (safer)", id="trash", value=self.config.delete_method == "trash"),
                RadioButton("⚠️  Permanent Delete (cannot be undone)", id="permanent", value=self.config.delete_method == "permanent"),
                id="delete-method-radios"
            ),
            Label(""),
            Checkbox("Keep database", value=self.config.keep_database, id="keep-db"),
            Checkbox("Recheck archives", value=self.config.recheck_archives, id="recheck"),
            Checkbox("Recheck already scanned files", value=self.config.recheck_targets, id="recheck-targets"),
            Label("  (Files are always rechecked if metadata or size changed)", classes="text-muted"),
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
            id="settings-container"
        )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save-btn":
            self._save_settings()
        elif event.button.id == "cancel-btn":
            self.action_go_back()
    
    def _save_settings(self) -> None:
        """Save settings and go back."""
        # Get delete method from RadioSet
        radios = self.query_one("#delete-method-radios", RadioSet)
        if radios.pressed_button:
            self.config.delete_method = str(radios.pressed_button.id)
        
        self.config.keep_database = self.query_one("#keep-db", Checkbox).value
        self.config.recheck_archives = self.query_one("#recheck", Checkbox).value
        self.config.recheck_targets = self.query_one("#recheck-targets", Checkbox).value
        self.config.auto_select_duplicates = self.query_one("#auto-select", Checkbox).value
        self.config.dry_run = self.query_one("#dry-run", Checkbox).value
        try:
            self.config.min_file_size = int(self.query_one("#min-size", Input).value)
        except ValueError:
            pass
        self.action_go_back()
    
    def action_go_back(self) -> None:
        """Go back without saving."""
        self.dismiss()
    
    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()


class ScanningScreen(Screen):
    """Screen showing scan progress."""
    
    BINDINGS = [
        Binding("q", "quit", "Quit"),
    ]
    
    def __init__(self, config: AppConfig):
        super().__init__()
        self.config = config
        self.db = None
        self.duplicates_by_archive = {}
        self._cancelled = False
        self._is_ui_complete = False
        self._progress_queue = asyncio.Queue()
        self._scan_complete = asyncio.Event()
    
    def compose(self) -> ComposeResult:
        yield Container(
            Static("🔍 Scanning...", classes="header"),
            Label(""),
            Label("Phase 1: Scanning source archives...", id="phase-label", classes="status-scanning"),
            ProgressBar(total=100, show_eta=False, id="progress"),
            Label("Initializing...", id="status-label"),
            Label("", id="current-file-label"),
            Label(""),
            Button("❌ Cancel", id="cancel-btn"),
            id="scanning-container"
        )
    
    async def on_mount(self) -> None:
        """Start scanning when screen is mounted."""
        # Start progress update handler
        self.run_worker(self._progress_updater(), group="scan_workers")
        # Start scanning in background worker (pass method reference, not result)
        self.run_worker(self._do_scan, thread=True, group="scan_workers")
    
    async def _progress_updater(self) -> None:
        """Handle progress updates from the scan worker."""
        while not self._scan_complete.is_set() or not self._progress_queue.empty():
            try:
                progress = await asyncio.wait_for(self._progress_queue.get(), timeout=0.1)
                await self._update_ui(progress)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.debug(f"Progress update error: {e}")
    
    async def _update_ui(self, progress: ScanProgress) -> None:
        """Update UI with progress information."""
        if self._cancelled or self._is_ui_complete:
            return
        try:
            phase_label = self.query_one("#phase-label", Label)
            status_label = self.query_one("#status-label", Label)
            file_label = self.query_one("#current-file-label", Label)
            progress_bar = self.query_one("#progress", ProgressBar)
            
            # Update phase label
            if progress.phase == "source_scan":
                phase_label.update("Phase 1: Scanning source archives...")
                if progress.total_archives > 0:
                    status_label.update(
                        f"Archive {progress.archives_processed}/{progress.total_archives}"
                        + (f" | Files: {progress.files_processed}" if progress.files_processed > 0 else "")
                    )
                    pct = (progress.archives_processed / progress.total_archives) * 100
                    progress_bar.update(progress=pct)
                else:
                    status_label.update("Finding archives...")
                    progress_bar.update(progress=0)
                    
            elif progress.phase == "target_scan":
                phase_label.update("Phase 2: Scanning target directories...")
                if progress.total_files > 0:
                    status_label.update(
                        f"File {progress.files_processed}/{progress.total_files}"
                        + (f" | Matches: {progress.archives_processed}" if progress.archives_processed > 0 else "")
                    )
                    pct = (progress.files_processed / progress.total_files) * 100
                    progress_bar.update(progress=pct)
                else:
                    status_label.update("Finding files...")
                    progress_bar.update(progress=0)
                    
            elif progress.phase == "complete":
                self._is_ui_complete = True
                phase_label.update("✅ Scan complete!")
                phase_label.add_class("status-complete")
                progress_bar.update(progress=100)
                total_dupes = progress.files_processed
                status_label.update(
                    f"Found {total_dupes} duplicate files across {progress.archives_processed} archives"
                )
                file_label.update("")
                # Update button
                cancel_btn = self.query_one("#cancel-btn", Button)
                cancel_btn.label = "Continue →"
                cancel_btn.variant = "primary"
                return
            
            # Update current file label
            if progress.current_file:
                file_label.update(f"Current: {Path(progress.current_file).name}")
            elif progress.current_archive:
                file_label.update(f"Archive: {Path(progress.current_archive).name}")
            else:
                file_label.update("")
                
        except Exception as e:
            logger.debug(f"UI update error: {e}")
    
    def _do_scan(self) -> None:
        """Run the scanning process in a background thread."""
        try:
            # Connect to database
            self.db = DatabaseManager(self.config.db_path)
            self.db.connect()
            
            def queue_progress(progress: ScanProgress):
                if self._cancelled:
                    return
                try:
                    # Put directly to queue - asyncio.Queue is thread-safe for put_nowait
                    self._progress_queue.put_nowait(progress)
                except Exception as e:
                    logger.debug(f"Queue progress error: {e}")
            
            # Phase 1: Scan source archives
            source_scanner = SourceScanner(self.config, self.db, progress_callback=queue_progress)
            archive_infos = source_scanner.scan_source_directories()
            
            if self._cancelled:
                return
            
            # Phase 2: Scan target directories
            target_scanner = TargetScanner(self.config, self.db, progress_callback=queue_progress)
            self.duplicates_by_archive = target_scanner.scan_target_directories()
            
            if self._cancelled:
                return
            
            # Signal completion
            total_dupes = sum(len(v) for v in self.duplicates_by_archive.values())
            completion_progress = ScanProgress(
                phase="complete",
                files_processed=total_dupes,
                archives_processed=len(self.duplicates_by_archive)
            )
            queue_progress(completion_progress)
            
        except Exception as e:
            logger.error(f"Scan failed: {e}")
            try:
                error_progress = ScanProgress(
                    phase="error",
                    current_file=str(e)
                )
                queue_progress(error_progress)
            except Exception:
                pass
        finally:
            # Always close database in the worker thread to avoid threading issues
            if self.db:
                try:
                    self.db.close()
                except Exception:
                    pass
                self.db = None
            self._scan_complete.set()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel-btn":
            if self._is_ui_complete:
                # Go to review screen
                if self.duplicates_by_archive:
                    self.app.push_screen(ReviewScreen(self.config, self.config.db_path, self.duplicates_by_archive))
                else:
                    self.app.push_screen(MessageScreen("No Duplicates", "No duplicate files were found."))
                    self.app.pop_screen()
            else:
                self._cancelled = True
                self._scan_complete.set()
                # Don't close db here - it will be closed in the worker thread's finally block
                self.app.pop_screen()
        elif event.button.id == "continue-btn":
            # This ID might still be sent if we haven't removed it everywhere or if it's used elsewhere
            # But with our changes, we primarily use cancel-btn with a different label
            if self.duplicates_by_archive:
                self.app.push_screen(ReviewScreen(self.config, self.config.db_path, self.duplicates_by_archive))
            else:
                self.app.push_screen(MessageScreen("No Duplicates", "No duplicate files were found."))
                self.app.pop_screen()
    
    def action_quit(self) -> None:
        """Quit the application."""
        self._cancelled = True
        self._scan_complete.set()
        # Don't close db here - it will be closed in the worker thread's finally block
        self.app.exit()


class ReviewScreen(Screen):
    """Screen for reviewing duplicates."""

    BINDINGS = [
        Binding("space", "toggle_selection", "Toggle Selection"),
        Binding("a", "select_all", "Select All"),
        Binding("n", "deselect_all", "Deselect All"),
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
    ]

    def __init__(self, config: AppConfig, db_path: str, duplicates_by_archive: Dict):
        super().__init__()
        self.config = config
        self.db_path = db_path
        self.db = None
        self.duplicates_by_archive = duplicates_by_archive
        self.current_selections = {}
        self.row_map = {}  # row_index -> (key, match)
        self._data_loaded = False

        # Initialize selections
        for archive, matches in duplicates_by_archive.items():
            for match in matches:
                key = (match.source_file.full_hash or match.source_file.quick_hash, match.target_path)
                self.current_selections[key] = match.selected_for_deletion

    def _get_db(self) -> DatabaseManager:
        """Get or create database connection."""
        if self.db is None:
            self.db = DatabaseManager(self.db_path)
            self.db.connect()
        return self.db

    def _close_db(self) -> None:
        """Close database connection if open."""
        if self.db:
            try:
                self.db.close()
            except Exception:
                pass
            self.db = None

    def compose(self) -> ComposeResult:
        yield Container(
            Static("📋 Review Duplicates", classes="header"),
            Label("Use Space to toggle selection, Arrow keys to navigate, Esc to go back"),
            DataTable(id="dup-table"),
            Horizontal(
                Button("← Back", id="back-btn"),
                Button("Select All (A)", id="select-all-btn"),
                Button("Deselect All (N)", id="deselect-all-btn"),
                Button("Continue →", id="continue-btn", variant="primary"),
            ),
            id="review-container"
        )

    async def on_mount(self) -> None:
        """Load data into the table when screen is mounted."""
        await self._load_data()

    async def _load_data(self) -> None:
        """Load duplicate data into the DataTable."""
        if self._data_loaded:
            return

        table = self.query_one("#dup-table", DataTable)

        # Configure table
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_column("Select", width=8)
        table.add_column("Source File", width=30)
        table.add_column("Target Path", width=50)
        table.add_column("Size", width=12)

        # Flatten data for table
        row_index = 0
        for archive_path, matches in self.duplicates_by_archive.items():
            archive_name = Path(archive_path).name

            # Add archive header row
            table.add_row(
                "",
                f"📦 {archive_name}",
                f"({len(matches)} duplicates)",
                "",
                key=f"archive-{row_index}"
            )
            # Mark header row (we'll use the row_map to identify it)
            row_index += 1

            # Add duplicate rows
            for match in matches:
                key = (match.source_file.full_hash or match.source_file.quick_hash, match.target_path)
                selected = self.current_selections.get(key, True)
                checkbox = "[X]" if selected else "[ ]"
                size_str = FileOperations.format_size(match.target_size)

                table.add_row(
                    checkbox,
                    match.source_file.filename,
                    match.target_path,
                    size_str,
                    key=f"row-{row_index}"
                )

                self.row_map[row_index] = (key, match)
                row_index += 1

        self._data_loaded = True

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back-btn":
            self.action_go_back()
        elif event.button.id == "select-all-btn":
            self.action_select_all()
        elif event.button.id == "deselect-all-btn":
            self.action_deselect_all()
        elif event.button.id == "continue-btn":
            self._continue_to_confirmation()

    def _continue_to_confirmation(self) -> None:
        """Move to confirmation screen."""
        selected_files = []
        for archive, matches in self.duplicates_by_archive.items():
            for match in matches:
                key = (match.source_file.full_hash or match.source_file.quick_hash, match.target_path)
                if self.current_selections.get(key, True):
                    selected_files.append(match.target_path)

        if selected_files:
            self.app.push_screen(ConfirmationScreen(self.config, self.db_path, selected_files, self.duplicates_by_archive))
        else:
            self.app.push_screen(MessageScreen("No Selection", "No files selected for deletion."))

    def action_toggle_selection(self) -> None:
        """Toggle selection of the current row."""
        table = self.query_one("#dup-table", DataTable)
        cursor_row = table.cursor_row

        # Check if this is a data row (not an archive header)
        row_key = table.get_row_at(cursor_row).key
        if not row_key or not row_key.startswith("row-"):
            return  # Archive header row, skip

        # Extract row index from key
        try:
            row_idx = int(row_key.split("-")[1])
        except (ValueError, IndexError):
            return

        if row_idx in self.row_map:
            key, match = self.row_map[row_idx]
            new_state = not self.current_selections.get(key, True)
            self.current_selections[key] = new_state

            # Update UI
            checkbox = "[X]" if new_state else "[ ]"
            table.update_cell(cursor_row, "Select", checkbox)

    async def action_select_all(self) -> None:
        """Select all duplicates."""
        table = self.query_one("#dup-table", DataTable)

        for row_idx, (key, match) in self.row_map.items():
            self.current_selections[key] = True
            # Update the table row directly
            row_key = f"row-{row_idx}"
            try:
                table.update_cell(row_key, "Select", "[X]")
            except Exception:
                pass

    async def action_deselect_all(self) -> None:
        """Deselect all duplicates."""
        table = self.query_one("#dup-table", DataTable)

        for row_idx, (key, match) in self.row_map.items():
            self.current_selections[key] = False
            # Update the table row directly
            row_key = f"row-{row_idx}"
            try:
                table.update_cell(row_key, "Select", "[ ]")
            except Exception:
                pass

    def action_go_back(self) -> None:
        """Go back to previous screen."""
        self._close_db()
        self.app.pop_screen()

    def action_quit(self) -> None:
        """Quit the application."""
        self._close_db()
        self.app.exit()



class ConfirmationScreen(Screen):
    """Final confirmation before deletion."""
    
    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
    ]
    
    def __init__(self, config: AppConfig, db_path: str, selected_files: List[str], duplicates_by_archive: Dict):
        super().__init__()
        self.config = config
        self.db_path = db_path
        self.db = None
        self.selected_files = selected_files
        self.duplicates_by_archive = duplicates_by_archive
    
    def _get_db(self) -> DatabaseManager:
        """Get or create database connection."""
        if self.db is None:
            self.db = DatabaseManager(self.db_path)
            self.db.connect()
        return self.db
    
    def _close_db(self) -> None:
        """Close database connection if open."""
        if self.db:
            try:
                self.db.close()
            except Exception:
                pass
            self.db = None
    
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
            id="confirmation-container"
        )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back-btn":
            self.action_go_back()
        elif event.button.id == "proceed-btn":
            self._execute_deletion()
    
    def _execute_deletion(self) -> None:
        """Execute file deletion."""
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
        
        self._close_db()
        self.app.push_screen(MessageScreen("Complete", result_msg))
    
    def action_go_back(self) -> None:
        """Go back without deleting."""
        self.app.pop_screen()
    
    def action_quit(self) -> None:
        """Quit the application."""
        self._close_db()
        self.app.exit()


class MessageScreen(Screen):
    """Simple message display screen."""
    
    BINDINGS = [
        Binding("escape", "dismiss_screen", "OK"),
        Binding("enter", "dismiss_screen", "OK"),
        Binding("q", "quit", "Quit"),
    ]
    
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
            id="message-container"
        )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.action_dismiss_screen()
    
    def action_dismiss_screen(self) -> None:
        """Dismiss this screen."""
        self.app.pop_screen()
    
    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()


class DupCleanerApp(App):
    """Main application."""
    
    CSS_PATH = "styles.tcss"
    TITLE = "Archive Duplicate Finder"
    
    def __init__(self, config: AppConfig):
        super().__init__()
        self.config = config
    
    def on_mount(self) -> None:
        self.push_screen(ConfigScreen(self.config))
