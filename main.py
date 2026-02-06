#!/usr/bin/env python3
"""
Archive Duplicate Finder - Main entry point.

Scans archives, finds duplicate files in target directories, and helps remove them.
"""
import sys
import logging
import click
from pathlib import Path

from core.models import AppConfig
from core.database import DatabaseManager


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('dup_cleaner.log'),
            logging.StreamHandler()
        ]
    )


@click.command()
@click.option('--source', '-s', multiple=True, help='Source archive directories (can specify multiple)')
@click.option('--target', '-t', multiple=True, help='Target directories to search for duplicates (can specify multiple)')
@click.option('--db-path', default='./dup_cache.db', help='Database file path')
@click.option('--no-keep-db', is_flag=True, help='Don\'t keep database between runs')
@click.option('--recheck', is_flag=True, help='Recheck archives for changes even if they haven\'t changed')
@click.option('--recheck-targets', is_flag=True, help='Recheck target files for changes even if they haven\'t changed')
@click.option('--search-archives', is_flag=True, help='Search inside target archives too')
@click.option('--dry-run', is_flag=True, help='Dry run - show what would be deleted')
@click.option('--auto', is_flag=True, help='Automated mode - no prompts')
@click.option('--delete-method', type=click.Choice(['trash', 'permanent']), default='trash', help='Deletion method')
@click.option('--no-auto-select', is_flag=True, help='Don\'t auto-select duplicates')
@click.option('--min-size', default=0, type=int, help='Minimum file size in bytes')
@click.option('--partial-threshold', default=1048576, type=int, help='Partial hash threshold in bytes')
@click.option('--workers', default=4, type=int, help='Number of parallel workers')
@click.option('--verbose', '-v', is_flag=True, help='Verbose logging')
def main(source, target, db_path, no_keep_db, recheck, recheck_targets, search_archives, 
         dry_run, auto, delete_method, no_auto_select, min_size, 
         partial_threshold, workers, verbose):
    """
    Archive Duplicate Finder - Find and remove duplicate files from archives.
    
    Run without arguments to start interactive TUI mode.
    """
    setup_logging(verbose)
    
    # Build configuration
    config = AppConfig(
        source_dirs=list(source),
        target_dirs=list(target),
        db_path=db_path,
        keep_database=not no_keep_db,
        recheck_archives=recheck,
        recheck_targets=recheck_targets,
        search_target_archives=search_archives,
        dry_run=dry_run,
        auto_mode=auto,
        delete_method=delete_method,
        auto_select_duplicates=not no_auto_select,
        min_file_size=min_size,
        partial_hash_threshold=partial_threshold,
        parallel_workers=workers
    )
    
    # If no sources/targets provided and not auto mode, start interactive TUI
    if not source and not target and not auto:
        from tui.app import DupCleanerApp
        app = DupCleanerApp(config)
        app.run()
    elif auto:
        # Run in automated mode
        from automated import run_automated
        run_automated(config)
    else:
        # Validate configuration
        errors = config.validate()
        if errors:
            click.echo("Configuration errors:", err=True)
            for error in errors:
                click.echo(f"  - {error}", err=True)
            sys.exit(1)
        
        # Run with CLI configuration
        from tui.app import DupCleanerApp
        app = DupCleanerApp(config)
        app.run()


if __name__ == '__main__':
    main()
