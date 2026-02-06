#!/usr/bin/env python3
"""
Test runner for Archive Duplicate Finder.
Run this script to execute all tests.

Usage:
    python run_tests.py              # Run standard tests
    python run_tests.py --extensive  # Run extensive integration tests
    python run_tests.py -v           # Run with verbose output
"""
import subprocess
import sys

def main():
    """Run all tests with pytest."""
    args = ["-v", "--tb=short"]
    test_path = "tests/"
    
    # Check for extensive flag
    if "--extensive" in sys.argv:
        sys.argv.remove("--extensive")
        args.extend(["-m", "extensive"])
        print("=" * 60)
        print("Running EXTENSIVE integration tests")
        print("This will test the entire application with real files")
        print("=" * 60)
    else:
        # Skip extensive tests by default
        args.extend(["-m", "not extensive"])
    
    # Add any remaining arguments
    if len(sys.argv) > 1:
        args.extend(sys.argv[1:])
    
    print(f"Running: pytest {test_path} {' '.join(args)}")
    print("-" * 60)
    
    result = subprocess.run(
        [sys.executable, "-m", "pytest", test_path] + args,
        cwd="/home/engine/project"
    )
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())
