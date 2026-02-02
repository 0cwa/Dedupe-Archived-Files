#!/usr/bin/env python3
"""
Test runner for Archive Duplicate Finder.
Run this script to execute all tests.
"""
import subprocess
import sys

def main():
    """Run all tests with pytest."""
    args = ["-v", "--tb=short"]
    if len(sys.argv) > 1:
        args.extend(sys.argv[1:])
    
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/"] + args,
        cwd="/home/engine/project"
    )
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())
