#!/usr/bin/env python3
"""
Test script to verify the improved parsing works on a small subset of files.
"""
import sys
from pathlib import Path
sys.path.append('src')

from parse import find_filing_files, parse_filing_file

def main():
    print("Testing improved parsing on first 5 files...")
    
    # Find all filing files
    filing_files = find_filing_files()
    print(f"Found {len(filing_files)} total files")
    
    # Test on first 5 files only
    test_files = filing_files[:5]
    print(f"Testing on first {len(test_files)} files:")
    
    for i, file_path in enumerate(test_files, 1):
        print(f"\n{i}. Processing: {file_path.name}")
        result = parse_filing_file(file_path)
        
        if result["success"]:
            if result["transfer_agent_raw"]:
                print(f"   ✓ Found transfer agent: {result['transfer_agent_raw']}")
            else:
                print(f"   - No transfer agent found")
        else:
            print(f"   ✗ Error: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main() 