#!/usr/bin/env python3
"""
Test script for the SEC 10-K analysis pipeline.
Runs a small sample to validate functionality.
"""
import os
import json
import tempfile
import shutil
from sec_10k_analysis import SEC10KAnalyzer, SUBMISSIONS_DIR

def create_test_data():
    """Create a small test dataset."""
    test_data = {
        "cik": "0000320193",
        "filings": {
            "recent": {
                "form": ["10-K", "8-K", "10-K/A"],
                "accessionNumber": [
                    "0000320193-24-000010",
                    "0000320193-24-000011", 
                    "0000320193-24-000012"
                ],
                "filingDate": ["2024-01-01", "2024-01-15", "2024-02-01"]
            }
        }
    }
    
    # Create test submissions directory
    test_dir = "test_submissions"
    os.makedirs(test_dir, exist_ok=True)
    
    # Save test JSON file
    with open(f"{test_dir}/CIK0000320193.json", "w") as f:
        json.dump(test_data, f)
    
    return test_data, test_dir

def test_10k_identification():
    """Test the 10-K identification functionality."""
    print("Testing 10-K identification...")
    
    # Create test data
    test_data, test_dir = create_test_data()
    
    # Create analyzer instance
    analyzer = SEC10KAnalyzer()
    
    try:
        # Temporarily modify the module constant for testing
        import sec_10k_analysis
        original_dir = sec_10k_analysis.SUBMISSIONS_DIR
        sec_10k_analysis.SUBMISSIONS_DIR = test_dir
        
        # Test identification
        filings = analyzer.identify_10k_filings()
        
        print(f"Found {len(filings)} 10-K filings")
        for filing in filings:
            print(f"  - {filing['cik']}: {filing['accession']} ({filing['date']})")
        
        # Verify we found the expected filings
        expected_forms = ["10-K", "10-K/A"]
        found_forms = [f['accession'] for f in filings]
        
        print(f"Expected forms: {expected_forms}")
        print(f"Found forms: {found_forms}")
        
        return len(filings) == 2  # Should find 2 10-K filings
        
    finally:
        # Clean up
        sec_10k_analysis.SUBMISSIONS_DIR = original_dir
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

def test_transfer_agent_extraction():
    """Test transfer agent extraction from sample text."""
    print("\nTesting transfer agent extraction...")
    
    # Sample 10-K text with transfer agent mentions
    sample_text = """
    Our transfer agent is Computershare Trust Company, N.A. 
    The registrar for our common stock is State Street Bank and Trust Company.
    BNY Mellon serves as our stock transfer agent for certain securities.
    """
    
    # Create temporary HTML file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.htm', delete=False) as f:
        f.write(f"<html><body>{sample_text}</body></html>")
        temp_file = f.name
    
    try:
        analyzer = SEC10KAnalyzer()
        mentions = analyzer.extract_transfer_agents(temp_file)
        
        print(f"Found {len(mentions)} transfer agent mentions:")
        for mention in mentions:
            print(f"  - Brand: {mention['brand']}")
            print(f"    Context: {mention['context'][:100]}...")
        
        # Check if we found expected brands
        found_brands = [m['brand'] for m in mentions]
        expected_brands = ['Computershare', 'State Street', 'BNY Mellon']
        
        print(f"Expected brands: {expected_brands}")
        print(f"Found brands: {found_brands}")
        
        return len(mentions) >= 2  # Should find at least 2 mentions
        
    finally:
        # Clean up
        os.unlink(temp_file)

def main():
    """Run all tests."""
    print("Running SEC 10-K Analysis Pipeline Tests")
    print("=" * 50)
    
    # Test 1: 10-K identification
    test1_passed = test_10k_identification()
    
    # Test 2: Transfer agent extraction
    test2_passed = test_transfer_agent_extraction()
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST RESULTS")
    print("=" * 50)
    print(f"10-K Identification: {'PASSED' if test1_passed else 'FAILED'}")
    print(f"Transfer Agent Extraction: {'PASSED' if test2_passed else 'FAILED'}")
    
    if test1_passed and test2_passed:
        print("\nAll tests passed! The pipeline is ready to run.")
        print("Run 'python sec_10k_analysis.py' to start the full analysis.")
    else:
        print("\nSome tests failed. Please check the implementation.")

if __name__ == "__main__":
    main() 