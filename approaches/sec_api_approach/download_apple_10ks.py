#!/usr/bin/env python3
"""
Download Apple's last 5 10-K filings using SEC-API.io
"""

import os
import json
from sec_api import QueryApi, RenderApi

# Configuration
API_KEY = "26a280450813af7924eb4763f350bf24a7344e1b975937aba9447e4a70e33832"  # Replace with your SEC-API key
COMPANY_TICKER = "AAPL"
MAX_FILINGS = 5
DOWNLOAD_FOLDER = "./filings"

def setup_directories():
    """Create necessary directories."""
    if not os.path.isdir(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)
    if not os.path.isdir("./results"):
        os.makedirs("./results")

def get_apple_10k_urls():
    """Get URLs for Apple's recent 10-K filings."""
    print(f"Searching for {COMPANY_TICKER} 10-K filings...")
    
    queryApi = QueryApi(api_key=API_KEY)
    
    query = {
        "query": {
            "query_string": {
                "query": f'formType:"10-K" AND ticker:{COMPANY_TICKER}',
                "time_zone": "America/New_York"
            }
        },
        "from": "0",
        "size": str(MAX_FILINGS),
        "sort": [{"filedAt": {"order": "desc"}}]
    }
    
    response = queryApi.get_filings(query)
    
    if not response.get("filings"):
        print("No filings found!")
        return []
    
    filings = response["filings"]
    print(f"Found {len(filings)} {COMPANY_TICKER} 10-K filings")
    
    # Extract URLs and metadata
    filing_data = []
    for filing in filings:
        filing_data.append({
            'url': filing['linkToFilingDetails'],
            'filing_date': filing['filedAt'],
            'accession_number': filing['accessionNo'],
            'file_name': f"{filing['accessionNo']}-{COMPANY_TICKER}-10k.htm"
        })
    
    return filing_data

def download_filing(filing_info):
    """Download a single filing."""
    try:
        renderApi = RenderApi(api_key=API_KEY)
        filing_content = renderApi.get_filing(filing_info['url'])
        
        file_path = os.path.join(DOWNLOAD_FOLDER, filing_info['file_name'])
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(filing_content)
        
        print(f"✓ Downloaded: {filing_info['file_name']} ({filing_info['filing_date']})")
        return True
        
    except Exception as e:
        print(f"✗ Failed to download {filing_info['file_name']}: {e}")
        return False

def save_filing_metadata(filing_data):
    """Save filing metadata to JSON file."""
    metadata_file = "./results/filing_metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(filing_data, f, indent=2)
    print(f"Saved filing metadata to {metadata_file}")

def main():
    """Main execution function."""
    print("=" * 50)
    print("APPLE 10-K DOWNLOADER")
    print("=" * 50)
    
    # Check API key
    if API_KEY == "YOUR_API_KEY":
        print("❌ Please update the API_KEY in the script with your SEC-API key")
        print("   Get a free key at: https://sec-api.io")
        return
    
    # Setup directories
    setup_directories()
    
    # Get filing URLs
    filing_data = get_apple_10k_urls()
    
    if not filing_data:
        print("No filings to download.")
        return
    
    # Save metadata
    save_filing_metadata(filing_data)
    
    # Download filings
    print(f"\nDownloading {len(filing_data)} filings...")
    successful_downloads = 0
    
    for filing_info in filing_data:
        if download_filing(filing_info):
            successful_downloads += 1
    
    print(f"\n" + "=" * 50)
    print(f"DOWNLOAD COMPLETE")
    print(f"Successfully downloaded: {successful_downloads}/{len(filing_data)} filings")
    print(f"Files saved to: {DOWNLOAD_FOLDER}")
    print(f"Metadata saved to: ./results/filing_metadata.json")
    print("=" * 50)

if __name__ == "__main__":
    main() 