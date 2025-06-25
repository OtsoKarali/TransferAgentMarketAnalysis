#!/usr/bin/env python3
"""
Test script to check which SEC daily index dates are accessible.
"""
import requests
import time
from datetime import datetime, timedelta

def test_sec_access():
    """Test access to SEC daily index files."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    print("Testing SEC daily index access...")
    print("=" * 50)
    
    # Test dates from 30 days ago to today
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(35):  # Test 35 days
        date = base_date + timedelta(days=i)
        date_str = date.strftime("%Y%m%d")
        
        # SEC daily index URL format
        daily_url = f"https://www.sec.gov/Archives/edgar/daily-index/{date.year}/QTR{((date.month-1)//3)+1}/master.{date_str}.idx"
        
        try:
            time.sleep(0.5)  # Rate limiting
            response = session.get(daily_url, timeout=10)
            
            status = response.status_code
            if status == 200:
                print(f"✅ {date_str}: OK (200)")
            elif status == 404:
                print(f"❌ {date_str}: Not Found (404)")
            elif status == 403:
                print(f"🚫 {date_str}: Forbidden (403)")
            else:
                print(f"⚠️  {date_str}: HTTP {status}")
                
        except Exception as e:
            print(f"❌ {date_str}: Error - {e}")
    
    print("=" * 50)
    print("Test complete!")

if __name__ == "__main__":
    test_sec_access() 