#!/usr/bin/env python3
"""
Test script to download a single known 10-K filing and test transfer agent extraction.
"""
import requests
import time
from bs4 import BeautifulSoup
import re
import os

# Transfer agent brands to look for (expanded list with variations)
TRANSFER_AGENT_BRANDS = [
    "Computershare",
    "BNY Mellon", 
    "The Bank of New York Mellon",
    "Equiniti",
    "State Street",
    "DST",
    "Broadridge",
    "American Stock Transfer",
    "Continental Stock Transfer",
    "VStock Transfer",
    "Issuer Direct",
    "Fidelity",
    "Vanguard",
    "Mellon",
    "Bank of New York"
]

def extract_transfer_agents(text):
    """Extract transfer agent mentions from text."""
    mentions = []
    
    # Look for transfer agent mentions with broader patterns
    patterns = [
        # Pattern 1: transfer agent/registrar followed by brand
        r'(?:transfer agent|registrar|stock transfer|trust company).{0,300}?(' + '|'.join(TRANSFER_AGENT_BRANDS) + ')',
        # Pattern 2: brand followed by transfer agent/registrar
        r'(' + '|'.join(TRANSFER_AGENT_BRANDS) + ').{0,300}?(?:transfer agent|registrar|stock transfer|trust company)',
        # Pattern 3: just the brand name (for broader capture)
        r'\b(' + '|'.join(TRANSFER_AGENT_BRANDS) + r')\b'
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            # Get surrounding context
            start = max(0, match.start() - 150)
            end = min(len(text), match.end() + 150)
            context = text[start:end].replace('\n', ' ').strip()
            
            # Clean up the brand name
            brand = match.group(1).strip()
            
            mentions.append({
                'brand': brand,
                'context': context,
                'pattern_used': pattern[:50] + "..." if len(pattern) > 50 else pattern
            })
    
    # Remove duplicates based on brand and similar context
    unique_mentions = []
    seen = set()
    
    for mention in mentions:
        # Create a key based on brand and first 50 chars of context
        key = (mention['brand'].lower(), mention['context'][:50].lower())
        if key not in seen:
            seen.add(key)
            unique_mentions.append(mention)
    
    return unique_mentions

def test_single_filing():
    """Test with a single known 10-K filing."""
    
    # Apple's 2023 10-K (known to exist)
    test_url = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/aapl-20230930.htm"
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    print(f"Testing with Apple's 10-K: {test_url}")
    print("=" * 60)
    
    try:
        # Download with rate limiting
        time.sleep(0.5)
        response = session.get(test_url, timeout=30)
        response.raise_for_status()
        
        # Save the file
        os.makedirs("raw_10k", exist_ok=True)
        with open("raw_10k/test_apple_10k.htm", "w", encoding="utf-8") as f:
            f.write(response.text)
        
        print("✅ Successfully downloaded Apple's 10-K")
        
        # Extract transfer agents
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        
        mentions = extract_transfer_agents(text)
        
        print(f"\nFound {len(mentions)} transfer agent mentions:")
        for i, mention in enumerate(mentions, 1):
            print(f"\n{i}. Brand: {mention['brand']}")
            print(f"   Context: {mention['context'][:200]}...")
            print(f"   Pattern: {mention['pattern_used']}")
        
        if mentions:
            print(f"\n✅ Transfer agent extraction working! Found {len(mentions)} mentions.")
        else:
            print("\n⚠️  No transfer agent mentions found in Apple's 10-K")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_single_filing() 