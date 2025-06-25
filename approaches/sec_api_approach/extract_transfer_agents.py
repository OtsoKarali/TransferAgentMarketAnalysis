#!/usr/bin/env python3
"""
Extract transfer agent information from downloaded Apple 10-K filings
"""

import os
import re
import json
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

# Transfer agent brands to look for (comprehensive list)
TRANSFER_AGENT_BRANDS = [
    # Computershare and variations
    "Computershare",
    "Computershare Trust Company",
    "Computershare Canada",
    # American Stock Transfer
    "American Stock Transfer & Trust Company",
    "American Stock Transfer",
    " AST ",
    # Broadridge
    "Broadridge",
    "Broadridge Corporate Issuer Solutions",
    # Continental
    "Continental Stock Transfer & Trust Company",
    "Continental Stock Transfer",
    # Equiniti
    "Equiniti",
    "EQ Shareowner Services",
    " EQ ",
    # BNY Mellon and variations
    "The Bank of New York Mellon",
    "BNY Mellon",
    "Mellon Investor Services",
    "Mellon",
    "Bank of New York",
    # Wells Fargo
    "Wells Fargo Shareowner Services",
    "Wells Fargo Bank",
    "Wells Fargo",
    # JPMorgan Chase
    "JPMorgan Chase Bank",
    "JPMorgan",
    # Citi
    "Citibank",
    " Citi ",
    # Other major transfer agents
    "State Street",
    "DST",
    "VStock Transfer",
    "Issuer Direct",
    "Fidelity",
    "Vanguard",
    "Mellon",
    "Bank of New York"
]

def extract_transfer_agents(text):
    """Extract transfer agent mentions from text with better filtering."""
    mentions = []
    
    # Transfer agent related keywords that should be nearby
    transfer_keywords = [
        'transfer agent', 'registrar', 'shareholder services', 'stock transfer',
        'trust company', 'transfer and paying agent', 'dividend paying agent'
    ]
    
    # Look for transfer agent mentions with stricter patterns
    patterns = [
        # Pattern 1: transfer agent/registrar followed by brand
        r'(?:transfer\s+agent|registrar|shareholder\s+services?)(?:\s+and\s+trust\s+company)?(?:\s*[:\-]?\s*)([^,\.\n]+)',
        # Pattern 2: brand name followed by transfer agent/registrar
        r'([^,\.\n]+?)(?:\s+transfer\s+agent|\s+registrar|\s+shareholder\s+services?|\s+trust\s+company)',
        # Pattern 3: brand name in transfer agent context
        r'([^,\.\n]*?(?:' + '|'.join(re.escape(brand) for brand in TRANSFER_AGENT_BRANDS) + r')[^,\.\n]*)'
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            mention = match.group(1) if len(match.groups()) > 0 else match.group(0)
            mention = mention.strip()
            
            # Get context (surrounding text)
            start = max(0, match.start() - 200)
            end = min(len(text), match.end() + 200)
            context = text[start:end].replace('\n', ' ').strip()
            
            # Check if any transfer agent brand is in the mention
            for brand in TRANSFER_AGENT_BRANDS:
                if brand.lower() in mention.lower():
                    # Additional validation: check if transfer agent keywords are nearby
                    context_lower = context.lower()
                    has_transfer_context = any(keyword in context_lower for keyword in transfer_keywords)
                    
                    # For pattern 3 (standalone mentions), require transfer agent context
                    if pattern == patterns[2] and not has_transfer_context:
                        continue
                    
                    # Filter out common false positives
                    false_positives = [
                        'high-fidelity', 'fidelity bond', 'fidelity insurance',
                        'high fidelity', 'fidelity program', 'fidelity system',
                        'equity', 'equity-based', 'equity compensation',
                        'citizen', 'citizens', 'citizenship',
                        'mellon institute', 'mellon foundation'
                    ]
                    
                    if any(fp in context_lower for fp in false_positives):
                        continue
                    
                    mentions.append({
                        'brand': brand,
                        'mention': mention,
                        'context': context,
                        'pattern': pattern,
                        'has_transfer_context': has_transfer_context
                    })
                    break
    
    return mentions

def process_filing(file_path, filing_metadata):
    """Process a single 10-K filing and extract transfer agent information."""
    print(f"Processing: {os.path.basename(file_path)}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Parse HTML and extract text
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text()
        
        # Extract transfer agent mentions
        mentions = extract_transfer_agents(text)
        
        results = []
        for mention in mentions:
            results.append({
                'file_name': os.path.basename(file_path),
                'filing_date': filing_metadata.get('filing_date', 'Unknown'),
                'accession_number': filing_metadata.get('accession_number', 'Unknown'),
                'brand': mention['brand'],
                'mention': mention['mention'],
                'context': mention['context'],
                'pattern': mention['pattern'],
                'has_transfer_context': mention['has_transfer_context']
            })
        
        print(f"  Found {len(mentions)} transfer agent mentions")
        return results
        
    except Exception as e:
        print(f"  Error processing {file_path}: {e}")
        return []

def main():
    """Main execution function."""
    print("=" * 50)
    print("TRANSFER AGENT EXTRACTION")
    print("=" * 50)
    
    # Load filing metadata
    metadata_file = "./results/filing_metadata.json"
    if not os.path.exists(metadata_file):
        print(f"❌ Metadata file not found: {metadata_file}")
        print("   Please run download_apple_10ks.py first")
        return
    
    with open(metadata_file, 'r') as f:
        filing_metadata = json.load(f)
    
    # Create a lookup for filing metadata
    metadata_lookup = {}
    for filing in filing_metadata:
        metadata_lookup[filing['file_name']] = filing
    
    # Process all downloaded filings
    filings_dir = "./filings"
    if not os.path.exists(filings_dir):
        print(f"❌ Filings directory not found: {filings_dir}")
        print("   Please run download_apple_10ks.py first")
        return
    
    all_results = []
    filing_files = [f for f in os.listdir(filings_dir) if f.endswith('.htm')]
    
    print(f"Found {len(filing_files)} filing files to process")
    print()
    
    # Process each filing and show top mentions
    for file_name in sorted(filing_files):
        file_path = os.path.join(filings_dir, file_name)
        metadata = metadata_lookup.get(file_name, {})
        results = process_filing(file_path, metadata)
        all_results.extend(results)
        
        # Show top mentions for this filing
        if results:
            df_filing = pd.DataFrame(results)
            brand_counts = df_filing['brand'].value_counts()
            print(f"  Top mentions in {os.path.basename(file_name)}:")
            for brand, count in brand_counts.head(3).items():
                print(f"    {brand}: {count}")
        print()
    
    # Save results
    if all_results:
        df = pd.DataFrame(all_results)
        output_file = "./results/transfer_agents.csv"
        df.to_csv(output_file, index=False)
        
        print("=" * 50)
        print("EXTRACTION COMPLETE")
        print(f"Total mentions found: {len(all_results)}")
        print(f"Results saved to: {output_file}")
        
        # Show summary by brand
        if not df.empty:
            brand_counts = df['brand'].value_counts()
            print(f"\nTransfer Agent Mentions by Brand:")
            for brand, count in brand_counts.items():
                print(f"  {brand}: {count}")
            
            # Show validation stats
            transfer_context_count = df['has_transfer_context'].sum()
            print(f"\nValidation Statistics:")
            print(f"  Mentions with transfer agent context: {transfer_context_count}/{len(df)}")
            print(f"  Context validation rate: {transfer_context_count/len(df)*100:.1f}%")
    else:
        print("\nNo transfer agent mentions found in the filings.")
    
    print("=" * 50)

if __name__ == "__main__":
    main() 