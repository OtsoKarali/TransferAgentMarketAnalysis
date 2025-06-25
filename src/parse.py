"""
parse.py: Extract transfer agent information from filings.
"""
import re
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup
import glob
from tqdm import tqdm

def extract_transfer_agent_from_html(html_content: str) -> Optional[str]:
    """
    Extract transfer agent information from HTML content using regex patterns.
    
    Returns the raw transfer agent text if found, None otherwise.
    """
    # Convert to lowercase for case-insensitive matching
    content_lower = html_content.lower()
    
    # Primary patterns for transfer agent mentions - more specific to avoid false positives
    patterns = [
        # Pattern 1: "transfer agent is [company name]" - more specific
        r'(?:transfer\s+agent|registrar)\s*(?:is|:)\s*([^<\n\r]{3,120})',
        
        # Pattern 2: "our transfer agent" followed by company name
        r'our\s+transfer\s+agent[^<\n\r]*?([^<\n\r]{3,120})',
        
        # Pattern 3: "serves as transfer agent"
        r'([^<\n\r]{3,120})\s+serves\s+as\s+(?:our\s+)?transfer\s+agent',
        
        # Pattern 4: "transfer agent and registrar"
        r'(?:transfer\s+agent\s+and\s+registrar|registrar\s+and\s+transfer\s+agent)\s*(?:is|:)\s*([^<\n\r]{3,120})',
        
        # Pattern 5: Direct company name mentions - more specific with word boundaries
        r'\b(computershare|broadridge|american\s+stock\s+transfer|equiniti|continental\s+stock|eq\s+shareowner|wells\s+fargo\s+shareowner)\b',
        
        # Pattern 6: "AST" as standalone or with context
        r'\b(?:american\s+stock\s+transfer|ast\s+(?:trust|&|and))\b',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, content_lower, re.IGNORECASE | re.DOTALL)
        if matches:
            # Clean up the match
            match = matches[0].strip()
            # Remove HTML entities and extra whitespace
            match = re.sub(r'\s+', ' ', match)
            match = re.sub(r'&nbsp;', ' ', match)
            match = re.sub(r'[^\w\s\.,&()-]', '', match)
            
            if len(match) >= 3:  # Minimum meaningful length
                return match
    
    return None

def parse_filing_file(file_path: Path) -> Dict:
    """
    Parse a single filing file and extract transfer agent information.
    
    Returns a dictionary with parsing results.
    """
    try:
        # Read the file
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Extract transfer agent
        transfer_agent_raw = extract_transfer_agent_from_html(content)
        
        # Parse file path to get metadata
        # Expected structure: data/filings/sec-edgar-filings/{cik}/{form_type}/{accession}/full-submission.txt
        parts = file_path.parts
        if len(parts) >= 5:
            cik = parts[-4]  # sec-edgar-filings/{cik}/{form_type}/{accession}/full-submission.txt
            form_type = parts[-3]
            accession = parts[-2]
            
            # Extract year from accession number (format: 0000320193-23-000006)
            year_match = re.search(r'-(\d{2})-', accession)
            year = 2000 + int(year_match.group(1)) if year_match else 2020
        else:
            cik = "UNKNOWN"
            form_type = "UNKNOWN"
            year = 2020
        
        return {
            "cik": cik,
            "year": year,
            "form_type": form_type,
            "transfer_agent_raw": transfer_agent_raw,
            "file_path": str(file_path),
            "success": True
        }
        
    except Exception as e:
        return {
            "cik": str(file_path.parts[-4]) if len(file_path.parts) >= 4 else "UNKNOWN",
            "year": 0,
            "form_type": "UNKNOWN",
            "transfer_agent_raw": None,
            "file_path": str(file_path),
            "success": False,
            "error": str(e)
        }

def find_filing_files(filings_dir: str = "data/filings") -> List[Path]:
    """Find all filing files in the filings directory."""
    # Look for .txt files (sec-edgar-downloader saves as .txt)
    pattern = f"{filings_dir}/**/*.txt"
    return [Path(f) for f in glob.glob(pattern, recursive=True)]

def parse_all_filings(
    filings_dir: str = "data/filings",
    logger: Optional[logging.Logger] = None
) -> List[Dict]:
    """
    Parse all filing files and extract transfer agent information.
    
    Returns a list of parsing results.
    """
    if logger:
        logger.info("Starting filing parsing process")
    
    # Find all filing files
    filing_files = find_filing_files(filings_dir)
    
    if logger:
        logger.info(f"Found {len(filing_files)} filing files to parse")
    
    # Parse each file with progress bar
    results = []
    found_agents_count = 0
    
    for file_path in tqdm(filing_files, desc="🔍 Parsing filings", unit="file"):
        result = parse_filing_file(file_path)
        results.append(result)
        
        if result["success"] and result["transfer_agent_raw"]:
            found_agents_count += 1
            if logger:
                logger.info(f"Found transfer agent in {file_path.name}: {result['transfer_agent_raw'][:50]}...")
    
    # Summary
    successful_parses = sum(1 for r in results if r["success"])
    
    if logger:
        logger.info(f"Parsing complete: {successful_parses}/{len(results)} successful, {found_agents_count} transfer agents found")
    
    return results

def main():
    """Main parsing function."""
    results = parse_all_filings()
    
    # Print summary
    print(f"Parsed {len(results)} files")
    print(f"Found transfer agents in {sum(1 for r in results if r['transfer_agent_raw'])} files")
    
    # Show some examples
    examples = [r for r in results if r["transfer_agent_raw"]][:5]
    for example in examples:
        print(f"  {example['cik']} ({example['year']}): {example['transfer_agent_raw']}")

if __name__ == "__main__":
    main() 