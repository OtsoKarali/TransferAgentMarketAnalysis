#!/usr/bin/env python3
"""
SEC 10-K Transfer Agent Analysis Pipeline
Downloads recent 10-K filings and extracts transfer agent mentions to analyze market share.
"""
import os
import csv
import time
import requests
import re
from typing import List, Dict, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sec_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
RAW_10K_DIR = "raw_10k"
OUTPUT_CSV = "all_10ks.csv"
TRANSFER_AGENTS_CSV = "transfer_agents.csv"
MARKET_SHARE_CSV = "market_share.csv"

# Transfer agent brands to look for (expanded list with variations)
TRANSFER_AGENT_BRANDS = [
    # Computershare and variations
    "Computershare",
    "Computershare Trust Company",
    "Computershare Canada",
    # American Stock Transfer
    "American Stock Transfer & Trust Company",
    "American Stock Transfer",
    "AST",
    # Broadridge
    "Broadridge",
    "Broadridge Corporate Issuer Solutions",
    # Continental
    "Continental Stock Transfer & Trust Company",
    "Continental Stock Transfer",
    # Equiniti
    "Equiniti",
    "EQ Shareowner Services",
    "EQ",
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
    "Shareowner Services",
    # Issuer Direct
    "Issuer Direct",
    # VStock
    "VStock Transfer",
    # Pacific
    "Pacific Stock Transfer",
    # Empire
    "Empire Stock Transfer",
    # Securities Transfer Corporation
    "Securities Transfer Corporation",
    # Colonial
    "Colonial Stock Transfer",
    # Philadelphia
    "Philadelphia Stock Transfer",
    # Registrar and Transfer Company
    "Registrar and Transfer Company",
    "R&T",
    # UMB
    "UMB Bank",
    "UMB",
    # Zions
    "Zions Bank",
    "Zions First National Bank",
    # State Street
    "State Street",
    "State Street Bank and Trust",
    # TSX/Canada
    "TSX Trust Company",
    "Odyssey Trust Company",
    # Others
    "Fidelity",
    "Vanguard",
    "Shareholder Services"
]

# SEC rate limiting (very conservative)
SEC_DELAY = 0.5  # 500ms between requests
MAX_RETRIES = 3
RETRY_DELAY = 600.0  # 10 minutes pause on rate limit
MAX_FILINGS = 100  # Limit for initial test run

class SEC10KAnalyzer:
    def __init__(self):
        self.session = requests.Session()
        # Set user agent to avoid blocking
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_recent_10k_filings(self, days_back: int = 30) -> List[Dict]:
        """Get recent 10-K filings from SEC's daily index."""
        logger.info(f"Getting 10-K filings from the last {days_back} days...")
        
        filings = []
        # Start from a recent past date (2 days ago to avoid partial/incomplete days)
        base_date = datetime.today() - timedelta(days=2)
        
        for i in range(days_back * 2):  # Double the range to account for weekends
            date = base_date - timedelta(days=i)
            date_str = date.strftime("%Y%m%d")
            
            # Skip weekends (Saturday=5, Sunday=6)
            if date.weekday() >= 5:
                logger.debug(f"Skipping weekend date {date_str}")
                continue
            
            # SEC daily index URL format
            daily_url = f"https://www.sec.gov/Archives/edgar/daily-index/{date.year}/QTR{((date.month-1)//3)+1}/master.{date_str}.idx"
            
            try:
                time.sleep(SEC_DELAY)
                response = self.session.get(daily_url, timeout=30)
                
                if response.status_code == 404:
                    logger.debug(f"No index file for {date_str}")
                    continue
                elif response.status_code == 403:
                    logger.warning(f"Access forbidden for {date_str} - may be too recent")
                    continue
                elif response.status_code != 200:
                    logger.warning(f"HTTP {response.status_code} for {date_str}")
                    continue
                
                logger.info(f"Processing index file for {date_str}")
                
                # Parse the daily index
                lines = response.text.split('\n')
                for line in lines[11:]:  # Skip header lines
                    if not line.strip():
                        continue
                    
                    parts = line.split('|')
                    if len(parts) >= 4:
                        cik = parts[0].strip()
                        company = parts[1].strip()
                        form = parts[2].strip()
                        accession = parts[3].strip()
                        
                        if form in ('10-K', '10-K/A'):
                            # Build URL
                            cik_int = int(cik)
                            path = accession.replace('-', '')
                            url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{path}/{accession}.htm"
                            
                            filings.append({
                                'cik': cik,
                                'company': company,
                                'accession': accession,
                                'date': date_str,
                                'url': url
                            })
                            
                            # Stop if we have enough filings
                            if len(filings) >= MAX_FILINGS:
                                logger.info(f"Reached limit of {MAX_FILINGS} filings")
                                break
                
                if len(filings) >= MAX_FILINGS:
                    break
                    
            except Exception as e:
                logger.warning(f"Error processing {date_str}: {e}")
                continue
        
        logger.info(f"Found {len(filings)} recent 10-K filings")
        return filings
    
    def save_filings_csv(self, filings: List[Dict]) -> None:
        """Save filings to CSV file."""
        with open(OUTPUT_CSV, 'w', newline='') as fout:
            writer = csv.DictWriter(fout, fieldnames=['cik', 'company', 'accession', 'date', 'url'])
            writer.writeheader()
            writer.writerows(filings)
        
        logger.info(f"Saved {len(filings)} filings to {OUTPUT_CSV}")
    
    def download_10k_document(self, filing: Dict) -> Optional[str]:
        """Download a single 10-K document using SEC index.json to find the correct file."""
        cik = filing['cik']
        accession = filing['accession']
        cik_int = int(cik)
        path = accession.replace('-', '')
        filing_dir = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{path}"
        index_json_url = f"{filing_dir}/index.json"

        try:
            time.sleep(SEC_DELAY)
            response = self.session.get(index_json_url, timeout=30)
            if response.status_code == 404:
                logger.warning(f"404 Not Found for {index_json_url}, skipping.")
                return None
            response.raise_for_status()
            index_data = response.json()
            # Find the primary document (usually the main 10-K HTML)
            primary_doc = index_data.get('directory', {}).get('item', [])[0].get('name')
            # Try to find the first .htm or .html file if not found
            if not primary_doc or not (primary_doc.endswith('.htm') or primary_doc.endswith('.html')):
                for item in index_data.get('directory', {}).get('item', []):
                    name = item.get('name', '')
                    if name.lower().endswith(('.htm', '.html')):
                        primary_doc = name
                        break
            if not primary_doc:
                logger.warning(f"No HTML document found in {index_json_url}, skipping.")
                return None
            # Download the primary document
            doc_url = f"{filing_dir}/{primary_doc}"
            filename = f"{cik}_{accession}.htm"
            filepath = os.path.join(RAW_10K_DIR, filename)
            if os.path.exists(filepath):
                return filepath
            for attempt in range(MAX_RETRIES + 1):
                try:
                    time.sleep(SEC_DELAY)
                    doc_response = self.session.get(doc_url, timeout=30)
                    if doc_response.status_code == 404:
                        logger.warning(f"404 Not Found for {doc_url}, skipping.")
                        return None
                    if doc_response.status_code == 429:
                        logger.error(f"RATE LIMITED! Stopping for 10 minutes. URL: {doc_url}")
                        logger.error("This is a hard stop. Please wait 10 minutes before running again.")
                        time.sleep(RETRY_DELAY)
                        return None
                    doc_response.raise_for_status()
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(doc_response.text)
                    logger.info(f"Downloaded {filename}")
                    return filepath
                except requests.exceptions.RequestException as e:
                    if attempt == MAX_RETRIES:
                        logger.warning(f"Failed to download {doc_url} after {MAX_RETRIES + 1} attempts: {e}")
                        return None
                    else:
                        wait_time = RETRY_DELAY * (2 ** attempt)
                        logger.warning(f"Attempt {attempt + 1} failed for {doc_url}, waiting {wait_time}s before retry: {e}")
                        time.sleep(wait_time)
            return None
        except Exception as e:
            logger.warning(f"Error processing {index_json_url}: {e}")
            return None
    
    def download_all_10ks(self, filings: List[Dict], max_workers: int = 1) -> List[str]:
        """Download all 10-K documents with parallel processing."""
        logger.info(f"Downloading {len(filings)} 10-K documents...")
        
        # Create output directory
        os.makedirs(RAW_10K_DIR, exist_ok=True)
        
        downloaded_files = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all download tasks
            future_to_filing = {
                executor.submit(self.download_10k_document, filing): filing 
                for filing in filings
            }
            
            # Process completed downloads
            for future in as_completed(future_to_filing):
                filing = future_to_filing[future]
                try:
                    filepath = future.result()
                    if filepath:
                        downloaded_files.append(filepath)
                except Exception as e:
                    logger.error(f"Error downloading {filing['url']}: {e}")
        
        logger.info(f"Successfully downloaded {len(downloaded_files)} documents")
        return downloaded_files
    
    def extract_transfer_agents(self, html_file: str) -> List[Dict]:
        """Extract transfer agent mentions from an HTML file."""
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Parse HTML and extract text
            soup = BeautifulSoup(html_content, 'html.parser')
            text = soup.get_text()
            
            # Extract CIK and accession from filename
            filename = os.path.basename(html_file)
            if filename.endswith('.htm'):
                base = filename[:-4]
                if '_' in base:
                    cik, accession = base.split('_', 1)
                else:
                    # Fallback for test files or unexpected names
                    cik, accession = 'TESTCIK', 'TESTACCESSION'
            else:
                cik, accession = 'TESTCIK', 'TESTACCESSION'
            
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
                        'cik': cik,
                        'accession': accession,
                        'brand': brand,
                        'context': context,
                        'file': html_file,
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
            
        except Exception as e:
            logger.warning(f"Error extracting transfer agents from {html_file}: {e}")
            return []
    
    def analyze_all_documents(self, html_files: List[str]) -> pd.DataFrame:
        """Analyze all downloaded documents for transfer agent mentions."""
        logger.info(f"Analyzing {len(html_files)} documents for transfer agent mentions...")
        
        all_mentions = []
        
        for html_file in html_files:
            mentions = self.extract_transfer_agents(html_file)
            all_mentions.extend(mentions)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_mentions)
        
        if not df.empty:
            # Save raw mentions
            df.to_csv(TRANSFER_AGENTS_CSV, index=False)
            logger.info(f"Found {len(df)} transfer agent mentions, saved to {TRANSFER_AGENTS_CSV}")
        else:
            logger.warning("No transfer agent mentions found")
        
        return df
    
    def calculate_market_share(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate market share based on transfer agent mentions."""
        if df.empty:
            logger.warning("No data to calculate market share")
            return pd.DataFrame()
        
        # Get the latest mention per CIK
        df['date'] = pd.to_datetime(df['accession'].str[:8], format='%Y%m%d', errors='coerce')
        latest_per_cik = df.loc[df.groupby('cik')['date'].idxmax()]
        
        # Count by brand
        market_share = latest_per_cik['brand'].value_counts().reset_index()
        market_share.columns = ['Brand', 'Issuer_Count']
        
        # Calculate percentages
        total_issuers = market_share['Issuer_Count'].sum()
        market_share['Market_Share_Pct'] = (market_share['Issuer_Count'] / total_issuers * 100).round(2)
        
        # Save market share
        market_share.to_csv(MARKET_SHARE_CSV, index=False)
        logger.info(f"Market share analysis saved to {MARKET_SHARE_CSV}")
        
        return market_share
    
    def run_full_analysis(self):
        """Run the complete analysis pipeline."""
        logger.info("Starting SEC 10-K Transfer Agent Analysis")
        
        # Step 1: Get recent 10-K filings
        filings = self.get_recent_10k_filings(days_back=30)
        if not filings:
            logger.error("No 10-K filings found. Exiting.")
            return
        
        # Step 2: Save filings to CSV
        self.save_filings_csv(filings)
        
        # Step 3: Download 10-K documents
        downloaded_files = self.download_all_10ks(filings)
        
        # Step 4: Extract transfer agent mentions
        mentions_df = self.analyze_all_documents(downloaded_files)
        
        # Step 5: Calculate market share
        market_share_df = self.calculate_market_share(mentions_df)
        
        # Display results
        if not market_share_df.empty:
            print("\n" + "="*60)
            print("TRANSFER AGENT MARKET SHARE ANALYSIS")
            print("="*60)
            print(market_share_df.to_string(index=False))
            print(f"\nTotal issuers analyzed: {market_share_df['Issuer_Count'].sum():,}")
            print("="*60)
        
        logger.info("Analysis complete!")

def main():
    analyzer = SEC10KAnalyzer()
    analyzer.run_full_analysis()

if __name__ == "__main__":
    main() 