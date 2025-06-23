#!/usr/bin/env python3
"""
SEC 10-K Transfer Agent Analysis Pipeline
Downloads all 10-K filings and extracts transfer agent mentions to analyze market share.
"""
import os
import json
import csv
import time
import requests
import re
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import pandas as pd

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
SUBMISSIONS_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
SUBMISSIONS_DIR = "submissions_json"
RAW_10K_DIR = "raw_10k"
OUTPUT_CSV = "all_10ks.csv"
TRANSFER_AGENTS_CSV = "transfer_agents.csv"
MARKET_SHARE_CSV = "market_share.csv"

# Transfer agent brands to look for
TRANSFER_AGENT_BRANDS = [
    "Computershare",
    "BNY Mellon", 
    "Equiniti",
    "State Street",
    "DST",
    "Broadridge",
    "American Stock Transfer",
    "Continental Stock Transfer",
    "VStock Transfer",
    "Issuer Direct",
    "Fidelity",
    "Vanguard"
]

# SEC rate limiting (10 requests per second)
SEC_DELAY = 0.2  # 200ms between requests (more conservative)
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # Start with 1 second delay for retries
MAX_FILINGS = 100  # Limit for initial test run (set to None for full run)

class SEC10KAnalyzer:
    def __init__(self):
        self.session = requests.Session()
        # Set user agent to avoid blocking
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def download_submissions(self) -> bool:
        """Download and extract the SEC submissions archive."""
        try:
            logger.info("Downloading SEC submissions archive...")
            
            # Download the ZIP file
            response = self.session.get(SUBMISSIONS_URL, timeout=30)
            response.raise_for_status()
            
            with open("submissions.zip", "wb") as f:
                f.write(response.content)
            
            # Extract the ZIP file
            import zipfile
            with zipfile.ZipFile("submissions.zip", 'r') as zip_ref:
                zip_ref.extractall(SUBMISSIONS_DIR)
            
            logger.info(f"Successfully extracted submissions to {SUBMISSIONS_DIR}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download submissions: {e}")
            return False
    
    def identify_10k_filings(self) -> List[Dict]:
        """Parse all JSON files and identify 10-K filings."""
        logger.info("Identifying 10-K filings...")
        
        filings = []
        json_files = list(Path(SUBMISSIONS_DIR).glob("*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                cik = data.get('cik', '')
                if not cik:
                    continue
                
                filings_data = data.get('filings', {}).get('recent', {})
                forms = filings_data.get('form', [])
                accs = filings_data.get('accessionNumber', [])
                dates = filings_data.get('filingDate', [])
                
                for i, form in enumerate(forms):
                    if form in ('10-K', '10-K/A'):
                        if i < len(accs) and i < len(dates):
                            acc = accs[i]
                            date = dates[i]
                            
                            # Build URL
                            cik_int = int(cik)
                            path = acc.replace('-', '')
                            url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{path}/{acc}.htm"
                            
                            filings.append({
                                'cik': cik,
                                'accession': acc,
                                'date': date,
                                'url': url
                            })
                            
            except Exception as e:
                logger.warning(f"Error processing {json_file}: {e}")
                continue
        
        logger.info(f"Found {len(filings)} 10-K filings")
        return filings
    
    def save_filings_csv(self, filings: List[Dict]) -> None:
        """Save filings to CSV file."""
        with open(OUTPUT_CSV, 'w', newline='') as fout:
            writer = csv.DictWriter(fout, fieldnames=['cik', 'accession', 'date', 'url'])
            writer.writeheader()
            writer.writerows(filings)
        
        logger.info(f"Saved {len(filings)} filings to {OUTPUT_CSV}")
    
    def download_10k_document(self, filing: Dict) -> Optional[str]:
        """Download a single 10-K document with retry logic."""
        filename = f"{filing['cik']}_{filing['accession']}.htm"
        filepath = os.path.join(RAW_10K_DIR, filename)
        
        # Skip if already downloaded
        if os.path.exists(filepath):
            return filepath
        
        # Download with rate limiting and retries
        for attempt in range(MAX_RETRIES + 1):
            try:
                time.sleep(SEC_DELAY)
                response = self.session.get(filing['url'], timeout=30)
                
                if response.status_code == 429:
                    # Rate limited - wait longer and retry
                    wait_time = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Rate limited for {filing['url']}, waiting {wait_time}s before retry {attempt + 1}/{MAX_RETRIES}")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                
                # Save the file
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                logger.debug(f"Downloaded {filename}")
                return filepath
                
            except requests.exceptions.RequestException as e:
                if attempt == MAX_RETRIES:
                    logger.warning(f"Failed to download {filing['url']} after {MAX_RETRIES + 1} attempts: {e}")
                    return None
                else:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Attempt {attempt + 1} failed for {filing['url']}, waiting {wait_time}s before retry: {e}")
                    time.sleep(wait_time)
        
        return None
    
    def download_all_10ks(self, filings: List[Dict], max_workers: int = 3) -> List[str]:
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
            
            # Look for transfer agent mentions
            patterns = [
                r'(?:transfer agent|registrar|stock transfer).{0,200}?(' + '|'.join(TRANSFER_AGENT_BRANDS) + ')',
                r'(' + '|'.join(TRANSFER_AGENT_BRANDS) + ').{0,200}?(?:transfer agent|registrar|stock transfer)'
            ]
            
            for pattern in patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    # Get surrounding context
                    start = max(0, match.start() - 100)
                    end = min(len(text), match.end() + 100)
                    context = text[start:end].replace('\n', ' ').strip()
                    
                    mentions.append({
                        'cik': cik,
                        'accession': accession,
                        'brand': match.group(1),
                        'context': context,
                        'file': html_file
                    })
            
            return mentions
            
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
        
        # Step 1: Download submissions (if not already done)
        if not os.path.exists(SUBMISSIONS_DIR):
            if not self.download_submissions():
                logger.error("Failed to download submissions. Exiting.")
                return
        
        # Step 2: Identify 10-K filings
        filings = self.identify_10k_filings()
        if not filings:
            logger.error("No 10-K filings found. Exiting.")
            return
        
        # Limit filings for initial test run
        if MAX_FILINGS and len(filings) > MAX_FILINGS:
            logger.info(f"Limiting to first {MAX_FILINGS} filings for test run (total found: {len(filings)})")
            filings = filings[:MAX_FILINGS]
        
        # Step 3: Save filings to CSV
        self.save_filings_csv(filings)
        
        # Step 4: Download 10-K documents
        downloaded_files = self.download_all_10ks(filings)
        
        # Step 5: Extract transfer agent mentions
        mentions_df = self.analyze_all_documents(downloaded_files)
        
        # Step 6: Calculate market share
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