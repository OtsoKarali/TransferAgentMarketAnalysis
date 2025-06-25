"""
crawl.py: Download filings for NYSE companies using sec-edgar-downloader.
"""
import yaml
import time
import logging
from pathlib import Path
from typing import List, Dict, Optional
from sec_edgar_downloader import Downloader
from tqdm import tqdm

def setup_logging(log_dir: str = "logs") -> logging.Logger:
    """Set up logging for the crawl process."""
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"crawl_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_companies(seeds_file: str = "seeds/nyse_bootstrap.yaml") -> List[Dict]:
    """Load the list of companies to process."""
    with open(seeds_file, 'r') as f:
        return yaml.safe_load(f)

def download_filings_for_company(
    downloader: Downloader,
    cik: str,
    company_name: str,
    start_year: int = 2000,
    end_year: int = 2024,
    forms: List[str] = ["10-K", "10-Q", "8-K"],
    delay: float = 0.3,
    logger: Optional[logging.Logger] = None
) -> Dict:
    """Download filings for a single company."""
    results = {
        "cik": cik,
        "company_name": company_name,
        "downloaded": 0,
        "errors": 0,
        "errors_detail": []
    }
    
    for year in range(start_year, end_year + 1):
        for form_type in forms:
            try:
                # Download filing
                downloader.get(
                    form_type, 
                    cik, 
                    after=f"{year}-01-01", 
                    before=f"{year}-12-31",
                    download_details=False
                )
                results["downloaded"] += 1
                
                if logger:
                    logger.info(f"Downloaded {form_type} for {cik} ({company_name}) - {year}")
                
                # Rate limiting
                time.sleep(delay)
                
            except Exception as e:
                results["errors"] += 1
                error_msg = f"Error downloading {form_type} for {cik} ({company_name}) - {year}: {str(e)}"
                results["errors_detail"].append(error_msg)
                
                if logger:
                    logger.error(error_msg)
                
                # Still sleep on error to respect rate limits
                time.sleep(delay)
    
    return results

def main(
    download_folder: str = "data/filings",
    company_name: str = "TransferAgents Bot",
    email_address: str = "your@email.com",
    delay: float = 0.3,
    max_companies: Optional[int] = None
):
    """Main crawling function."""
    logger = setup_logging()
    logger.info("Starting filing download process")
    
    # Load companies
    companies = load_companies()
    if max_companies:
        companies = companies[:max_companies]
    
    logger.info(f"Processing {len(companies)} companies")
    
    # Initialize downloader
    downloader = Downloader(
        company_name=company_name,
        email_address=email_address,
        download_folder=download_folder
    )
    
    # Process companies
    results = []
    for company in tqdm(companies, desc="Downloading filings"):
        result = download_filings_for_company(
            downloader=downloader,
            cik=company["cik"],
            company_name=company.get("ticker", company["cik"]),
            logger=logger
        )
        results.append(result)
    
    # Summary
    total_downloaded = sum(r["downloaded"] for r in results)
    total_errors = sum(r["errors"] for r in results)
    
    logger.info(f"Download complete: {total_downloaded} filings downloaded, {total_errors} errors")
    
    return results

if __name__ == "__main__":
    main() 