import os
import time
import requests
import pandas as pd
from lxml import etree

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
OUTPUT_CSV = os.path.join(DATA_DIR, 'transfer_agents.csv')
HEADERS = {'User-Agent': 'TransferAgentResearch/1.0 (research@example.com)'}

# Demo: 10 large US company CIKs (zero-padded to 10 digits)
CIKS = [
    '0000320193',  # Apple
    '0000789019',  # Microsoft
    '0001652044',  # Alphabet
    '0001318605',  # Facebook/Meta
    '0001018724',  # Amazon
]

MAX_FILINGS_PER_CIK = 2  # Limit for testing
SEC_DELAY = 3.0  # Increased delay to avoid rate limiting


def get_recent_filings(cik):
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    filings = data.get('filings', {}).get('recent', {})
    accession_numbers = filings.get('accessionNumber', [])
    filing_dates = filings.get('filingDate', [])
    forms = filings.get('form', [])
    # Only 10-Ks
    ten_ks = [
        (acc, date) for acc, date, form in zip(accession_numbers, filing_dates, forms)
        if form == '10-K'
    ]
    return ten_ks[:MAX_FILINGS_PER_CIK]


def get_xbrl_url(cik, accession):
    """Get the correct XBRL instance URL by first checking the filing's index.json."""
    # Remove dashes for path
    acc_nodash = accession.replace('-', '')
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodash}/"
    index_url = f"{base}index.json"
    
    try:
        time.sleep(SEC_DELAY)
        resp = requests.get(index_url, headers=HEADERS)
        print(f"    Checking index.json: {resp.status_code}")
        if resp.status_code != 200:
            return None
        
        index_data = resp.json()
        # Look for the XBRL instance document (the .xml file that's not a linkbase)
        linkbase_suffixes = ['_cal.xml', '_def.xml', '_lab.xml', '_pre.xml']
        for file_info in index_data.get('directory', {}).get('item', []):
            name = file_info.get('name', '')
            if name.endswith('.xml') and not any(name.endswith(suffix) for suffix in linkbase_suffixes):
                print(f"    Found XBRL instance: {name}")
                return f"{base}{name}"
        
        print(f"    No XBRL instance found in index.json, using fallback")
        # Fallback: try the canonical naming pattern
        return f"{base}{accession}.xml"
    except Exception as e:
        print(f"    Error fetching index.json: {e}")
        # Fallback: try the canonical naming pattern
        return f"{base}{accession}.xml"


def extract_transfer_agent_from_xbrl(xml_content):
    try:
        root = etree.fromstring(xml_content)
        nsmap = root.nsmap
        # Find DEI namespace
        dei_ns = None
        for k, v in nsmap.items():
            if v and 'dei' in v:
                dei_ns = v
                break
        if not dei_ns:
            return None, None
        name = root.find(f'.//{{{dei_ns}}}EntityTransferAgentName')
        cik = root.find(f'.//{{{dei_ns}}}EntityTransferAgentCIK')
        return (
            name.text.strip() if name is not None and name.text else None,
            cik.text.strip() if cik is not None and cik.text else None
        )
    except Exception as e:
        return None, None


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    results = []
    for cik in CIKS:
        print(f"Processing CIK {cik}...")
        try:
            filings = get_recent_filings(cik)
        except Exception as e:
            print(f"Failed to get filings for {cik}: {e}")
            continue
        for accession, filing_date in filings:
            xbrl_url = get_xbrl_url(cik, accession)
            if xbrl_url is None:
                print(f"  Skipping {accession} - no XBRL instance found")
                continue
            print(f"  Downloading {xbrl_url}")
            try:
                time.sleep(SEC_DELAY)
                resp = requests.get(xbrl_url, headers=HEADERS)
                if resp.status_code != 200:
                    print(f"    Not found: {xbrl_url}")
                    continue
                name, agent_cik = extract_transfer_agent_from_xbrl(resp.content)
                results.append({
                    'CompanyCIK': cik,
                    'FilingDate': filing_date,
                    'TransferAgentName': name,
                    'TransferAgentCIK': agent_cik,
                    'Accession': accession,
                    'XBRL_URL': xbrl_url
                })
                print(f"    Extracted: {name} (CIK: {agent_cik})")
                time.sleep(2.0)  # Additional delay after successful download
            except Exception as e:
                print(f"    Error: {e}")
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(df)} results to {OUTPUT_CSV}")

if __name__ == "__main__":
    main() 