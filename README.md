# SEC 10-K Transfer Agent Market Analysis

This project analyzes SEC 10-K filings to identify transfer agent mentions and calculate market share across major transfer agent brands.

## Overview

The pipeline follows these steps:
1. **Download** SEC submissions archive (bulk JSON data)
2. **Identify** all 10-K and 10-K/A filings
3. **Download** the actual 10-K documents from SEC EDGAR
4. **Extract** transfer agent mentions using regex patterns
5. **Calculate** market share based on issuer counts

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

## Usage

Run the complete analysis pipeline:

```bash
python sec_10k_analysis.py
```

## Output Files

- `all_10ks.csv` - List of all identified 10-K filings with URLs
- `transfer_agents.csv` - Raw transfer agent mentions with context
- `market_share.csv` - Market share analysis by brand
- `sec_analysis.log` - Detailed execution log

## Configuration

### Transfer Agent Brands

The script looks for these major transfer agent brands:
- Computershare
- BNY Mellon
- Equiniti
- State Street
- DST
- Broadridge
- American Stock Transfer
- Continental Stock Transfer
- VStock Transfer
- Issuer Direct
- Fidelity
- Vanguard

### Rate Limiting

The script respects SEC's rate limiting guidelines (10 requests/second) with configurable delays.

## Archive

Previous scripts using TA-2 filing data are archived in the `archive/` directory:
- `TestScript.py` - Original market share analysis
- `MarketShare.py` - Alternative implementation

## Features

- **Parallel Processing**: Downloads multiple documents simultaneously
- **Resume Capability**: Skips already downloaded files
- **Comprehensive Logging**: Detailed execution logs
- **Error Handling**: Graceful handling of network issues
- **Context Extraction**: Captures surrounding text for mentions

## Example Output

```
============================================================
TRANSFER AGENT MARKET SHARE ANALYSIS
============================================================
           Brand  Issuer_Count  Market_Share_Pct
0   Computershare          125             45.6
1      BNY Mellon           89             32.5
2       Equiniti            34             12.4
3    State Street           16              5.8
4            DST             6              2.2
5     Broadridge             4              1.5

Total issuers analyzed: 274
============================================================
```

## Technical Details

- Uses SEC's bulk submissions API for efficient data access
- Implements proper rate limiting to respect SEC guidelines
- Extracts plain text from HTML documents using BeautifulSoup
- Uses regex patterns to identify transfer agent mentions
- Calculates market share based on latest filing per issuer

## Future Enhancements

- Support for other filing types (20-F, 8-K)
- International market analysis (Canada SEDAR+, UK Companies House)
- Machine learning-based mention detection
- Real-time monitoring and updates