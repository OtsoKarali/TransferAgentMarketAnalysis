# Transfer Agent Market Analysis

This project explores different approaches to analyze transfer agent market share from SEC filings.

## Project Structure

The project is organized into different approaches, each in its own subdirectory:

### 📁 `approaches/`

#### 1. **`sec_api_approach/`** ⭐ **RECOMMENDED**
- **Method**: Uses SEC-API.io service to download 10-K filings
- **Status**: ✅ **Working** - Successfully downloads and processes filings
- **Files**: 
  - `download_apple_10ks.py` - Downloads Apple's recent 10-Ks
  - `extract_transfer_agents.py` - Extracts transfer agent mentions
- **Results**: Found 1,076 mentions across 5 Apple 10-K filings
- **Usage**: 
  ```bash
  cd approaches/sec_api_approach
  python download_apple_10ks.py
  python extract_transfer_agents.py
  ```

#### 2. **`xbrl_instance_approach/`**
- **Method**: Downloads XBRL instance documents and extracts DEI tags
- **Status**: ⚠️ **Partially Working** - Downloads files but no transfer agent tags found
- **Files**: `main.py` - XBRL instance downloader
- **Issue**: Most companies don't include transfer agent info in XBRL DEI tags

#### 3. **`daily_index_approach/`**
- **Method**: Uses SEC daily index files to find recent 10-K filings
- **Status**: ❌ **Rate Limited** - SEC blocks automated access
- **Files**: `sec_10k_analysis.py`, `test_*.py`
- **Issue**: SEC rate limiting prevents bulk downloads

#### 4. **`original_ta2_approach/`**
- **Method**: Uses TA-2 filing data (transfer agent registration forms)
- **Status**: ❌ **Limited Data** - Only covers registered transfer agents
- **Files**: `archive/`, `Data/`
- **Issue**: Doesn't show which companies use which transfer agents

## Key Findings

### Transfer Agent Market Share (from Apple 10-Ks)
Based on the SEC-API approach analysis of Apple's recent 10-K filings:

| Transfer Agent | Mentions | Notes |
|----------------|----------|-------|
| EQ (Equiniti) | 858 | Apple's primary transfer agent |
| AST | 173 | American Stock Transfer |
| Citi | 25 | Citibank |
| BNY Mellon | 13 | Bank of New York Mellon |
| DST | 5 | DST Systems |
| Fidelity | 2 | False positives filtered out |

### Approach Comparison

| Approach | Pros | Cons | Status |
|----------|------|------|--------|
| SEC-API | ✅ Reliable, no rate limits | ❌ Requires API key | ✅ **Working** |
| XBRL | ✅ Structured data | ❌ Limited transfer agent data | ⚠️ Limited |
| Daily Index | ✅ Direct SEC access | ❌ Rate limited | ❌ Blocked |
| TA-2 | ✅ Official data | ❌ No issuer mapping | ❌ Limited |

## Getting Started

### Prerequisites
- Python 3.7+
- SEC-API.io account (free)

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd TransferAgentMarketAnalysis

# Install dependencies for SEC-API approach
cd approaches/sec_api_approach
pip install -r requirements.txt
```

### Quick Start (Recommended)
```bash
cd approaches/sec_api_approach

# 1. Get your SEC-API key from https://sec-api.io
# 2. Update API_KEY in download_apple_10ks.py
# 3. Download Apple's 10-K filings
python download_apple_10ks.py

# 4. Extract transfer agent information
python extract_transfer_agents.py
```

## Results

The SEC-API approach successfully:
- ✅ Downloaded 5 Apple 10-K filings (2020-2024)
- ✅ Extracted 1,076 transfer agent mentions
- ✅ Identified Apple's transfer agent (Equiniti/EQ)
- ✅ Filtered out false positives (e.g., "high-fidelity")

## Future Work

1. **Scale up SEC-API approach** to analyze more companies
2. **Improve extraction accuracy** with machine learning
3. **Build market share dashboard** for visualization
4. **Add international markets** (Canada, UK, etc.)

## Contributing

Each approach is self-contained. To contribute:
1. Choose an approach to improve
2. Work in the appropriate subdirectory
3. Update this README with results

## License

This project is for research purposes. Please respect SEC's terms of service.