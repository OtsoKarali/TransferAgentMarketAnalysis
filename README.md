# NYSE Transfer Agent Time-Series Dataset

This project builds and maintains a time-series dataset of NYSE-listed companies and their transfer agents (2000–present) using only free/open EDGAR resources.

## Features
- Annual (and optionally quarterly) snapshots of transfer agent assignments for all NYSE issuers
- Uses 10-K, 10-Q, and 8-K filings
- Fuzzy name matching and canonicalisation
- Incremental update and manual review queue

## Directory Structure
```
data/
  filings/{cik}/{year}/...
  outputs/transfer_agents_annual.parquet
logs/
review/
seeds/nyse_bootstrap.yaml
reference/agents.yaml
src/
```

## Setup
1. Clone the repo and `cd` into it.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Prepare `seeds/nyse_bootstrap.yaml` (see below).
4. Run the pipeline:
   ```bash
   python src/pipeline.py
   ```

## Seeds and Reference Files
- `seeds/nyse_bootstrap.yaml`: List of all NYSE CIKs, tickers, and estimated listed years (see [dseuss/nyse_listing_history](https://github.com/dseuss/nyse_listing_history)).
- `reference/agents.yaml`: Canonical transfer agent names and variants.

## Environment Variables
Copy `.env.example` to `.env` and fill in any required API keys (e.g., for sec-api.io).

## Logging & Quality Control
- Logs are written to `logs/` per run.
- Rows with unknown agents are queued in `review/unknown_agents.csv`.

## License
MIT. See LICENSE file.