# XBRL Transfer Agent Pipeline

This pipeline extracts transfer agent information for US public companies by parsing the DEI tags from XBRL instance documents in SEC EDGAR filings.

## How it works
- For each company CIK, the pipeline fetches recent filings using the SEC Submissions API.
- For each 10-K (or 10-Q) filing, it downloads the XBRL instance XML.
- It extracts `<dei:EntityTransferAgentName>` and `<dei:EntityTransferAgentCIK>` from the XML.
- Results are saved to `data/transfer_agents.csv` with columns: `CompanyCIK`, `FilingDate`, `TransferAgentName`, `TransferAgentCIK`.

## Usage
- Edit `main.py` to specify CIKs or universe.
- Run: `python main.py`
- Output: `data/transfer_agents.csv`

## Requirements
See `requirements.txt`. 