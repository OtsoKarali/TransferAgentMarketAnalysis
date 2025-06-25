"""
pipeline.py: Orchestrate the end-to-end pipeline for transfer agent extraction.
"""
import yaml
import pandas as pd
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from tqdm import tqdm

# Import our modules
from crawl import setup_logging, load_companies, download_filings_for_company
from parse import parse_all_filings
from normalise import normalise_parsing_results, get_agent_statistics

def print_header(title: str):
    """Print a beautiful header."""
    print("\n" + "="*60)
    print(f"🚀 {title}")
    print("="*60)

def print_step(step_num: int, title: str, emoji: str = "📋"):
    """Print a step header."""
    print(f"\n{emoji} Step {step_num}: {title}")
    print("-" * 40)

def print_success(message: str, count: int = None):
    """Print a success message."""
    if count is not None:
        print(f"✅ {message}: {count}")
    else:
        print(f"✅ {message}")

def print_info(message: str, count: int = None):
    """Print an info message."""
    if count is not None:
        print(f"ℹ️  {message}: {count}")
    else:
        print(f"ℹ️  {message}")

def print_warning(message: str, count: int = None):
    """Print a warning message."""
    if count is not None:
        print(f"⚠️  {message}: {count}")
    else:
        print(f"⚠️  {message}")

def create_final_dataset(
    normalised_results: List[Dict],
    companies: List[Dict],
    output_file: str = "data/outputs/transfer_agents_timeseries.csv"
) -> pd.DataFrame:
    """
    Create the final time-series dataset showing transfer agent evolution.
    
    Columns: cik, ticker, company_name, period_end, form_type, 
             transfer_agent_raw, transfer_agent_clean, filing_url, retrieved_at
    """
    # Create lookup dictionaries
    company_lookup = {company["cik"]: company for company in companies}
    
    # Prepare records
    records = []
    for result in normalised_results:
        if not result.get("success"):
            continue
            
        company_info = company_lookup.get(result["cik"], {})
        
        record = {
            "cik": result["cik"],
            "ticker": company_info.get("ticker", result["cik"]),
            "company_name": company_info.get("company_name", result["cik"]),
            "period_end": f"{result['year']}-12-31",  # Default to year-end
            "form_type": result["form_type"],
            "transfer_agent_raw": result.get("transfer_agent_raw"),
            "transfer_agent_clean": result.get("transfer_agent_clean"),
            "filing_url": result.get("file_path", ""),  # For now, use file path
            "retrieved_at": datetime.utcnow().isoformat(" ")
        }
        records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Sort by company and time for better time-series analysis
    df = df.sort_values(['cik', 'period_end', 'form_type'])
    
    # Ensure output directory exists
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    
    return df

def analyze_transfer_agent_evolution(df: pd.DataFrame) -> Dict:
    """
    Analyze transfer agent evolution over time for each company.
    
    Returns insights about transfer agent changes and patterns.
    """
    evolution_insights = {
        "companies_analyzed": 0,
        "transfer_agent_changes": [],
        "current_agents": {},
        "agent_transitions": []
    }
    
    # Group by company and analyze changes
    for cik in df['cik'].unique():
        company_data = df[df['cik'] == cik].sort_values('period_end')
        company_name = company_data['ticker'].iloc[0]
        
        evolution_insights["companies_analyzed"] += 1
        
        # Track transfer agent changes over time
        agent_history = []
        for _, row in company_data.iterrows():
            if pd.notna(row['transfer_agent_clean']) and row['transfer_agent_clean'] != 'UNKNOWN':
                agent_history.append({
                    'period': row['period_end'],
                    'agent': row['transfer_agent_clean'],
                    'form_type': row['form_type']
                })
        
        # Find changes in transfer agents
        if len(agent_history) > 1:
            current_agent = None
            for entry in agent_history:
                if current_agent is None:
                    current_agent = entry['agent']
                elif entry['agent'] != current_agent:
                    # Transfer agent changed
                    evolution_insights["transfer_agent_changes"].append({
                        'cik': cik,
                        'ticker': company_name,
                        'from_agent': current_agent,
                        'to_agent': entry['agent'],
                        'change_date': entry['period'],
                        'form_type': entry['form_type']
                    })
                    current_agent = entry['agent']
        
        # Record current transfer agent
        if agent_history:
            evolution_insights["current_agents"][company_name] = agent_history[-1]['agent']
    
    return evolution_insights

def print_evolution_analysis(evolution_insights: Dict):
    """Print transfer agent evolution analysis."""
    print("\n📈 Transfer Agent Evolution Analysis:")
    print("-" * 40)
    
    print(f"🏢 Companies analyzed: {evolution_insights['companies_analyzed']}")
    
    if evolution_insights["current_agents"]:
        print(f"\n🏢 Current Transfer Agents:")
        for ticker, agent in evolution_insights["current_agents"].items():
            print(f"   {ticker}: {agent}")
    
    if evolution_insights["transfer_agent_changes"]:
        print(f"\n🔄 Transfer Agent Changes Detected:")
        for change in evolution_insights["transfer_agent_changes"]:
            print(f"   {change['ticker']} ({change['change_date']}): {change['from_agent']} → {change['to_agent']}")
    else:
        print(f"\n✅ No transfer agent changes detected in the analyzed period")

def main(
    max_companies: int = 5,  # Start small for testing
    start_year: int = 2020,  # Start with recent years
    end_year: int = 2024,
    forms: List[str] = ["10-K", "10-Q", "8-K"],
    delay: float = 0.5,  # Conservative rate limiting
    company_name: str = "TransferAgents Bot",
    email_address: str = "your@email.com"
):
    """
    Main pipeline function that orchestrates the entire process.
    """
    print_header("NYSE Transfer Agent Extraction Pipeline")
    
    # Setup logging
    logger = setup_logging()
    logger.info("Starting transfer agent extraction pipeline")
    
    # Load companies
    print_step(0, "Loading Configuration", "⚙️")
    companies = load_companies()
    if max_companies:
        companies = companies[:max_companies]
    
    print_success(f"Loaded {len(companies)} companies to process")
    print_info(f"Processing years {start_year} to {end_year}")
    print_info(f"Form types: {', '.join(forms)}")
    
    # Step 1: Download filings
    print_step(1, "Downloading SEC Filings", "📥")
    from sec_edgar_downloader import Downloader
    
    downloader = Downloader(
        company_name=company_name,
        email_address=email_address,
        download_folder="data/filings"
    )
    
    print_info("Initializing SEC EDGAR downloader")
    print_info(f"Rate limiting: {delay}s delay between requests")
    
    download_results = []
    for company in tqdm(companies, desc="📥 Downloading", unit="company"):
        result = download_filings_for_company(
            downloader=downloader,
            cik=company["cik"],
            company_name=company.get("ticker", company["cik"]),
            start_year=start_year,
            end_year=end_year,
            forms=forms,
            delay=delay,
            logger=logger
        )
        download_results.append(result)
    
    total_downloaded = sum(r["downloaded"] for r in download_results)
    total_errors = sum(r["errors"] for r in download_results)
    
    print_success("Download complete", total_downloaded)
    if total_errors > 0:
        print_warning("Download errors", total_errors)
    
    # Step 2: Parse filings
    print_step(2, "Extracting Transfer Agent Information", "🔍")
    print_info("Scanning downloaded filings for transfer agent mentions")
    
    parsing_results = parse_all_filings(logger=logger)
    
    successful_parses = sum(1 for r in parsing_results if r["success"])
    found_agents = sum(1 for r in parsing_results if r["success"] and r["transfer_agent_raw"])
    
    print_success("Parsing complete", successful_parses)
    print_success("Transfer agents found", found_agents)
    
    # Step 3: Normalize agent names
    print_step(3, "Normalizing Transfer Agent Names", "🔄")
    print_info("Matching raw agent names to canonical forms")
    
    normalised_results = normalise_parsing_results(parsing_results, logger=logger)
    
    matched_agents = sum(1 for r in normalised_results if r.get("transfer_agent_clean") and r["transfer_agent_clean"] != "UNKNOWN")
    print_success("Agents matched to canonical names", matched_agents)
    
    # Step 4: Create final dataset
    print_step(4, "Creating Final Dataset", "💾")
    print_info("Assembling results into structured dataset")
    
    df = create_final_dataset(normalised_results, companies)
    
    print_success("Dataset created", len(df))
    print_success(f"Saved to: data/outputs/transfer_agents_timeseries.csv")
    
    # Step 5: Generate statistics and evolution analysis
    print_step(5, "Generating Statistics & Evolution Analysis", "📊")
    agent_stats = get_agent_statistics(normalised_results)
    
    print_info("Transfer agent market share:")
    for agent, count in list(agent_stats.items())[:5]:
        percentage = (count / matched_agents * 100) if matched_agents > 0 else 0
        print(f"   🏢 {agent}: {count} filings ({percentage:.1f}%)")
    
    # Analyze transfer agent evolution
    evolution_insights = analyze_transfer_agent_evolution(df)
    print_evolution_analysis(evolution_insights)
    
    # Final summary
    print_header("Pipeline Complete! 🎉")
    
    print_success("Companies processed", len(companies))
    print_success("Filings downloaded", total_downloaded)
    if total_errors > 0:
        print_warning("Download errors", total_errors)
    print_success("Files parsed", len(parsing_results))
    print_success("Transfer agents found", found_agents)
    print_success("Final dataset records", len(df))
    
    print("\n📈 Key Insights:")
    if agent_stats:
        top_agent, top_count = list(agent_stats.items())[0]
        print(f"   🥇 Top transfer agent: {top_agent} ({top_count} filings)")
    
    if evolution_insights["transfer_agent_changes"]:
        print(f"   🔄 Transfer agent changes detected: {len(evolution_insights['transfer_agent_changes'])}")
    
    print(f"\n💡 Next steps:")
    print(f"   📁 Check data/outputs/transfer_agents_timeseries.csv")
    if any(r.get("transfer_agent_clean") == "UNKNOWN" for r in normalised_results):
        print(f"   🔍 Review unknown agents in review/unknown_agents.csv")
    
    return df

if __name__ == "__main__":
    # Load NYSE bootstrap
    seeds_path = Path("seeds/nyse_bootstrap.yaml")
    agents_path = Path("reference/agents.yaml")
    with open(seeds_path, 'r') as f:
        seeds = yaml.safe_load(f)
    with open(agents_path, 'r') as f:
        agents = yaml.safe_load(f)
    print(f"Loaded {len(seeds)} NYSE seed companies.")
    print(f"Loaded {len(agents)} canonical transfer agents.")
    print("First seed:", seeds[0])
    print("First agent mapping:", list(agents.items())[0])
    
    # Run the pipeline
    main() 