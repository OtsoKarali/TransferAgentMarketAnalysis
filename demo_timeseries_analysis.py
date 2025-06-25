#!/usr/bin/env python3
"""
Demo script showing transfer agent time-series analysis.
This demonstrates what the evolution analysis would look like.
"""
import pandas as pd
from datetime import datetime

def create_sample_timeseries_data():
    """Create sample time-series data to demonstrate the analysis."""
    sample_data = [
        # AAPL - consistent with Broadridge
        {"cik": "0000320193", "ticker": "AAPL", "company_name": "Apple Inc.", 
         "period_end": "2020-12-31", "form_type": "10-K", "transfer_agent_clean": "Broadridge Corporate Issuer Solutions, LLC"},
        {"cik": "0000320193", "ticker": "AAPL", "company_name": "Apple Inc.", 
         "period_end": "2021-12-31", "form_type": "10-K", "transfer_agent_clean": "Broadridge Corporate Issuer Solutions, LLC"},
        {"cik": "0000320193", "ticker": "AAPL", "company_name": "Apple Inc.", 
         "period_end": "2022-12-31", "form_type": "10-K", "transfer_agent_clean": "Broadridge Corporate Issuer Solutions, LLC"},
        {"cik": "0000320193", "ticker": "AAPL", "company_name": "Apple Inc.", 
         "period_end": "2023-12-31", "form_type": "10-K", "transfer_agent_clean": "Broadridge Corporate Issuer Solutions, LLC"},
        
        # MSFT - consistent with Computershare
        {"cik": "0000789019", "ticker": "MSFT", "company_name": "Microsoft Corporation", 
         "period_end": "2020-12-31", "form_type": "10-K", "transfer_agent_clean": "Computershare Trust Company, N.A."},
        {"cik": "0000789019", "ticker": "MSFT", "company_name": "Microsoft Corporation", 
         "period_end": "2021-12-31", "form_type": "10-K", "transfer_agent_clean": "Computershare Trust Company, N.A."},
        {"cik": "0000789019", "ticker": "MSFT", "company_name": "Microsoft Corporation", 
         "period_end": "2022-12-31", "form_type": "10-K", "transfer_agent_clean": "Computershare Trust Company, N.A."},
        {"cik": "0000789019", "ticker": "MSFT", "company_name": "Microsoft Corporation", 
         "period_end": "2023-12-31", "form_type": "10-K", "transfer_agent_clean": "Computershare Trust Company, N.A."},
        
        # Example of a company that changed transfer agents
        {"cik": "0001234567", "ticker": "EXAMPLE", "company_name": "Example Corp", 
         "period_end": "2020-12-31", "form_type": "10-K", "transfer_agent_clean": "Computershare Trust Company, N.A."},
        {"cik": "0001234567", "ticker": "EXAMPLE", "company_name": "Example Corp", 
         "period_end": "2021-12-31", "form_type": "10-K", "transfer_agent_clean": "Computershare Trust Company, N.A."},
        {"cik": "0001234567", "ticker": "EXAMPLE", "company_name": "Example Corp", 
         "period_end": "2022-12-31", "form_type": "10-K", "transfer_agent_clean": "Broadridge Corporate Issuer Solutions, LLC"},
        {"cik": "0001234567", "ticker": "EXAMPLE", "company_name": "Example Corp", 
         "period_end": "2023-12-31", "form_type": "10-K", "transfer_agent_clean": "Broadridge Corporate Issuer Solutions, LLC"},
    ]
    
    return pd.DataFrame(sample_data)

def analyze_transfer_agent_evolution(df: pd.DataFrame):
    """Analyze transfer agent evolution over time for each company."""
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

def print_evolution_analysis(evolution_insights):
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

def main():
    """Demo the time-series analysis."""
    print("🚀 Transfer Agent Time-Series Analysis Demo")
    print("=" * 50)
    
    # Create sample data
    print("\n📊 Creating sample time-series data...")
    df = create_sample_timeseries_data()
    
    # Save to CSV
    output_file = "data/outputs/demo_timeseries.csv"
    df.to_csv(output_file, index=False)
    print(f"✅ Sample data saved to: {output_file}")
    
    # Analyze evolution
    print("\n🔍 Analyzing transfer agent evolution...")
    evolution_insights = analyze_transfer_agent_evolution(df)
    print_evolution_analysis(evolution_insights)
    
    # Show the data structure
    print(f"\n📋 Sample Data Structure:")
    print(df.head(8).to_string(index=False))
    
    print(f"\n💡 This demonstrates how the pipeline creates a time-series database")
    print(f"   showing transfer agent evolution over time for each company.")

if __name__ == "__main__":
    main() 