"""
normalise.py: Canonicalise transfer agent names using rapidfuzz and reference YAML.
"""
import yaml
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from rapidfuzz import process, fuzz

def load_canonical_agents(reference_file: str = "reference/agents.yaml") -> Dict[str, List[str]]:
    """Load the canonical transfer agent names and their variants."""
    with open(reference_file, 'r') as f:
        return yaml.safe_load(f)

def normalise_agent_name(
    raw_name: str,
    canonical_agents: Dict[str, List[str]],
    similarity_threshold: float = 80.0
) -> Tuple[str, float]:
    """
    Normalise a raw transfer agent name to its canonical form.
    
    Returns (canonical_name, similarity_score).
    If no match above threshold, returns ("UNKNOWN", 0.0).
    """
    if not raw_name or len(raw_name.strip()) < 3:
        return ("UNKNOWN", 0.0)
    
    # Clean the raw name
    cleaned_name = raw_name.strip()
    
    # Create a list of all variants to match against
    all_variants = []
    canonical_to_variant = {}
    
    for canonical, variants in canonical_agents.items():
        # Add the canonical name itself
        all_variants.append(canonical)
        canonical_to_variant[canonical] = canonical
        
        # Add all variants
        for variant in variants:
            all_variants.append(variant)
            canonical_to_variant[variant] = canonical
    
    # Use rapidfuzz to find the best match
    if all_variants:
        result = process.extractOne(
            cleaned_name,
            all_variants,
            scorer=fuzz.ratio,
            score_cutoff=similarity_threshold
        )
        
        if result:
            best_match, score, _ = result  # extractOne returns (match, score, index)
            canonical_name = canonical_to_variant[best_match]
            return canonical_name, score
    
    return ("UNKNOWN", 0.0)

def normalise_parsing_results(
    parsing_results: List[Dict],
    reference_file: str = "reference/agents.yaml",
    similarity_threshold: float = 80.0,
    logger: Optional[logging.Logger] = None
) -> List[Dict]:
    """
    Normalise transfer agent names in parsing results.
    
    Adds 'transfer_agent_clean' field to each result.
    """
    if logger:
        logger.info("Starting transfer agent name normalization")
    
    # Load canonical agents
    canonical_agents = load_canonical_agents(reference_file)
    
    if logger:
        logger.info(f"Loaded {len(canonical_agents)} canonical transfer agents")
    
    # Process each result
    normalised_results = []
    unknown_agents = []
    
    for result in parsing_results:
        normalised_result = result.copy()
        
        if result.get("transfer_agent_raw"):
            canonical_name, similarity = normalise_agent_name(
                result["transfer_agent_raw"],
                canonical_agents,
                similarity_threshold
            )
            
            normalised_result["transfer_agent_clean"] = canonical_name
            normalised_result["similarity_score"] = similarity
            
            if canonical_name == "UNKNOWN":
                unknown_agents.append({
                    "cik": result["cik"],
                    "year": result["year"],
                    "raw_name": result["transfer_agent_raw"],
                    "file_path": result.get("file_path", "")
                })
        else:
            normalised_result["transfer_agent_clean"] = None
            normalised_result["similarity_score"] = 0.0
        
        normalised_results.append(normalised_result)
    
    # Save unknown agents for manual review
    if unknown_agents:
        review_dir = Path("review")
        review_dir.mkdir(exist_ok=True)
        
        review_file = review_dir / "unknown_agents.csv"
        with open(review_file, 'w') as f:
            f.write("cik,year,raw_name,file_path\n")
            for agent in unknown_agents:
                f.write(f"{agent['cik']},{agent['year']},\"{agent['raw_name']}\",{agent['file_path']}\n")
        
        if logger:
            logger.info(f"Saved {len(unknown_agents)} unknown agents to {review_file}")
    
    # Summary
    matched_agents = sum(1 for r in normalised_results if r.get("transfer_agent_clean") and r["transfer_agent_clean"] != "UNKNOWN")
    
    if logger:
        logger.info(f"Normalization complete: {matched_agents} agents matched to canonical names")
    
    return normalised_results

def get_agent_statistics(normalised_results: List[Dict]) -> Dict[str, int]:
    """Get statistics on transfer agent usage."""
    agent_counts = {}
    
    for result in normalised_results:
        agent = result.get("transfer_agent_clean")
        if agent and agent != "UNKNOWN":
            agent_counts[agent] = agent_counts.get(agent, 0) + 1
    
    return dict(sorted(agent_counts.items(), key=lambda x: x[1], reverse=True))

def main():
    """Main normalization function."""
    # This would typically be called from the pipeline
    # For testing, we can load some sample data
    print("Normalization module ready")
    print("Use normalise_parsing_results() to process parsing results")

if __name__ == "__main__":
    main() 