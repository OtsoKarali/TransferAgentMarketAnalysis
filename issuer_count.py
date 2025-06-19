#!/usr/bin/env python3
import os
import pandas as pd

# ─── CONFIGURATION ───
# Parent folder containing subfolders like "2006-Q1", "2006-Q2", ..., "2025-Q1"
DATA_ROOT = "/Users/otsok/Desktop/Coding/RenWave/TransferAgentsAnalysis/TransferAgentMarketAnalysis/Data"
# ─────────────────────

def brand_of(name: str) -> str:
    n = (name or "").lower()
    if "computershare" in n:            return "Computershare"
    if "state street" in n:             return "State Street"
    if any(x in n for x in ("bny mellon","pershing","jpmorgan")):
        return "BNY Mellon"
    if "equiniti" in n:                 return "Equiniti"
    if "fidelity" in n:                 return "Fidelity"
    if "vanguard" in n:                 return "Vanguard"
    if "american funds" in n or "capital group" in n:
        return "American Funds"
    if n.startswith("dst "):            return "DST"
    return name

def main():
    pieces = []
    # 1) Loop through each quarter folder
    for quarter in sorted(os.listdir(DATA_ROOT)):
        qdir = os.path.join(DATA_ROOT, quarter)
        if not os.path.isdir(qdir):
            continue

        ta1_path = os.path.join(qdir, "TA1_REGISTRANT.tsv")
        svc_path = os.path.join(qdir, "TA_SERVICE_COMPANIES.tsv")
        if not (os.path.exists(ta1_path) and os.path.exists(svc_path)):
            continue

        # 2) Load registrations (TA-1 initial & amendments)
        df_ta1 = pd.read_csv(
            ta1_path, sep="\t",
            usecols=["ACCESSION_NUMBER","CIK"],
            dtype=str
        )

        # 3) Load service-company mapping
        df_svc = pd.read_csv(
            svc_path, sep="\t",
            usecols=["ACCESSION_NUMBER","ENTITYNAME"],
            dtype=str
        )

        # 4) Merge to (CIK, ENTITYNAME)
        df = df_ta1.merge(df_svc, on="ACCESSION_NUMBER", how="inner")
        pieces.append(df[["CIK","ENTITYNAME"]])

    # 5) Concatenate all quarters
    all_df = pd.concat(pieces, ignore_index=True)

    # 6) Dedupe each (CIK,ENTITYNAME) pair
    all_df = all_df.drop_duplicates()

    # 7) Count unique issuers per affiliate
    df_aff = (
        all_df
        .groupby("ENTITYNAME", as_index=False)["CIK"]
        .nunique()
        .rename(columns={"CIK":"NumCompanies"})
    )

    # 8) Map affiliates to brands and re‐aggregate
    df_aff["Brand"] = df_aff["ENTITYNAME"].map(brand_of)
    brand_counts = (
        df_aff
        .groupby("Brand", as_index=False)["NumCompanies"]
        .sum()
        .sort_values("NumCompanies", ascending=False)
    )

    # 9) Print the top 10 brands
    top10 = brand_counts.head(10)
    print("\nTop 10 Transfer-Agent Brands by # of Companies Served (2006–Present)\n")
    print(
        top10.to_string(
            index=False,
            formatters={"NumCompanies": "{:,}".format}
        )
    )
    print(f"\nTotal distinct companies ever registered: {all_df['CIK'].nunique():,}\n")

if __name__ == "__main__":
    main()
