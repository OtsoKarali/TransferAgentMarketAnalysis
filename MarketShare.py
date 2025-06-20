#!/usr/bin/env python3
import os
import pandas as pd

# ─── CONFIGURATION ───
# Parent folder containing all your “2006-Q1”, … “2025-Q1” subdirs:
DATA_ROOT = "/Users/otsok/Desktop/Coding/RenWave/TransferAgentsAnalysis/TransferAgentMarketAnalysis/Data"
# ─────────────────────

def brand_of(name: str) -> str:
    n = (name or "").lower()
    if "computershare" in n:                   return "Computershare"
    if "state street" in n:                    return "State Street"
    if any(x in n for x in ("bny mellon","pershing","jpmorgan")):
        return "BNY Mellon"
    if "equiniti" in n:                        return "Equiniti"
    if "fidelity" in n:                        return "Fidelity"
    if "vanguard" in n:                        return "Vanguard"
    if "american funds" in n or "capital group" in n:
        return "American Funds"
    if n.startswith("dst "):                   return "DST"
    return name

def main():
    regs = []  # will collect (CIK, ENTITYNAME, FILING_DATE)

    # 1) Walk through all quarter dirs
    for quarter in sorted(os.listdir(DATA_ROOT)):
        qdir = os.path.join(DATA_ROOT, quarter)
        if not os.path.isdir(qdir):
            continue

        # Paths for this quarter
        subm = os.path.join(qdir, "TA_SUBMISSION.tsv")
        svcp = os.path.join(qdir, "TA_SERVICE_COMPANIES.tsv")
        if not (os.path.exists(subm) and os.path.exists(svcp)):
            continue

        # 2) Load this quarter's TA_SUBMISSION, filter TA-1 & TA-1/A
        df_sub = pd.read_csv(
            subm, sep="\t",
            usecols=["ACCESSION_NUMBER","CIK","FILING_DATE","SUBMISSIONTYPE"],
            dtype=str,
            parse_dates=["FILING_DATE"]
        )
        df_ta1 = df_sub[df_sub["SUBMISSIONTYPE"].isin(("TA-1","TA-1/A"))]

        # 3) Load this quarter's service-company mapping
        df_svc = pd.read_csv(
            svcp, sep="\t",
            usecols=["ACCESSION_NUMBER","ENTITYNAME"],
            dtype=str
        )

        # 4) Merge → (CIK, ENTITYNAME, FILING_DATE)
        merged = df_ta1.merge(df_svc, on="ACCESSION_NUMBER", how="inner")
        regs.append(merged[["CIK","ENTITYNAME","FILING_DATE"]])

    # 5) Concatenate all quarters
    all_regs = pd.concat(regs, ignore_index=True)

    # 6) For each CIK pick the latest registration date (the “current” agent)
    idx = all_regs.groupby("CIK")["FILING_DATE"].idxmax()
    current = all_regs.loc[idx]

    # 7) Count distinct companies per affiliate
    df_aff = (
        current.groupby("ENTITYNAME", as_index=False)["CIK"]
               .nunique()
               .rename(columns={"CIK":"NumCompanies"})
    )

    # 8) Brand-collapse and re-aggregate
    df_aff["Brand"] = df_aff["ENTITYNAME"].map(brand_of)
    brand_counts = (
        df_aff.groupby("Brand", as_index=False)["NumCompanies"]
               .sum()
               .sort_values("NumCompanies", ascending=False)
    )

    # 9) Print the Top 10 brands
    print("\nCurrent Market Snapshot (by # of Issuers per Brand)\n")
    print(
        brand_counts.head(10)
                    .to_string(index=False, formatters={"NumCompanies":"{:,}".format})
    )
    print(f"\nTotal distinct issuers: {current['CIK'].nunique():,}\n")

if __name__ == "__main__":
    main()
