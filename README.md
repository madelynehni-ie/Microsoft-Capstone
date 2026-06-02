# MBDS Capstone Project 2026 – IE University

A data-driven capstone project developed as part of the Master in Business Analytics & Data Science (MBDS) at IE University. This project combines advanced analytics, machine learning, and business strategy to solve real-world challenges through actionable insights, scalable models, and impactful storytelling.

## Overview
This repository contains the full development lifecycle of our MBDS Capstone project, including:
- Problem definition & business context
- Data collection and preprocessing (ETL)
- Exploratory data analysis (EDA)
- Machine learning & predictive modeling
- Model evaluation and optimization
- Business recommendations & strategic insights
- Final visualizations and presentation materials

## Tech Stack
Python 3 (Standard Library only for ETL) · SQL · Pandas · Scikit-learn · XGBoost · Power BI/Tableau · Streamlit · Cloud & API integrations · Azure · DataBricks · Spark

## Objective
To bridge business and technology by leveraging analytics and AI to create measurable impact and support data-driven decision-making.

---

## Getting Started: Exploratory ETL Pipeline

We have developed a pure Python 3, zero-dependency exploratory ETL pipeline in the `Exploratory ETL` directory to extract, transform, and load Tenders Electronic Daily (TED) public procurement data.

### Project Directory Structure
After running the script, your project directory will look like this:
```text
Microsoft-Capstone/
├── README.md
├── .gitignore
├── Exploratory ETL/
│   └── ted_exploratory_etl.py
└── data/
    ├── raw/
    │   └── packages/          # Downloaded compressed archives (.tar.gz)
    ├── bronze/
    │   └── packages/          # Raw XML notice files extracted
    ├── silver/
    │   ├── notices.csv        # Transformed clean notice records
    │   ├── organizations.csv  # Organization entities & roles (Buyers/Winners)
    │   ├── lots.csv           # Lot division datasets
    │   └── cpv_codes.csv      # Common Procurement Vocabulary mapping
    ├── gold/
    │   ├── daily_aggregates.csv  # Aggregated daily procurement volumes & values
    │   └── cpv_aggregates.csv    # Distribution of notices across CPV divisions
    └── reports/
        └── etl_profile.json   # Quality profile of the ETL run (counts, missingness)
```

### Running the Pipeline
You can run the script using standard Python 3. It automatically fetches package data from the remote or **gracefully falls back to creating realistic mock TED procurement datasets locally** if the remote is unreachable or you are offline.

To run the pipeline for a single package (e.g., May 2026):
```bash
python3 "Exploratory ETL/ted_exploratory_etl.py" --packages 2026-05
```

To run a range of packages (e.g., January 2026 to March 2026):
```bash
python3 "Exploratory ETL/ted_exploratory_etl.py" --packages 2026-01..2026-03
```

To limit the number of files processed per package for quick testing (e.g., maximum 5 files):
```bash
python3 "Exploratory ETL/ted_exploratory_etl.py" --packages 2026-05 --max-files 5
```

### Command Line Arguments
* `--packages`, `-p`: Space-separated list of package IDs (e.g., `2026-05`) or ranges (`2026-01..2026-03`).
* `--local-archive`, `-l`: Space-separated list of local archive mappings (e.g., `/path/to/file.tar.gz` or `2026-05:/path/to/file.tar.gz`).
* `--data-dir`, `-d`: Target folder for outputs (defaults to `./data`).
* `--base-url`, `-u`: Remote URL to pull compressed archives from (defaults to `https://ted.europa.eu/`).
* `--max-files`, `-n`: Cap the number of XML notices parsed per package (great for testing).
* `--no-bronze-xml`: Skip writing raw XMLs to the `bronze` folder.
* `--overwrite`: Force re-download and reprocessing of existing local packages.
* `--country`, `-c`: Filter notices by country codes (e.g., `-c FR DE`).
* `--cpv-prefix`: Filter notices by CPV prefix (e.g., `--cpv-prefix 45`).
* `--notice-type`: Filter notices by form tag type (e.g., `--notice-type F02_2014`).
* `--log-level`: Set log reporting level (`DEBUG`, `INFO`, `WARNING`, `ERROR`).

---

## S2 Silver Layer: Cleaning, Entity Resolution & Semantic Tags

S2 owns the Silver cleaning, buyer entity resolution, semantic tagging, and data-quality reporting layer for the Procurement Intelligence System. This layer builds on the exploratory TED ETL outputs and prepares business-ready buyer-level data for later Gold feature engineering and ML modelling.

Run the exploratory ETL first:

```bash
python3 "Exploratory ETL/ted_exploratory_etl.py" --packages 2026-05 --max-files 10
```

Then run the S2 Silver Layer workflow:

```bash
python3 src/s2_silver_layer.py \
  --input-dir data/silver \
  --output-dir data/silver \
  --reports-dir data/reports \
  --fuzzy-threshold 90
```

For a lightweight end-to-end run:

```bash
bash scripts/run_s2_pipeline.sh
```

The S2 workflow generates:

- `data/silver/notices_clean.csv`
- `data/silver/organizations_clean.csv`
- `data/silver/buyer_master.csv`
- `data/silver/buyer_notice_map.csv`
- `data/silver/procurement_semantic_layer.csv`
- `data/reports/s2_data_quality_report.md`
- `data/reports/s2_data_quality_metrics.json`

S3/S4 should use `buyer_master.csv` as the canonical resolved buyer table and `procurement_semantic_layer.csv` as the notice-level Microsoft relevance signal for Gold feature engineering and ML modelling. `buyer_notice_map.csv` links resolved buyers back to source notices.

Generated `data/` outputs may be ignored by Git. They can be recreated at any time by running the ETL and S2 pipeline commands above.

---

## Program
Master in Business Analytics & Data Science (MBDS)  
IE University — Class of 2026
