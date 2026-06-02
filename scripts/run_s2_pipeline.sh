#!/usr/bin/env bash
set -e

python3 "Exploratory ETL/ted_exploratory_etl.py" --packages 2026-05 --max-files 10
python3 src/s2_silver_layer.py

echo "Generated S2 output files:"
for file in \
  data/silver/notices_clean.csv \
  data/silver/organizations_clean.csv \
  data/silver/buyer_master.csv \
  data/silver/buyer_notice_map.csv \
  data/silver/procurement_semantic_layer.csv \
  data/reports/s2_data_quality_report.md \
  data/reports/s2_data_quality_metrics.json
do
  if [ -f "$file" ]; then
    echo "$file"
  else
    echo "missing: $file"
  fi
done
