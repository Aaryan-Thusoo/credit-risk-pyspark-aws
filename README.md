# Credit Risk Early Warning System (PySpark + AWS)

## Business Problem
Build an Early Warning System (EWS) that flags borrowers whose credit risk is
deteriorating, using historical behavior to predict adverse outcomes over a
future horizon (e.g., next 3 months). This supports portfolio monitoring and
risk prioritization.

## Tech Stack
- Python, PySpark (feature engineering + ML)
- AWS S3 (data lake)
- Spark MLlib (baseline models)

## Project Status
In progress — implementing time-window feature engineering and baseline models.

## Current ETL Scripts
- `src/etl/fix_origination_columns.py`: reads Freddie Mac origination pipe-delimited text files, selects the fields needed for modeling, and writes cleaned parquet.
- `src/etl/fix_performance_columns.py`: reads Freddie Mac monthly performance pipe-delimited text files, selects key servicing fields, derives `year` and `month`, and writes partitioned parquet.
- `src/etl/check_cleaned_schema.py`: prints the schema of a cleaned parquet dataset.
- `src/models/train_credit_risk_baseline.py`: joins origination, servicing, and 12-month default-label parquet outputs, then trains a fast sklearn baseline model.

## Example Usage
```bash
python3 src/etl/fix_origination_columns.py \
  --input s3a://credit-risk-ews-data/raw/freddie_mac/orig/ \
  --output s3a://credit-risk-ews-data/cleaned/freddie_mac/orig/v1/ \
  --use_s3_packages

python3 src/etl/fix_performance_columns.py \
  --input s3a://credit-risk-ews-data/raw/freddie_mac/svcg/ \
  --output s3a://credit-risk-ews-data/curated/freddie_mac/svcg/v1/ \
  --use_s3_packages

python3 src/etl/check_cleaned_schema.py \
  s3a://credit-risk-ews-data/curated/freddie_mac/svcg/v1/ \
  --use_s3_packages
```

## Baseline Model
Download the current S3 parquet outputs locally, then run the baseline:

```bash
aws s3 cp s3://credit-risk-ews-data/cleaned/freddie_mac/orig/v1/ work/aws/cleaned/freddie_mac/orig/v1/ --recursive
aws s3 cp s3://credit-risk-ews-data/curated/freddie_mac/svcg/v1/ work/aws/curated/freddie_mac/svcg/v1/ --recursive
aws s3 cp s3://credit-risk-ews-data/features/default_12m/ work/aws/features/default_12m/ --recursive

python3 src/models/train_credit_risk_baseline.py \
  --servicing work/aws/curated/freddie_mac/svcg/v1 \
  --origination work/aws/cleaned/freddie_mac/orig/v1 \
  --labels work/aws/features/default_12m \
  --metrics-output outputs/model_metrics.json \
  --scores-output outputs/risk_scores_top.csv
```

Latest local baseline results are summarized in `MODEL_RESULTS.md`.
