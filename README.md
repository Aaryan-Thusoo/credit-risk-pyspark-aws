# Credit Risk Early Warning System (PySpark + AWS)

An end-to-end pipeline that flags mortgage borrowers whose credit risk is deteriorating, using historical origination and servicing behavior to predict default over a forward-looking horizon. Built on Freddie Mac single-family loan-level data, staged in an AWS S3 data lake and processed with PySpark.

**Status: Baseline complete.** See [`MODEL_RESULTS.md`](./MODEL_RESULTS.md) for full metrics.

## Results

| Metric | Value |
|---|---|
| Model | `HistGradientBoostingClassifier` |
| ROC AUC | **0.9535** |
| PR AUC | 0.6436 |
| Precision @ 0.30 threshold | 0.5512 |
| Recall @ 0.30 threshold | 0.6967 |
| F1 @ 0.30 threshold | 0.6155 |
| Top-10% risk bucket capture | **87.99%** of defaults |
| Default prevalence | 1.69% |
| Joined modeling rows | 3,768,675 |
| Training sample | 250,000 (downsampled from 3.28M) |
| Test set | 487,445 rows, chronological, Jan 2024 onward |

The model achieves strong discrimination (0.95 ROC AUC) despite a highly imbalanced target (1.69% default rate), and concentrates default risk effectively: the highest-risk decile alone captures nearly 88% of actual defaults, which is the property that matters for prioritizing a risk team's limited review capacity.

## Problem

Lenders need to identify which borrowers in an existing portfolio are becoming riskier *before* they default, not after — an early warning system, not a post-mortem. This project builds a monthly-refreshable scoring pipeline that ranks borrowers by predicted risk of default within a future window, so a risk team can prioritize outreach and review toward the borrowers most likely to need it.

## Data

- **Freddie Mac origination data** — pipe-delimited loan and borrower characteristics at the time of origination (credit score, LTV, DTI, loan purpose, etc.)
- **Freddie Mac monthly performance data** — servicing records updated monthly, including delinquency status, used to construct forward-looking default labels
- Both datasets are staged in an **AWS S3** data lake and joined on loan ID

## Pipeline

1. `fix_origination_columns.py` — extracts and cleans origination records into a consistent schema
2. `fix_performance_columns.py` — processes monthly performance data with year/month partitioning
3. `check_cleaned_schema.py` — validates cleaned datasets against the expected schema before joining
4. `train_credit_risk_baseline.py` — joins origination and performance data, engineers time-window features, constructs the 12-month default label, and trains the baseline model

Feature engineering focuses on time-window behavior (e.g., trailing delinquency patterns) rather than static origination characteristics alone, since the goal is detecting *deterioration*, not just initial credit quality.

## Repository structure

```
.
├── architecture/     # pipeline/architecture diagrams
├── notebooks/        # exploratory analysis
├── src/               # ETL and training scripts
├── MODEL_RESULTS.md  # full metrics and evaluation
├── requirements.txt
└── README.md
```

## Tech stack

- **Processing:** Python, PySpark
- **Modeling:** scikit-learn (`HistGradientBoostingClassifier`), Spark MLlib
- **Storage:** AWS S3
- **Data:** Freddie Mac Single-Family Loan-Level Dataset

## Next steps

- Move the downsampled training approach to a fully distributed Spark MLlib training run at full scale
- Add a monthly batch-scoring job so the model can be refreshed as new performance data lands
- Expand time-window features beyond the current baseline set
