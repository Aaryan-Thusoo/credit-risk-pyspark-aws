# Baseline Model Results

Model: `HistGradientBoostingClassifier`

Data:
- Joined monthly modeling rows: 3,768,675
- Training rows before sampling: 3,281,230
- Training rows used: 250,000
- Chronological test rows: 487,445
- Test period: `yyyymm >= 202401`
- Label: `default_next_12m`

Metrics:
- ROC AUC: 0.9535
- PR AUC: 0.6436
- Precision at 0.30 threshold: 0.5512
- Recall at 0.30 threshold: 0.6967
- F1 at 0.30 threshold: 0.6155
- Positive/default rate: 1.69%
- Top 10% risk bucket captured 87.99% of positive/default cases

Resume-ready summary:

> Built a credit-risk early warning baseline on 3.8M Freddie Mac monthly loan records using AWS S3 parquet data, pandas feature joins, and scikit-learn gradient boosting; achieved 0.954 ROC AUC and captured 88.0% of future 12-month defaults in the top decile risk bucket.
