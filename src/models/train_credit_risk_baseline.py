#!/usr/bin/env python3
"""
Train a fast baseline model for the Freddie Mac credit-risk early warning data.

The model joins:
  - monthly servicing rows
  - static origination attributes
  - the prebuilt 12-month forward default label

It intentionally uses pandas + scikit-learn so it can produce quick local
results from the current S3 parquet outputs without needing a Spark install.
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


NUMERIC_FEATURES = [
    "credit_score",
    "dti",
    "ltv",
    "orig_upb",
    "orig_rate",
    "orig_term",
    "num_borrowers",
    "current_upb",
    "delinq_num",
    "loan_age",
    "remaining_term",
    "current_int_rate",
    "eltv",
    "yyyymm",
    "month",
    "prev_delinq",
    "delinq_change_1m",
    "max_delinq_prev_3m",
    "avg_delinq_prev_3m",
    "max_delinq_prev_6m",
    "avg_delinq_prev_6m",
    "ever_delinquent_prev_6m",
]

CATEGORICAL_FEATURES = ["occupancy", "property_state", "modification_flag"]


def parquet_files(path: str) -> list[Path]:
    root = Path(path).expanduser()
    if root.is_file():
        return [root]
    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under {root}")
    return files


def read_parquet_dataset(path: str, columns: list[str] | None = None) -> pd.DataFrame:
    frames = [pd.read_parquet(file, columns=columns) for file in parquet_files(path)]
    return pd.concat(frames, ignore_index=True)


def normalize_delinquency(value) -> int:
    if pd.isna(value):
        return 0
    text = str(value).strip().upper()
    if text in {"", "NULL", "NAN"}:
        return 0
    if text in {"R", "REO"}:
        return 999
    return int(text) if text.isdigit() else 0


def add_time_window_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["loan_id", "yyyymm"]).copy()
    grouped = df.groupby("loan_id", sort=False)["delinq_num"]

    df["prev_delinq"] = grouped.shift(1).fillna(0)
    df["delinq_change_1m"] = df["delinq_num"] - df["prev_delinq"]

    shifted = grouped.shift(1)
    df["max_delinq_prev_3m"] = (
        shifted.groupby(df["loan_id"], sort=False)
        .rolling(3, min_periods=1)
        .max()
        .reset_index(level=0, drop=True)
        .fillna(0)
    )
    df["avg_delinq_prev_3m"] = (
        shifted.groupby(df["loan_id"], sort=False)
        .rolling(3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
        .fillna(0)
    )
    df["max_delinq_prev_6m"] = (
        shifted.groupby(df["loan_id"], sort=False)
        .rolling(6, min_periods=1)
        .max()
        .reset_index(level=0, drop=True)
        .fillna(0)
    )
    df["avg_delinq_prev_6m"] = (
        shifted.groupby(df["loan_id"], sort=False)
        .rolling(6, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
        .fillna(0)
    )
    df["ever_delinquent_prev_6m"] = (df["max_delinq_prev_6m"] > 0).astype(int)
    df["month"] = (df["yyyymm"] % 100).astype(int)
    return df


def load_modeling_table(args: argparse.Namespace) -> pd.DataFrame:
    servicing = read_parquet_dataset(
        args.servicing,
        columns=[
            "loan_id",
            "yyyymm",
            "current_upb",
            "delinquency_status",
            "loan_age",
            "remaining_term",
            "modification_flag",
            "current_int_rate",
            "eltv",
        ],
    )
    origination = read_parquet_dataset(args.origination)
    labels = read_parquet_dataset(args.labels)

    servicing["delinq_num"] = servicing["delinquency_status"].map(normalize_delinquency)
    servicing = add_time_window_features(servicing)

    df = (
        labels.merge(servicing, on=["loan_id", "yyyymm"], how="inner")
        .merge(origination, on="loan_id", how="left")
        .rename(columns={"default_next_12m": "label"})
    )

    df = df.dropna(subset=["label", "yyyymm"])
    df["label"] = df["label"].astype(int)
    return df


def downsample_train(train: pd.DataFrame, max_rows: int, negative_ratio: int, seed: int) -> pd.DataFrame:
    positives = train[train["label"] == 1]
    negatives = train[train["label"] == 0]
    max_negatives = min(len(negatives), max(len(positives) * negative_ratio, max_rows - len(positives)))
    sampled_negatives = negatives.sample(n=max_negatives, random_state=seed) if len(negatives) > max_negatives else negatives
    sampled = pd.concat([positives, sampled_negatives], ignore_index=True)
    if len(sampled) > max_rows:
        sampled = sampled.sample(n=max_rows, random_state=seed)
    return sampled.sample(frac=1, random_state=seed).reset_index(drop=True)


def evaluate(y_true: pd.Series, scores: np.ndarray, threshold: float) -> dict:
    preds = (scores >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, preds, labels=[0, 1]).ravel()

    order = np.argsort(-scores)
    top_n = max(int(len(scores) * 0.10), 1)
    top_pos = int(y_true.iloc[order[:top_n]].sum())
    positives = int(y_true.sum())

    return {
        "rows_evaluated": int(len(y_true)),
        "positive_labels": positives,
        "positive_rate": float(y_true.mean()),
        "roc_auc": float(roc_auc_score(y_true, scores)),
        "pr_auc": float(average_precision_score(y_true, scores)),
        "threshold": threshold,
        "precision": float(precision_score(y_true, preds, zero_division=0)),
        "recall": float(recall_score(y_true, preds, zero_division=0)),
        "f1": float(f1_score(y_true, preds, zero_division=0)),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "tp": int(tp),
        "top_10pct_bucket_size": top_n,
        "top_10pct_positive_cases": top_pos,
        "top_10pct_capture_rate": float(top_pos / positives) if positives else 0.0,
    }


def print_metrics(metrics: dict) -> None:
    print("\n================ MODEL RESULTS ================")
    print(f"Rows evaluated: {metrics['rows_evaluated']:,}")
    print(f"Positive labels: {metrics['positive_labels']:,}")
    print(f"Positive rate: {metrics['positive_rate']:.4f}")
    print(f"ROC AUC: {metrics['roc_auc']:.4f}")
    print(f"PR AUC: {metrics['pr_auc']:.4f}")
    print(f"Threshold: {metrics['threshold']:.2f}")
    print(f"Precision: {metrics['precision']:.4f}")
    print(f"Recall: {metrics['recall']:.4f}")
    print(f"F1: {metrics['f1']:.4f}")
    print("\nConfusion Matrix")
    print(f"TN: {metrics['tn']:,} | FP: {metrics['fp']:,}")
    print(f"FN: {metrics['fn']:,} | TP: {metrics['tp']:,}")
    print("\nRisk Ranking")
    print(f"Top 10% risk bucket size: {metrics['top_10pct_bucket_size']:,}")
    print(f"Positive cases captured in top 10%: {metrics['top_10pct_positive_cases']:,}")
    print(f"Top 10% capture rate: {metrics['top_10pct_capture_rate']:.4f}")
    print("================================================\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a quick Freddie Mac credit-risk baseline model.")
    parser.add_argument("--servicing", required=True, help="Curated servicing parquet directory.")
    parser.add_argument("--origination", required=True, help="Cleaned origination parquet directory.")
    parser.add_argument("--labels", required=True, help="12-month default label parquet directory.")
    parser.add_argument("--test-start-yyyymm", type=int, default=202401, help="Chronological test split start.")
    parser.add_argument("--max-train-rows", type=int, default=250_000, help="Training cap for fast local runs.")
    parser.add_argument("--negative-ratio", type=int, default=20, help="Max negative rows per positive training row.")
    parser.add_argument("--threshold", type=float, default=0.30, help="Classification threshold for reported metrics.")
    parser.add_argument("--metrics-output", default="outputs/model_metrics.json", help="Where to write metrics JSON.")
    parser.add_argument("--scores-output", default="outputs/risk_scores_top.csv", help="Where to write top risk scores.")
    parser.add_argument("--top-scores", type=int, default=5000, help="Number of top risk rows to save.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_modeling_table(args)

    features = [c for c in NUMERIC_FEATURES + CATEGORICAL_FEATURES if c in df.columns]
    train = df[df["yyyymm"] < args.test_start_yyyymm].copy()
    test = df[df["yyyymm"] >= args.test_start_yyyymm].copy()

    if train["label"].nunique() < 2:
        raise ValueError("Training data needs both positive and negative labels.")
    if test["label"].nunique() < 2:
        raise ValueError("Test data needs both positive and negative labels.")

    train_fit = downsample_train(train, args.max_train_rows, args.negative_ratio, seed=42)

    numeric = [c for c in NUMERIC_FEATURES if c in features]
    categorical = [c for c in CATEGORICAL_FEATURES if c in features]
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ]
    )
    model = HistGradientBoostingClassifier(
        max_iter=120,
        learning_rate=0.06,
        max_leaf_nodes=31,
        l2_regularization=0.05,
        random_state=42,
    )
    pipeline = Pipeline([("preprocess", preprocessor), ("model", model)])

    print(f"Joined modeling rows: {len(df):,}")
    print(f"Training rows before sampling: {len(train):,}")
    print(f"Training rows used: {len(train_fit):,}")
    print(f"Test rows: {len(test):,}")
    print(f"Features used ({len(features)}): {', '.join(features)}")

    pipeline.fit(train_fit[features], train_fit["label"])
    scores = pipeline.predict_proba(test[features])[:, 1]
    metrics = evaluate(test["label"], scores, args.threshold)
    metrics.update(
        {
            "model": "HistGradientBoostingClassifier",
            "features": features,
            "train_rows_before_sampling": int(len(train)),
            "train_rows_used": int(len(train_fit)),
            "test_start_yyyymm": int(args.test_start_yyyymm),
        }
    )
    print_metrics(metrics)

    metrics_path = Path(args.metrics_output)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(metrics, indent=2) + "\n")

    scores_path = Path(args.scores_output)
    scores_path.parent.mkdir(parents=True, exist_ok=True)
    scored = test[["loan_id", "yyyymm", "label"]].copy()
    scored["prob_default_next_12m"] = scores
    scored.sort_values("prob_default_next_12m", ascending=False).head(args.top_scores).to_csv(scores_path, index=False)

    print(f"Saved metrics to: {metrics_path}")
    print(f"Saved top risk scores to: {scores_path}")


if __name__ == "__main__":
    main()
