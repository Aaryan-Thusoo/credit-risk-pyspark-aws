import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from helpers import build_spark

def main(column_specs):
    parser = argparse.ArgumentParser(
        description="Curate Freddie Mac origination text files by selecting and renaming required columns."
    )
    parser.add_argument("--input", required=True, help="Input text file path or prefix, e.g. s3a://bucket/raw/.../origination/")
    parser.add_argument("--output", required=True, help="Output parquet path, e.g. s3a://bucket/cleaned/.../origination/v1/")
    parser.add_argument("--use_s3_packages", action="store_true", help="Enable if you get s3a filesystem/jar errors locally.")
    args = parser.parse_args()

    from pyspark.sql import functions as F

    spark = build_spark("freddie-mac-name-origination-cols", use_s3_packages=args.use_s3_packages)

    input_path = args.input.rstrip("/") + "/*.txt"

    df = (spark.read
        .option("sep", "|")
        .option("header", "false")
        .option("mode", "PERMISSIVE")
        .csv(input_path))

    select_exprs = []
    for raw_col, new_name, cast_type in column_specs:
        col_expr = F.col(raw_col)
        if cast_type is not None:
            col_expr = col_expr.cast(cast_type)
        select_exprs.append(col_expr.alias(new_name))

    df_curated = df.select(*select_exprs)

    df_curated = df_curated.filter(F.col("loan_id").isNotNull() & (F.length("loan_id") > 0))

    df_curated = df_curated.dropDuplicates(["loan_id"])

    # Write (no month partitioning for origination)
    df_curated.write.mode("overwrite").parquet(args.output)

    print(f"Wrote origination parquet to: {args.output}")
    spark.stop()

if __name__ == "__main__":
    """
    Required Columns:
    20  - Loan Sequence Number
     1  - Credit Score
    10  - Original Debt-to-Income (DTI) Ratio
    12  - Original Loan-to-Value (DTI) Ratio
    11  - Original UPB
    13  - Original Interest Rate
    22  - Original Loan Term
     8  - Occupancy Status
    23  - Number of Borrowers
    17  - Property State
    """

    # Needed columns
    column_specs = [
        ("_c19", "loan_id", None),
        ("_c0", "credit_score", "int"),
        ("_c9", "dti", "int"),
        ("_c11", "ltv", "int"),
        ("_c10", "orig_upb", "double"),
        ("_c12", "orig_rate", "double"),
        ("_c21", "orig_term", "int"),
        ("_c7", "occupancy", None),
        ("_c22", "num_borrowers", "int"),
        ("_c16", "property_state", None),
        ]

    main(column_specs)
#("yyyymm", "yyyymm", None),
"""
python3 src/etl/curate_origination.py \
  --input s3a://credit-risk-ews-data/raw/freddie_mac/origination/ \
  --output s3a://credit-risk-ews-data/cleaned/freddie_mac/origination/v1/ \
  --use_s3_packages
"""
