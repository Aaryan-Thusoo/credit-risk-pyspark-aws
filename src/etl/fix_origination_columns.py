import pandas as pd
import argparse
from pyspark.sql import SparkSession, functions as F

def build_spark(app_name: str, use_s3_packages: bool) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if use_s3_packages:
        builder = (
            builder.config(
                "spark.jars.packages",
                ",".join([
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                ]),
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
        )
    return builder.getOrCreate()

def main(column_specs):
    parser = argparse.ArgumentParser(
        description="Curate Freddie Mac parquet by selecting/renaming columns and partitioning by year/month derived from yyyymm."
    )
    parser.add_argument("--input", required=True, help="Input cleaned parquet path, e.g. s3a://bucket/cleaned/.../performance/")
    parser.add_argument("--output", required=True, help="Output curated parquet path, e.g. s3a://bucket/curated/.../performance/")
    parser.add_argument("--use_s3_packages", action="store_true", help="Enable if you get s3a filesystem/jar errors locally.")
    args = parser.parse_args()

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
    file_df = pd.read_excel("/Users/aaryanthusoo/Desktop/Personal/Credit-Risk-Analysis/data/file_layout.xlsx", skiprows=1)

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
  --input s3a://credit-risk-ews-data/cleaned/freddie_mac/performance/v1/ \
  --output s3a://credit-risk-ews-data/cleaned/freddie_mac/origination/v1/ \
  --use_s3_packages
"""
