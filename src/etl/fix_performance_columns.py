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
        description="Curate Freddie Mac performance parquet by selecting/renaming columns and partitioning by year/month derived from yyyymm."
    )
    parser.add_argument("--input", required=True, help="Input cleaned parquet path, e.g. s3a://.../cleaned/.../svcg/v1/")
    parser.add_argument("--output", required=True, help="Output curated parquet path, e.g. s3a://.../curated/.../svcg/v1/")
    parser.add_argument("--use_s3_packages", action="store_true", help="Enable if reading s3a:// fails locally.")
    args = parser.parse_args()

    spark = build_spark("freddie-mac-curate-performance", use_s3_packages=args.use_s3_packages)

    input_path = args.input.rstrip("/") + "/*.txt"

    df = (spark.read
          .option("sep", "|")
          .option("header", "false")
          .option("mode", "PERMISSIVE")
          .csv(input_path))

    # Build select expressions
    select_exprs = []
    for raw_col, new_name, cast_type in column_specs:
        col_expr = F.col(raw_col)
        if cast_type is not None:
            col_expr = col_expr.cast(cast_type)
        select_exprs.append(col_expr.alias(new_name))

    df_curated = df.select(*select_exprs)

    # yyyymm MUST be string for rlike; keep it string here
    df_curated = df_curated.filter(F.col("yyyymm").rlike(r"^\d{6}$"))

    # Derive year/month from yyyymm
    df_curated = (
        df_curated
        .withColumn("year", F.substring(F.col("yyyymm"), 1, 4).cast("int"))
        .withColumn("month", F.substring(F.col("yyyymm"), 5, 2).cast("int"))
    )

    # Optional: cast yyyymm to int AFTER deriving year/month
    df_curated = df_curated.withColumn("yyyymm", F.col("yyyymm").cast("int"))

    (
        df_curated.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    print(f"Wrote Parquet to: {args.output}")
    spark.stop()


if __name__ == "__main__":
    # Corrected & typed performance column specs
    # Field position N -> _c(N-1)
    column_specs = [
        ("_c0",  "loan_id",            "string"),   # Loan Sequence Number
        ("_c1",  "yyyymm",             "string"),   # Monthly Reporting Period (YYYYMM) - keep string for regex
        ("_c2",  "current_upb",        "double"),   # Current Actual UPB
        ("_c3",  "delinquency_status", "string"),   # Keep string (can include non-numeric codes)
        ("_c4",  "loan_age",           "int"),
        ("_c5",  "remaining_term",     "int"),
        ("_c7",  "modification_flag",  "string"),
        ("_c8",  "zero_balance_code",  "string"),
        ("_c9",  "zero_balance_date",  "string"),   # often YYYYMM; you can cast later if you want
        ("_c10", "current_int_rate",   "double"),   # Field position 11 -> _c10
        ("_c25", "eltv",               "int"),      # Field position 26 -> _c25
    ]

    main(column_specs)
